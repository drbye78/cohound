"""coHound — предметы решений гейтов G3/G4 и исходящий запрос на решение (точки интеграции).

**Зачем отдельный модуль.** G3 (strict-ревью) и G4 (утверждение публикации) выполняются
**внешними системами**. coHound не принимает эти решения: он (1) вычисляет отпечаток
предмета решения, (2) экспортирует предмет наружу, (3) записывает полученный ответ.
Поэтому предмет обязан быть детерминированным и полным: внешний ревьюер утверждает
**конкретное состояние**, и любое изменение состояния обязано инвалидировать утверждение.

**Отпечатки.**

* G3 — отсортированный набор ``(product_id, param_id, cell_state, status, review_reason)``
  очереди ревью (Ш7) + сводка оценки. Изменение очереди (новая ``⚠️``-ячейка, смена
  статуса) меняет отпечаток: внешний ревьюер утверждал другое состояние.
* G4 — Coverage (знаменатель, завершённые), значения Capability, аннотация
  Comparability, ранжирование, отпечаток **содержимого** публикуемых артефактов
  (``publish.content_digest`` — детерминированный дамп бандла и ячеек; хэши файлов
  добавляются, когда файлы уже существуют, — до Ш9 их физически нет) и секция
  ``absence`` — что именно публикуется как отсутствие: сила ``❌`` и дата носителя.
  Решение владельца 19.11.2026 (вариант D): ответственность за публикацию ``❌``
  переходит к G4, поэтому подписант обязан видеть то, что раньше видел автор
  per-cell подписи риска.

Метки времени, пути и порядок обхода в материал **не** входят: один и тот же прогон даёт
один и тот же отпечаток и один и тот же файл запроса (побайтно).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from core.domain import ABSENT_DENIAL_STRENGTHS, DenialStrength, Gate, Status
from core.pipeline.compute import ScoreBundle
from core.util.hashing import short_hash

if TYPE_CHECKING:  # pragma: no cover — только типы: review импортирует runner (цикл)
    from core.pipeline.review import ReviewItem

__all__ = [
    "DECISION_CONTRACT",
    "PUBLISHED_ARTIFACTS",
    "GateSubject",
    "SubjectResolver",
    "describe_subject_diff",
    "gate_request_payload",
    "publication_subject",
    "read_gate_request_digest",
    "read_gate_request_subject",
    "review_subject",
    "write_gate_request",
]

#: Длина отпечатка предмета решения (как у прочих дайджестов идентичности, §12.2).
_DIGEST_LEN = 24

#: Артефакты, которые Ш9 публикует в ``runs/{run_id}/outputs/`` (порядок фиксирован).
PUBLISHED_ARTIFACTS: tuple[str, ...] = (
    "battle_card.xlsx",
    "preliminary_findings.xlsx",
    "nda_questions.csv",
    "diff.md",
)

#: Контракт решения, который видит внешняя система в файле запроса.
DECISION_CONTRACT: dict[str, Any] = {
    "accepted_sources": ["internal", "external"],
    "required_fields": ["actor", "external_ref", "subject_digest", "notes"],
    "verdicts": ["approved", "rejected"],
    "note": (
        "решение записывается командой hound gate с --source external --external-ref "
        "<id> --subject-digest <subject_digest> --verdict approved|rejected --notes <why>"
    ),
}


def _canonical(payload: Any) -> str:
    """Канонический JSON: стабильные ключи/разделители, UTF-8 без escape."""
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


@dataclass(frozen=True)
class GateSubject:
    """Предмет решения гейта: отпечаток, материал и экспорт для внешней системы.

    ``digest`` — то, что обязано совпасть у решения (``subject_digest``). ``material`` —
    из чего отпечаток вычислен (по нему объясняется расхождение). ``export`` — то, что
    нужно внешней системе, чтобы принять решение (для G3 — очередь ревью, для G4 —
    Coverage/Capability/артефакты).
    """

    gate: Gate
    digest: str
    material: dict[str, Any]
    export: dict[str, Any] = field(default_factory=dict)


def _digest_of(material: Mapping[str, Any]) -> str:
    return short_hash(_canonical(dict(material)), _DIGEST_LEN)


# ---------------------------------------------------------------------------
# G3: очередь ревью
# ---------------------------------------------------------------------------


def _review_cell(item: ReviewItem) -> dict[str, Any]:
    """Строка предмета ревью: то, о чём решает внешний ревьюер (без провенанса)."""
    return {
        "product_id": item.product_id,
        "param_id": item.param_id,
        "cell_state": item.state.value,
        "status": item.status.value if item.status is not None else None,
        "review_reason": item.reason,
    }


def review_subject(
    items: Sequence[ReviewItem], *, run_id: str | None = None
) -> GateSubject:
    """Предмет G3 — очередь ревью (Ш7) и её сводка.

    :param items: очередь из :func:`core.pipeline.review.review_queue` (детерминирована).
    :param run_id: идентификатор прогона; входит в материал (решение относится к прогону),
        но не в ``export`` — экспорт совпадает по структуре с ``hound review --json``.

    Материал: отсортированные строки ``(product_id, param_id, cell_state, status,
    review_reason)`` + сводка ``total``/``by_state``/``by_status``. Добавление ячейки,
    смена статуса или причины меняет отпечаток — утверждение перестаёт действовать.
    """
    cells = sorted(
        (_review_cell(item) for item in items),
        key=lambda row: (row["product_id"], row["param_id"]),
    )
    by_state: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for row in cells:
        by_state[row["cell_state"]] = by_state.get(row["cell_state"], 0) + 1
        key = row["status"] if row["status"] is not None else "none"
        by_status[key] = by_status.get(key, 0) + 1
    material = {
        "run_id": run_id,
        "cells": cells,
        "summary": {
            "total": len(cells),
            "by_state": by_state,
            "by_status": by_status,
        },
    }
    export = {
        "run_id": run_id,
        "count": len(cells),
        "items": [
            {
                "product_id": row["product_id"],
                "param_id": row["param_id"],
                "state": row["cell_state"],
                "status": row["status"],
                "reason": row["review_reason"],
            }
            for row in cells
        ],
        "summary": material["summary"],
    }
    return GateSubject(
        gate=Gate.G3, digest=_digest_of(material), material=material, export=export
    )


# ---------------------------------------------------------------------------
# G4: публикация
# ---------------------------------------------------------------------------


def _coverage_material(bundle: ScoreBundle) -> dict[str, Any]:
    coverage = bundle.coverage
    return {
        "defined": bool(coverage.defined),
        "value": coverage.value,
        "numerator": coverage.numerator,
        "denominator": coverage.denominator,
        "universe": coverage.universe,
        "completed": coverage.completed,
        "n_used": coverage.n_used,
    }


def _capability_material(bundle: ScoreBundle) -> dict[str, Any]:
    score = bundle.capability
    return {
        "value": score.value,
        "numerator": score.numerator,
        "denominator": score.denominator,
        "n_used": score.n_used,
    }


def _modules_material(bundle: ScoreBundle) -> list[dict[str, Any]]:
    return [
        {
            "module": module.module,
            "score": module.score,
            "coverage": module.coverage,
            "weight": module.weight,
            "applicable_weight": module.applicable_weight,
        }
        for module in sorted(bundle.modules, key=lambda item: item.module)
    ]


def _comparability_material(bundle: ScoreBundle) -> dict[str, Any]:
    comparability = bundle.comparability
    return {
        "level": comparability.level,
        "min_coverage": comparability.min_coverage,
        "cross_cycle": bool(comparability.cross_cycle),
        "reasons": list(comparability.reasons),
    }


#: Классы силы отрицания, которые публикуются как ``❌``. Порядок — объявление в домене;
#: набор берётся из единственного источника (:data:`ABSENT_DENIAL_STRENGTHS`), чтобы новый
#: класс не появился в предмете решения «сам собой» и не пропал молча.
_ABSENCE_STRENGTHS: tuple[str, ...] = tuple(
    strength.value for strength in DenialStrength if strength in ABSENT_DENIAL_STRENGTHS
)


def _min_as_of(values: Sequence[str]) -> str | None:
    """Самая ранняя дата носителя (``absence_as_of``) среди непустых значений.

    Значения пишет :func:`core.domain.invariants.check_absence_dated` как
    ``min(дат).isoformat()`` — ISO-8601 в UTC, то есть с **одним и тем же** смещением
    ``+00:00``. Поэтому сравниваются разобранные даты, а не строки: лексикографический
    порядок ломается на разной точности (``…:56.5+00:00`` строкой «меньше»
    ``…:56+00:00``, хотя по времени позже). Для значений, которые не разбираются
    (legacy-строка) или смешивают aware/naive, остаётся строковое сравнение — тогда
    публикуется минимум как строка, как есть: подменять его на «даты нет» нельзя.
    """
    cleaned = [value for value in values if value]
    if not cleaned:
        return None
    parsed: list[tuple[_dt.datetime, str]] = []
    for value in cleaned:
        try:
            parsed.append((_dt.datetime.fromisoformat(value), value))
        except (TypeError, ValueError):
            return min(cleaned)
    try:
        return min(parsed, key=lambda item: item[0])[1]
    except TypeError:  # смешение aware/naive — сравниваем как строки
        return min(cleaned)


def _absence_material(cells: Sequence[Any]) -> dict[str, Any]:
    """Что именно публикуется как отсутствие: сила каждого ``❌`` и дата его носителя.

    Считается по **тем же** ячейкам, что уже переданы в :func:`publication_subject`:
    второго чтения стора нет — предмет решения описывает ровно то состояние, о котором
    спрашивают внешнюю систему.

    * ``by_strength`` — счётчики по классам, дающим ``❌`` (``explicit``/
      ``authoritative``/``bounded``). Класс берётся из
      ``cell.evidence_scope.denial_strength``. Класс вне этих трёх не схлопывается в
      чужой счётчик, а появляется отдельным ключом: это дефект данных (I2), и подписант
      обязан его видеть, а не получить аккуратную сумму.
    * ``as_of_min`` — самая ранняя дата носителя среди ``❌`` (``min``, как в I2d: она
      честная нижняя граница утверждения), ``None`` — публиковать нечего. Даты берутся
      только у ячеек с классом: у ``❌`` без ``evidence_scope`` нет ни силы, ни
      проверяемого носителя — такая ячейка учтена в ``undated``.
    * ``undated`` — ``❌``, у которых подписанту нечего проверить: нет даты носителя
      (``absence_as_of``) либо нет ``evidence_scope`` (нет и силы). Обязано быть 0;
      ненулевое значение — сигнал дефекта, а не норма.
    * ``products_without_carrier`` — продукты, у которых такой ``❌`` есть (отсортировано,
      чтобы отпечаток не зависел от порядка входа).

    Секция присутствует всегда, в том числе когда ``❌`` нет вовсе (нули и
    ``as_of_min=None``): «отсутствие не публикуется» — факт предмета, а не пустота,
    и подписант не должен отличать его от «секцию забыли посчитать».
    """
    by_strength: dict[str, int] = {strength: 0 for strength in _ABSENCE_STRENGTHS}
    dates: list[str] = []
    undated = 0
    without_carrier: set[str] = set()
    for cell in cells:
        if _enum_value(getattr(cell, "status", None)) != Status.ABSENT.value:
            continue
        product_id = str(getattr(cell, "product_id", None))
        scope = getattr(cell, "evidence_scope", None)
        strength = (
            _enum_value(getattr(scope, "denial_strength", None))
            if scope is not None
            else None
        )
        if strength is None:
            # Нет класса — значит нет и силы, которую подписант мог бы взвесить.
            undated += 1
            without_carrier.add(product_id)
            continue
        key = str(strength)
        by_strength[key] = by_strength.get(key, 0) + 1
        as_of = getattr(cell, "absence_as_of", None)
        if as_of:
            dates.append(str(as_of))
        else:
            undated += 1
            without_carrier.add(product_id)
    return {
        "by_strength": by_strength,
        "as_of_min": _min_as_of(dates),
        "undated": undated,
        "products_without_carrier": sorted(without_carrier),
    }


def _artifact_material(
    outputs_dir: str | Path | None,
    publish_digest: str,
) -> dict[str, Any]:
    """Состав публикуемых артефактов: имена + хэши файлов, если они уже записаны.

    До Ш9 файлов физически нет, поэтому отпечаток **содержимого** несёт
    ``content_digest`` (детерминированный дамп бандла и ячеек): именно он делает
    утверждение привязанным к тому, что будет опубликовано. Когда файлы существуют
    (resume/повторная проверка), их побайтовые хэши добавляются — так решение можно
    сверить с реально опубликованным артефактом.
    """
    entries: list[dict[str, Any]] = []
    for name in PUBLISHED_ARTIFACTS:
        path = Path(outputs_dir) / name if outputs_dir is not None else None
        sha256: str | None = None
        if path is not None and path.is_file():
            sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        entries.append({"name": name, "sha256": sha256})
    return {"artifacts": entries, "content_digest": publish_digest}


def publication_subject(
    bundle: ScoreBundle,
    *,
    run_id: str,
    cells: Iterable[Any] = (),
    outputs_dir: str | Path | None = None,
) -> GateSubject:
    """Предмет G4 — что именно будет опубликовано.

    :param bundle: ``ScoreBundle`` прогона (Coverage/Capability/Comparability/ранжирование).
    :param run_id: идентификатор прогона (входит в материал: решение относится к прогону).
    :param cells: ячейки прогона — входят в ``content_digest`` публикуемого содержимого и
        в секцию ``absence`` (сила и датированность каждого ``❌``).
    :param outputs_dir: ``runs/{run_id}/outputs`` — если артефакты уже записаны, их хэши
        попадают в материал; до Ш9 их нет и поле остаётся ``null``.

    Материал: Coverage, Capability, модульные баллы, Comparability, ранжирование
    (запрошенное/вычисленное), доля сбоев, состав артефактов и ``absence`` — что именно
    публикуется как отсутствие. Изменение любого из них — другое состояние публикации,
    то есть другое решение.
    """
    cell_list = list(cells)
    cell_rows = sorted(
        (
            {
                "product_id": getattr(cell, "product_id", None),
                "param_id": getattr(cell, "param_id", None),
                "state": _enum_value(getattr(cell, "state", None)),
                "status": _enum_value(getattr(cell, "status", None)),
            }
            for cell in cell_list
        ),
        key=lambda row: (str(row["product_id"]), str(row["param_id"])),
    )
    publish_digest = _digest_of(
        {"bundle": bundle.to_dict(), "cells": cell_rows}
    )
    # ``dict[str, Any]`` явно: материал — разнородный словарь, который уходит в JSON и в
    # отпечаток. Без аннотации mypy выводит объединение типов значений (``dict[str, object]``),
    # и обращение к вложенному словарю ниже («artifacts») становится неиндексируемым —
    # хотя ``_artifact_material`` возвращает ``dict[str, Any]``.
    material: dict[str, Any] = {
        "run_id": run_id,
        "coverage": _coverage_material(bundle),
        "capability": _capability_material(bundle),
        "modules": _modules_material(bundle),
        "comparability": _comparability_material(bundle),
        "ranking": {
            "requested": bundle.requested_ranking,
            "computed": bundle.computed_ranking,
            "epsilon": bundle.ranking_epsilon,
        },
        "failure_rate": bundle.failure_rate,
        "absence": _absence_material(cell_list),
        "publish": _artifact_material(outputs_dir, publish_digest),
    }
    export: dict[str, Any] = {
        "run_id": run_id,
        "coverage": material["coverage"],
        "capability": material["capability"],
        "comparability": material["comparability"],
        "ranking": material["ranking"],
        "absence": material["absence"],
        "artifacts": material["publish"]["artifacts"],
        "content_digest": publish_digest,
    }
    return GateSubject(
        gate=Gate.G4, digest=_digest_of(material), material=material, export=export
    )


def _enum_value(value: Any) -> Any:
    """Значение enum-поля (или ``None``) для материала отпечатка."""
    return getattr(value, "value", value)


# ---------------------------------------------------------------------------
# Диагностика расхождения
# ---------------------------------------------------------------------------


def _shorten(value: Any, limit: int = 12) -> str:
    """Короткое представление значения для сообщения (дайджесты — 12 символов)."""
    text = str(value)
    return text[:limit] + "…" if len(text) > limit else text


def _row_brief(value: Any) -> str:
    """Компактная идентичность элемента списка: ``p1/param_a`` вместо сырого JSON.

    Сообщение о расхождении читает человек/внешняя система: «добавлены [{...обрезанный
    JSON...}]» бесполезно, а «добавлены [p2/param_b]» называет ровно то, что изменилось.
    """
    if isinstance(value, dict):
        for keys in (("product_id", "param_id"), ("gate",), ("module",), ("name",)):
            if all(key in value for key in keys):
                return "/".join(str(value[key]) for key in keys)
        return _shorten(_canonical(value), 48)
    return _shorten(value)


def _describe_value(key: str, old: Any, new: Any) -> str:
    """Описание изменения одного компонента материала (список/словарь/скаляр)."""
    if isinstance(old, list) and isinstance(new, list):
        old_keys = {_canonical(item): item for item in old}
        new_keys = {_canonical(item): item for item in new}
        added = [new_keys[k] for k in new_keys if k not in old_keys]
        removed = [old_keys[k] for k in old_keys if k not in new_keys]
        parts: list[str] = []
        if added:
            parts.append(f"добавлены {_brief(added)}")
        if removed:
            parts.append(f"убраны {_brief(removed)}")
        return f"{key}: " + ("; ".join(parts) if parts else "изменён порядок/состав")
    if isinstance(old, dict) and isinstance(new, dict):
        added = sorted(set(new) - set(old))
        removed = sorted(set(old) - set(new))
        changed = sorted(k for k in new if k in old and new[k] != old[k])
        parts = []
        if added:
            parts.append(f"добавлены {added}")
        if removed:
            parts.append(f"убраны {removed}")
        if changed:
            parts.append(f"изменены {changed}")
        return f"{key}: " + "; ".join(parts)
    return f"{key}: {_shorten(old)} → {_shorten(new)}"


def _brief(values: Sequence[Any]) -> str:
    """Краткое перечисление элементов списка для сообщения (не более 3)."""
    rendered = [_row_brief(value) for value in values[:3]]
    tail = ", …" if len(values) > 3 else ""
    return "[" + ", ".join(rendered) + tail + "]"


def describe_subject_diff(
    previous: Mapping[str, Any], current: Mapping[str, Any]
) -> list[str]:
    """Чем текущий предмет отличается от того, по которому принимали решение.

    Возвращает строки по изменившимся компонентам (для списков — добавленные/убранные
    элементы). Пустой список — предметы совпадают. Без этого сообщения блок говорил бы
    только «решения нет», и человек/внешняя система пере-утверждали бы вслепую.
    """
    changes: list[str] = []
    for key in sorted(set(previous) | set(current)):
        in_old, in_new = key in previous, key in current
        if in_old != in_new:
            changes.append(f"{key}: компонент {'появился' if in_new else 'исчез'}")
            continue
        old, new = previous[key], current[key]
        if old != new:
            changes.append(_describe_value(key, old, new))
    return changes


# ---------------------------------------------------------------------------
# Исходящий запрос на решение (точка интеграции)
# ---------------------------------------------------------------------------


def gate_request_payload(
    *, run_id: str, subject: GateSubject
) -> dict[str, Any]:
    """Тело файла ``gates/{gate}-request.json`` (детерминированное, без меток времени)."""
    return {
        "run_id": run_id,
        "gate": subject.gate.value,
        "subject_digest": subject.digest,
        "subject": subject.material,
        "decision_contract": DECISION_CONTRACT,
        "export": subject.export,
    }


def write_gate_request(
    runs_dir: str | Path,
    run_id: str,
    subject: GateSubject,
) -> Path:
    """Пишет запрос на решение внешней системы: ``runs/{run_id}/gates/{gate}-request.json``.

    Содержимое детерминировано (канонический JSON, без меток времени): один и тот же
    прогон в одном и том же состоянии даёт побайтно тот же файл, а ``subject_digest`` в
    нём — ровно тот, который обязано нести совпадающее решение (``hound gate
    --subject-digest``). Перезапись идемпотентна.

    :returns: путь записанного файла.
    """
    path = Path(runs_dir) / run_id / "gates" / f"{subject.gate.value}-request.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        gate_request_payload(run_id=run_id, subject=subject),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    path.write_text(body + "\n", encoding="utf-8")
    return path


def read_gate_request_subject(
    runs_dir: str | Path, run_id: str, gate: Gate
) -> dict[str, Any] | None:
    """Материал предмета из запроса на решение (``None`` — запроса нет/он битый).

    По нему объясняется, **что** изменилось с момента решения: запрос — запись того
    состояния, о котором спрашивали внешнюю систему. Это диагностика, а не доверие:
    отпечаток текущего предмета всегда вычисляется заново.
    """
    path = Path(runs_dir) / run_id / "gates" / f"{gate.value}-request.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    subject = payload.get("subject")
    return subject if isinstance(subject, dict) else None


def read_gate_request_digest(
    runs_dir: str | Path, run_id: str, gate: Gate
) -> str | None:
    """``subject_digest`` из запроса на решение (``None`` — запроса нет/он битый).

    Нужен CLI: человек или внешняя система отвечает на конкретный запрос, поэтому
    отпечаток берётся из файла запроса, а не угадывается.
    """
    path = Path(runs_dir) / run_id / "gates" / f"{gate.value}-request.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    digest = payload.get("subject_digest")
    return str(digest) if digest else None


#: Тип резолвера предмета: ``Gate → GateSubject | None``.
SubjectResolver = Callable[[Gate], GateSubject | None]
