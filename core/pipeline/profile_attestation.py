"""coHound — аттестация **профиля** пакета-якоря для G1/G2: дайджест, дифф, резолвер.

**Зачем.** Профиль (когорта, роли, объём, паспорта) квази-константен: прогоны идут
еженедельно, а сам профиль меняется 2–5 раз в год. Поэтому человек аттестует
**содержимое** профиля один раз, а последующие прогоны с тем же содержимым не требуют
повторного решения. Per-run путь ``gate_decisions`` остаётся: он нужен как аудит-след и
как явный override на один прогон.

**Что именно аттестуется.** Ровно :data:`PROFILE_MATERIAL_KEYS` и ничего больше:
``case_id``/``package_id``/``anchor`` (идентичность профиля), ``cohort_digest`` (состав
когорты), ``products`` (продукты плана), ``focus_modules``/``focus_params`` (объявленный
scope), ``seeds_digest`` (объём seed-набора), ``roles`` (производный набор ролей из
``role_binding`` выбранного каталога) и ``passports`` (дайджест авторских байтов
паспортов по продуктам). Метки времени, пути и порядок входа в материал **не** входят:
дайджест обязан быть стабильным между прогонами одного профиля.

**Почему каждое поле.** Правка когорты/scope/seed-набора меняет то, что человек
утверждал («кого и чем меряем»); ``roles`` — производная матрицы роль×продукт (G2);
``passports`` — «паспорта фиксируются на цикл», поэтому правка паспорта обязана требовать
пере-аттестации, а не проходить молча.

**Диагностика блока.** Если аттестация есть, но дайджест не совпал, сообщение гейта
перечисляет изменившиеся компоненты (а для списков/словарей — добавленные/убранные
элементы). Молчаливое «нет строки» вынуждало бы человека пере-аттестовывать вслепую —
это и есть провал, который здесь закрыт.

Нейтрален к предметной категории (§1.3): ни названий продуктов, ни вендоров.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from core.domain import Gate
from core.ports.state import ProductStatePort
from core.util.hashing import short_hash

__all__ = [
    "PROFILE_MATERIAL_KEYS",
    "ProfileGateResult",
    "describe_material_diff",
    "make_profile_gate",
    "material_digest",
    "profile_attestation_id",
    "profile_digest",
    "profile_material",
]

#: Компоненты материала дайджеста профиля (порядок — документационный; хеш канонический).
PROFILE_MATERIAL_KEYS: tuple[str, ...] = (
    "case_id",
    "package_id",
    "anchor",
    "cohort_digest",
    "products",
    "focus_modules",
    "focus_params",
    "seeds_digest",
    "roles",
    "passports",
)

#: Длина дайджеста профиля (как у прочих дайджестов идентичности, §12.2).
_DIGEST_LEN = 24

#: Гейты, к которым применима аттестация профиля.
_PROFILE_GATES: frozenset[Gate] = frozenset({Gate.G1, Gate.G2})

#: Имя команды, которой аттестуется профиль (вводится отдельной полосой работ — CLI).
_ATTEST_COMMAND = "hound profile attest"


def _canonical(payload: Any) -> str:
    """Канонический JSON: стабильные ключи/разделители, UTF-8 без escape."""
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _sorted_unique(values: Iterable[str]) -> list[str]:
    """Отсортированный список уникальных строк (порядок входа не влияет на дайджест)."""
    return sorted({str(value) for value in values})


def profile_material(
    *,
    case_id: str,
    package_id: str,
    anchor: str,
    cohort_digest: str,
    products: Iterable[str],
    focus_modules: Iterable[str],
    focus_params: Iterable[str],
    seeds_digest: str,
    roles: Iterable[str],
    passports: Mapping[str, str | None],
) -> dict[str, Any]:
    """Собирает материал дайджеста профиля (ровно :data:`PROFILE_MATERIAL_KEYS`).

    Чистая функция: никакого I/O, меток времени и путей. Списки сортируются и
    дедуплицируются, ``passports`` — по ключу (это делает канонический JSON), поэтому
    порядок аргументов на результат не влияет. ``None`` в ``passports`` означает
    «у продукта нет паспортных файлов» и входит в материал как ``null`` (появление
    паспорта меняет дайджест).
    """
    return {
        "case_id": str(case_id),
        "package_id": str(package_id),
        "anchor": str(anchor),
        "cohort_digest": str(cohort_digest),
        "products": _sorted_unique(products),
        "focus_modules": _sorted_unique(focus_modules),
        "focus_params": _sorted_unique(focus_params),
        "seeds_digest": str(seeds_digest),
        "roles": _sorted_unique(roles),
        "passports": {
            str(product_id): (None if digest is None else str(digest))
            for product_id, digest in passports.items()
        },
    }


def material_digest(material: Mapping[str, Any]) -> str:
    """Дайджест готового материала профиля (24 hex).

    Материал хешируется как есть (канонический JSON), поэтому вызывающий обязан
    получить его из :func:`profile_material` — иначе сортировка/нормализация не
    гарантированы.
    """
    return short_hash(_canonical(dict(material)), _DIGEST_LEN)


def profile_digest(
    *,
    case_id: str,
    package_id: str,
    anchor: str,
    cohort_digest: str,
    products: Iterable[str],
    focus_modules: Iterable[str],
    focus_params: Iterable[str],
    seeds_digest: str,
    roles: Iterable[str],
    passports: Mapping[str, str | None],
) -> tuple[str, dict[str, Any]]:
    """Дайджест профиля и его материал: ``(digest, material)``.

    Детерминирован и стабилен между прогонами одного профиля (нет меток времени,
    путей и зависимости от порядка входа) и меняется при изменении любого компонента.
    Материал сохраняется рядом с аттестацией (``profile_attestation.digest_material``),
    чтобы при расхождении назвать изменившиеся компоненты.
    """
    material = profile_material(
        case_id=case_id,
        package_id=package_id,
        anchor=anchor,
        cohort_digest=cohort_digest,
        products=products,
        focus_modules=focus_modules,
        focus_params=focus_params,
        seeds_digest=seeds_digest,
        roles=roles,
        passports=passports,
    )
    return material_digest(material), material


def profile_attestation_id(
    *,
    case_id: str,
    package_id: str,
    anchor: str,
    gate: Gate,
    profile_digest: str,
    attested_by: str,
) -> str:
    """Детерминированный ``attestation_id`` аттестации профиля.

    Ключ — ``(case_id, package_id, anchor, gate, profile_digest, attested_by)`` **без**
    метки времени: повторная подача того же решения идемпотентно перезаписывает ту же
    строку, а другое решение (другой человек/другой профиль) сохраняется отдельно —
    аудит-след не теряется. Формула — единственная: CLI-полоса обязана использовать её же.
    """
    raw = (
        f"{case_id}|{package_id}|{anchor}|{Gate(gate).value}"
        f"|{profile_digest}|{attested_by}"
    )
    return "pa_" + short_hash(raw, 24)


def _shorten(value: Any) -> str:
    """Короткое представление значения для сообщения (дайджесты — 12 символов)."""
    text = str(value)
    return text[:12] if len(text) > 16 else text


def _describe_component(key: str, old: Any, new: Any) -> str:
    """Человекочитаемое описание изменения одного компонента материала."""
    if isinstance(old, list) and isinstance(new, list):
        added = [item for item in new if item not in old]
        removed = [item for item in old if item not in new]
        parts: list[str] = []
        if added:
            parts.append(f"добавлены {sorted(added)}")
        if removed:
            parts.append(f"убраны {sorted(removed)}")
        return f"{key}: " + "; ".join(parts)
    if isinstance(old, dict) and isinstance(new, dict):
        added = [item for item in new if item not in old]
        removed = [item for item in old if item not in new]
        changed = [item for item in new if item in old and new[item] != old[item]]
        parts = []
        if added:
            parts.append(f"добавлены {sorted(added)}")
        if removed:
            parts.append(f"убраны {sorted(removed)}")
        if changed:
            parts.append(f"изменены {sorted(changed)}")
        return f"{key}: " + "; ".join(parts)
    return f"{key}: {_shorten(old)} → {_shorten(new)}"


def describe_material_diff(
    previous: Mapping[str, Any], current: Mapping[str, Any]
) -> list[str]:
    """Описывает, чем текущий материал отличается от аттестованного (по компонентам).

    Возвращает список строк по изменившимся компонентам :data:`PROFILE_MATERIAL_KEYS`
    (для списков — добавленные/убранные элементы, для ``passports`` — добавленные/
    убранные/изменённые продукты, для скаляров — ``было → стало``). Пустой список —
    материалы совпадают. Компоненты, которых нет ни в старом, ни в новом материале,
    пропускаются; появившиеся/исчезнувшие компоненты называются отдельно.
    """
    changes: list[str] = []
    for key in PROFILE_MATERIAL_KEYS:
        in_old, in_new = key in previous, key in current
        if not in_old and not in_new:
            continue
        if in_old != in_new:
            changes.append(
                f"{key}: компонент {'появился' if in_new else 'исчез'} в материале"
            )
            continue
        old, new = previous[key], current[key]
        if old != new:
            changes.append(_describe_component(key, old, new))
    for key in sorted(set(previous) - set(PROFILE_MATERIAL_KEYS)):
        changes.append(f"{key}: компонент исчез из материала")
    for key in sorted(set(current) - set(PROFILE_MATERIAL_KEYS)):
        changes.append(f"{key}: в материале появился неизвестный компонент")
    return changes


@dataclass(frozen=True)
class ProfileGateResult:
    """Результат проверки гейта по аттестации профиля.

    ``passed`` — аттестация действует для текущего дайджеста. ``detail`` — причина
    блока (готовая строка ошибки фазы): она обязана называть, **что** изменилось, либо
    что аттестации нет и какой командой её создать.
    """

    passed: bool
    detail: str | None = None


def _no_attestation_message(
    gate: Gate, material: Mapping[str, Any], digest: str
) -> str:
    """Ошибка гейта: аттестации профиля нет вовсе."""
    return (
        f"гейт {gate.value} не пройден: нет строки в gate_decisions (S9) и нет "
        f"аттестации профиля (case_id={material.get('case_id')}, "
        f"package_id={material.get('package_id')}, anchor={material.get('anchor')}, "
        f"дайджест профиля {_shorten(digest)}). Профиль якоря ещё не аттестован — "
        f"аттестуйте текущий профиль: `{_ATTEST_COMMAND} --gate {gate.value} "
        f"--actor <вы>` (команда вводится отдельной полосой работ)."
    )


def _stale_attestation_message(
    gate: Gate, previous: Any, material: Mapping[str, Any], digest: str
) -> str:
    """Ошибка гейта: аттестация есть, но профиль изменился (называет компоненты)."""
    changes = describe_material_diff(previous.digest_material, material)
    changed = "; ".join(changes) if changes else "компоненты не сопоставимы"
    return (
        f"гейт {gate.value} не пройден: нет строки в gate_decisions (S9); аттестация "
        f"профиля (case_id={material.get('case_id')}, "
        f"package_id={material.get('package_id')}, anchor={material.get('anchor')}) "
        f"от {previous.attested_at} (attested_by={previous.attested_by}) устарела: "
        f"профиль изменился — {changed}. Текущий дайджест {_shorten(digest)}, "
        f"аттестованный {_shorten(previous.profile_digest)}. Пере-аттестуйте профиль: "
        f"`{_ATTEST_COMMAND} --gate {gate.value} --actor <вы>` (команда вводится "
        f"отдельной полосой работ)."
    )


def make_profile_gate(
    state: ProductStatePort, material: Mapping[str, Any]
) -> Callable[[Gate], ProfileGateResult]:
    """Строит резолвер гейта по аттестации профиля: ``Gate → ProfileGateResult``.

    :param state: durable-состояние пакета (``state.db``); именно оно переживает прогоны.
    :param material: материал дайджеста из :func:`profile_material` (тот же, что ушёл в
        ``profile_digest``). Из него берутся ``case_id``/``package_id``/``anchor``.
    :returns: замыкание без состояния: на каждый гейт читает ``state`` и решает,
        действует ли аттестация текущего дайджеста. Per-run ``gate_decisions``
        проверяется **до** него (в :func:`core.pipeline.runner._gate_phase`) и остаётся
        приоритетным путём: аудит-след и override на прогон.

    Отозванная аттестация (``superseded_at``) действующей не считается и как база для
    диффа не используется (иначе «что изменилось» ссылалось бы на отозванное решение).
    """
    digest = material_digest(material)
    case_id = str(material.get("case_id", ""))
    package_id = str(material.get("package_id", ""))
    anchor = str(material.get("anchor", ""))

    def _resolve(gate: Gate) -> ProfileGateResult:
        normalized = Gate(gate)
        if normalized not in _PROFILE_GATES:
            return ProfileGateResult(
                passed=False,
                detail=(
                    f"гейт {normalized.value} не пройден: аттестация профиля "
                    "применима только к G1/G2"
                ),
            )
        found = state.find_profile_attestation(
            case_id, package_id, anchor, normalized, digest
        )
        if found is not None and found.superseded_at is None:
            return ProfileGateResult(passed=True)
        previous = None
        for candidate in state.iter_profile_attestations(
            case_id=case_id, package_id=package_id, anchor=anchor, gate=normalized
        ):
            previous = candidate
        if previous is None:
            return ProfileGateResult(
                passed=False,
                detail=_no_attestation_message(normalized, material, digest),
            )
        return ProfileGateResult(
            passed=False,
            detail=_stale_attestation_message(normalized, previous, material, digest),
        )

    return _resolve
