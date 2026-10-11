"""coHound — загрузчики конфигурации и входов пакета (L1, W5/T5.1).

Модуль читает артефакты раскладки на диске (``03-data-config-cli.md`` §A.1/D.2) и
**валидирует их в существующие доменные модели** (``core/domain/models.py``):

- ``run.yaml``            → :class:`~core.domain.models.RunConfig` (§4.5, D.1);
- ``package.yaml``        → :class:`~core.domain.models.PackageManifest` (§4.2);
- ``catalog.csv``         → ``list[CatalogRow]`` (11 колонок, §4.2);
- ``baselines.csv``       → ``list[BaselineRow]`` (§4.2);
- ``cohort.yaml``         → :class:`~core.domain.models.Cohort` (§1.6);
- ``golden.csv``          → ``list[GoldenRow]`` (§4.2/§11.6);
- ``domain_profiles.yaml``→ ``dict[str, DomainProfile]`` (оси fetch/extract домена, D.2 —
  загрузчик живёт в :mod:`core.config.domain_profiles`).

Загрузчики только парсят и приводят к моделям; семантические правила, требующие
сопоставления нескольких артефактов (S11, golden-full без страты ❌, showcase и т.п.),
относятся к линтеру ``pipeline/lint.py`` (T5.2) — здесь они либо уже выражены
pydantic-валидаторами домена, либо честно не применяются (см. docstring).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
Допустимые зависимости слоя ``config`` — ``domain``/``config``/``util`` (01 §1).
"""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from core.domain.enums import (
    AbsenceRuleType,
    GoldenTier,
    Importance,
    InheritanceMode,
    ParamType,
    SourceKind,
    Status,
)
from core.domain.models import (
    BaselineRow,
    CatalogRow,
    Cohort,
    GoldenRow,
    PackageManifest,
    RunConfig,
)

__all__ = [
    "load_run_config",
    "load_package_manifest",
    "load_catalog",
    "load_baselines",
    "load_cohort",
    "load_golden",
    "cross_check_products",
]

# Разделители списочных значений внутри CSV-ячейки (учитывая, что запятая — разделитель
# самого CSV, поэтому ячейка со списком обязана быть закавычена, если содержит запятые).
_ITEM_SEP = re.compile(r"[|,;\s]+")

# Канонические наборы колонок (§4.2).
_CATALOG_COLUMNS = (
    "param_id",
    "title",
    "module",
    "group",
    "importance",
    "param_type",
    "rule",
    "absence_rule",
    "role_binding",
    "concept_terms",
    "search_langs",
)
_BASELINE_COLUMNS = ("baseline_id", "param_id", "inheritance_mode")
_GOLDEN_COLUMNS = (
    "product_id",
    "param_id",
    "expected_status",
    "expected_evidence_kind",
    "case_class",
)


# ---------------------------------------------------------------------------
# Низкоуровневые помощники
# ---------------------------------------------------------------------------


def _read_yaml(path: str | Path) -> Any:
    """Читает YAML-файл; ошибки I/O и синтаксиса приводит к ``ValueError``."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {p}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {p}: {exc}") from exc


def _as_mapping(data: Any, where: str) -> dict[str, Any]:
    """Проверяет, что документ — непустой YAML-словарь."""
    if data is None:
        raise ValueError(f"{where}: пустой документ")
    if not isinstance(data, dict):
        raise ValueError(f"{where}: ожидался YAML-словарь, получено {type(data).__name__}")
    return data


def _iter_csv(
    path: str | Path, required: tuple[str, ...], where: str
) -> Iterator[tuple[int, dict[str, str | None]]]:
    """Итерирует строки CSV, проверяя наличие обязательных колонок.

    Отдаёт пары ``(номер_строки, dict)``; номер начинается с 2 (после заголовка),
    чтобы сообщения об ошибках указывали на физическую строку файла.
    """
    p = Path(path)
    try:
        with p.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []
            missing = [c for c in required if c not in fieldnames]
            if missing:
                raise ValueError(f"{where}: отсутствуют обязательные колонки {missing} в {p}")
            for lineno, row in enumerate(reader, start=2):
                yield lineno, row
    except csv.Error as exc:
        raise ValueError(f"некорректный CSV {p}: {exc}") from exc


def _req(row: dict[str, str | None], key: str) -> str:
    """Возвращает непустое строковое значение колонки."""
    value = row.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"колонка «{key}» обязательна и не может быть пустой")
    return str(value).strip()


def _opt(row: dict[str, str | None], key: str) -> str | None:
    """Возвращает опциональное значение колонки (``None`` для пустой ячейки)."""
    value = row.get(key)
    if value is None or str(value).strip() == "":
        return None
    return str(value).strip()


#: Истинные значения опциональной булевой CSV-колонки (регистр не важен).
_TRUE_VALUES = frozenset({"1", "true", "yes", "y", "да", "истина"})


def _opt_bool(row: dict[str, str | None], key: str) -> bool:
    """Разбирает опциональную булеву колонку; пустое/отсутствующее значение → ``False``."""
    value = _opt(row, key)
    return value is not None and value.lower() in _TRUE_VALUES


def _split_items(raw: Any) -> list[str]:
    """Разбирает списочное значение: готовый список, JSON-массив или строка с разделителями."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    text = str(raw).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"некорректный JSON-список: {exc}") from exc
        if not isinstance(data, list):
            raise ValueError("ожидался JSON-массив")
        return [str(item).strip() for item in data if str(item).strip()]
    return [part for part in _ITEM_SEP.split(text) if part]


def _parse_concept_terms(raw: Any) -> dict[str, list[str]]:
    """Разбирает ``concept_terms`` (типизировано как map списков, §4.2).

    Поддерживаются две формы записи ячейки:

    - JSON-объект: ``{"ru": ["a", "b"], "en": ["c"]}``;
    - компактная форма: ``ru=a|b;en=c`` (языки через ``;``, термины через ``|``).
    """
    if raw is None:
        return {}
    text = str(raw).strip()
    if not text:
        return {}
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"concept_terms: некорректный JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("concept_terms: ожидался JSON-объект {lang: [terms]}")
        return {str(lang): _split_items(terms) for lang, terms in data.items()}

    result: dict[str, list[str]] = {}
    for chunk in text.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" not in chunk:
            raise ValueError(f"concept_terms: ожидался формат «lang=term1|term2», получено «{chunk}»")
        lang, terms = chunk.split("=", 1)
        lang = lang.strip()
        if not lang:
            raise ValueError("concept_terms: пустой код языка")
        result[lang] = _split_items(terms)
    return result


# ---------------------------------------------------------------------------
# Публичные загрузчики
# ---------------------------------------------------------------------------


def load_run_config(path: str | Path) -> RunConfig:
    """Загружает ``run.yaml`` (§4.5, D.1) в доменный :class:`RunConfig`.

    Pydantic-валидаторы ``RunConfig`` обеспечивают: ``anchor ∈ products``,
    уникальность ``products`` и запрет ``preliminary`` + ``capability_ranked`` (I13).
    Согласованность ``products`` с когортой (S11) проверяет :func:`cross_check_products`.
    """
    data = _as_mapping(_read_yaml(path), "run.yaml")
    return RunConfig.model_validate(data)


def load_package_manifest(path: str | Path) -> PackageManifest:
    """Загружает ``package.yaml`` (§4.2) в :class:`PackageManifest`.

    Валидаторы домена обеспечивают: сумма весов модулей = 100, уникальность
    ``module_id``/``role_id``, ``baseline_participant is None`` для архетипа C.
    """
    data = _as_mapping(_read_yaml(path), "package.yaml")
    return PackageManifest.model_validate(data)


def load_catalog(path: str | Path) -> list[CatalogRow]:
    """Загружает ``catalog.csv`` (11 колонок, §4.2) в ``list[CatalogRow]``.

    Все 11 колонок обязаны присутствовать в заголовке; дополнительные игнорируются.
    Опциональные колонки ``deprecated``/``replaced_by`` (§13) читаются, если присутствуют;
    пакет без них грузится с дефолтами (``False``/``None``) — обратная совместимость.
    Валидаторы домена: ``param_id`` — slug в нижнем регистре; ``showcase`` → ``rule``
    упоминает публичную величину/метрику.
    """
    rows: list[CatalogRow] = []
    for lineno, row in _iter_csv(path, _CATALOG_COLUMNS, "catalog.csv"):
        try:
            rows.append(_catalog_row(row))
        except (ValidationError, ValueError) as exc:
            raise ValueError(f"catalog.csv строка {lineno}: {exc}") from exc
    return rows


def _catalog_row(row: dict[str, str | None]) -> CatalogRow:
    """Преобразует одну CSV-строку каталога в :class:`CatalogRow`."""
    return CatalogRow(
        param_id=_req(row, "param_id"),
        title=_req(row, "title"),
        module=_req(row, "module"),
        group=_req(row, "group"),
        importance=Importance(_req(row, "importance")),
        param_type=ParamType(_req(row, "param_type")),
        rule=_req(row, "rule"),
        absence_rule=AbsenceRuleType(_opt(row, "absence_rule") or AbsenceRuleType.EXPLICIT),
        role_binding=_opt(row, "role_binding"),
        concept_terms=_parse_concept_terms(row.get("concept_terms")),
        search_langs=_split_items(row.get("search_langs")),
        # §13: опциональные колонки; при отсутствии — дефолтные значения модели.
        deprecated=_opt_bool(row, "deprecated"),
        replaced_by=_opt(row, "replaced_by"),
    )


def load_baselines(path: str | Path) -> list[BaselineRow]:
    """Загружает ``baselines.csv`` (§4.2) в ``list[BaselineRow]``."""
    rows: list[BaselineRow] = []
    for lineno, row in _iter_csv(path, _BASELINE_COLUMNS, "baselines.csv"):
        try:
            rows.append(
                BaselineRow(
                    baseline_id=_req(row, "baseline_id"),
                    param_id=_req(row, "param_id"),
                    min_version=_opt(row, "min_version"),
                    inheritance_mode=InheritanceMode(
                        _req(row, "inheritance_mode")
                        if row.get("inheritance_mode")
                        else InheritanceMode.DIRECT
                    ),
                    note=_opt(row, "note"),
                )
            )
        except (ValidationError, ValueError) as exc:
            raise ValueError(f"baselines.csv строка {lineno}: {exc}") from exc
    return rows


def load_cohort(path: str | Path) -> Cohort:
    """Загружает ``cohort.yaml`` (§1.6) в :class:`Cohort`.

    Валидатор домена обеспечивает ровно один ``anchor_relation: anchor`` и
    уникальность ``product_id``.
    """
    data = _as_mapping(_read_yaml(path), "cohort.yaml")
    return Cohort.model_validate(data)


def load_golden(path: str | Path) -> list[GoldenRow]:
    """Загружает ``golden.csv`` (§4.2/§11.6) в ``list[GoldenRow]``.

    Колонки ``tier`` (``fast``/``full``) и ``note`` опциональны; ``tier`` по умолчанию
    ``full``. Правило «golden-full обязан содержать страту ожидаемых ❌» — **линтерное**
    (T5.2): требует анализа состава всего файла, поэтому здесь не применяется.
    """
    rows: list[GoldenRow] = []
    for lineno, row in _iter_csv(path, _GOLDEN_COLUMNS, "golden.csv"):
        try:
            tier = _opt(row, "tier")
            rows.append(
                GoldenRow(
                    product_id=_req(row, "product_id"),
                    param_id=_req(row, "param_id"),
                    expected_status=Status(_req(row, "expected_status")),
                    expected_evidence_kind=[
                        SourceKind(item) for item in _split_items(row.get("expected_evidence_kind"))
                    ],
                    case_class=_req(row, "case_class"),
                    **({"tier": GoldenTier(tier)} if tier is not None else {}),
                    note=_opt(row, "note"),
                )
            )
        except (ValidationError, ValueError) as exc:
            raise ValueError(f"golden.csv строка {lineno}: {exc}") from exc
    return rows


# ---------------------------------------------------------------------------
# Кросс-артефактные проверки (S11)
# ---------------------------------------------------------------------------


def cross_check_products(run_cfg: RunConfig, cohort: Cohort) -> list[str]:
    """Проверяет S11: ``products`` run.yaml — проекция состава когорты.

    Возвращает список строк-нарушений; пустой список означает согласованность
    (один канонический источник состава — ``cohort.yaml``, §4.5/§1.6).
    """
    cfg_products = set(run_cfg.products)
    cohort_products = {entry.product_id for entry in cohort.entries}
    violations: list[str] = []

    missing = sorted(cohort_products - cfg_products)
    if missing:
        violations.append(
            "S11: products не содержит product_id из когорты: " + ", ".join(missing)
        )
    extra = sorted(cfg_products - cohort_products)
    if extra:
        violations.append(
            "S11: products содержит product_id вне когорты: " + ", ".join(extra)
        )
    return violations
