"""coHound — нормализация входов прогона (W5/T5.4, ``03`` §D.2/D.3).

Собирает все артефакты пакета категории в один объект :class:`RunInputs`, который
потребляют линтер (:mod:`core.pipeline.lint`) и планер (:mod:`core.pipeline.plan`).
Парсинг делегирован загрузчикам ``core.config.run_config`` (T5.1) — здесь нет
дублирования разбора YAML/CSV.

Раскладка пакета (``03`` §A.1): ``package.yaml``, ``catalog.csv``, ``baselines.csv``,
``cohort.yaml``, ``golden.csv``, ``domain_profiles.yaml``. Дополнительно читается
**опциональный** ``scope_attestation.yaml`` с полем ``procedure`` — пакетная настройка
процедуры аттестации полноты источника для ``absence_rule`` I2b (см. ``lint`` и
:data:`RunInputs.scope_attestation_procedure`).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from core.config.domain_profiles import DomainProfile, load_domain_profiles
from core.config.run_config import (
    cross_check_products,
    load_baselines,
    load_catalog,
    load_cohort,
    load_golden,
    load_package_manifest,
    load_run_config,
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
    "RunInputs",
    "load_inputs",
    "load_scope_attestation",
    "cross_check",
    "SCOPE_ATTESTATION_FILE",
]

#: Имя опционального файла пакета с процедурой аттестации ``source_scope`` (I2b).
SCOPE_ATTESTATION_FILE = "scope_attestation.yaml"


@dataclass
class RunInputs:
    """Нормализованные входы прогона: все артефакты пакета + ``run.yaml``.

    :param scope_attestation_procedure: описание действующей процедуры аттестации
        полноты источника (человеческая проверка «справочник действительно полон»).
        ``None``/пусто означает, что механизм не настроен: параметры с
        ``absence_rule ∈ {parameter_reference, distribution_absence}`` тогда
        блокируются линтером (§4.2/I2b). Загружается из опционального
        ``scope_attestation.yaml`` каталога пакета; при отсутствии — ``None``.
    """

    run: RunConfig
    package: PackageManifest
    catalog: list[CatalogRow]
    baselines: list[BaselineRow]
    cohort: Cohort
    golden: list[GoldenRow]
    domain_profiles: dict[str, DomainProfile] = field(default_factory=dict)
    scope_attestation_procedure: str | None = None


def load_scope_attestation(package_dir: str | Path) -> str | None:
    """Читает опциональный ``scope_attestation.yaml`` пакета → строка процедуры.

    Формат::

        procedure: "описание процедуры аттестации полноты источника"

    Отсутствие файла или пустое значение ``procedure`` → ``None`` (механизм не
    настроен). Не-словарный корень или не-словарь — :class:`ValueError`.
    """
    path = Path(package_dir) / SCOPE_ATTESTATION_FILE
    if not path.exists():
        return None
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(
            f"{SCOPE_ATTESTATION_FILE}: ожидался YAML-словарь, "
            f"получено {type(data).__name__}"
        )
    raw = data.get("procedure")
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def load_inputs(run_yaml_path: str | Path, package_dir: str | Path) -> RunInputs:
    """Нормализует входы прогона: ``run.yaml`` + артефакты каталога пакета.

    Обязательны ``run.yaml`` и пять артефактов пакета (``package.yaml``,
    ``catalog.csv``, ``baselines.csv``, ``cohort.yaml``, ``golden.csv``);
    ``domain_profiles.yaml`` и ``scope_attestation.yaml`` — опциональны.

    :raises ValueError: артефакт отсутствует или не разбирается/не валиден.
    """
    package_path = Path(package_dir)
    run = load_run_config(run_yaml_path)
    package = load_package_manifest(package_path / "package.yaml")
    catalog = load_catalog(package_path / "catalog.csv")
    baselines = load_baselines(package_path / "baselines.csv")
    cohort = load_cohort(package_path / "cohort.yaml")
    golden = load_golden(package_path / "golden.csv")

    profiles_path = package_path / "domain_profiles.yaml"
    domain_profiles = load_domain_profiles(profiles_path) if profiles_path.exists() else {}

    return RunInputs(
        run=run,
        package=package,
        catalog=catalog,
        baselines=baselines,
        cohort=cohort,
        golden=golden,
        domain_profiles=domain_profiles,
        scope_attestation_procedure=load_scope_attestation(package_path),
    )


def cross_check(inputs: RunInputs) -> list[str]:
    """Кросс-артефактные проверки входов (§4.5/§1.6).

    S11 — ``products`` совпадает с множеством ``product_id`` когорты (канонический
    источник состава); плюс ``anchor ∈ products``. Возвращает список нарушений;
    пустой список — согласованность. Блокирующую силу придаёт линтер.
    """
    violations = list(cross_check_products(inputs.run, inputs.cohort))
    if inputs.run.anchor not in inputs.run.products:
        violations.append(
            f"§4.5: anchor '{inputs.run.anchor}' обязан входить в products"
        )
    return violations
