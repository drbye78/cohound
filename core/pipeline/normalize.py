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

``golden.csv`` — **проверочный** вход, а не условие измерения: он не входит в
``execution_contract_digest`` и на вердикты не влияет, поэтому его отсутствие — законный
режим («сверять не с чем»), а не сбой загрузки. Состояние эталона объявляется файлом
``sources/golden_status.yaml`` (:mod:`core.pipeline.golden_status`): ``absent`` — файла
нет, ``present`` — есть и используется, ``declined`` — файл есть, но объявлен
непригодным (сверка не выполняется, файл остаётся). При ``absent``/``declined``
:attr:`RunInputs.golden` пуст по построению: набор, который не используется, не является
входом сверки.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from core.config.crawl_sources import CrawlSourcesConfig, load_crawl_sources
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
    ScopeAttestation,
)
from core.pipeline.golden_status import GoldenStatus, read_golden_status

__all__ = [
    "SCOPE_ATTESTATION_FILE",
    "RunInputs",
    "cross_check",
    "load_inputs",
    "load_scope_attestation",
    "load_scope_attestations",
]

#: Имя опционального файла пакета с процедурой аттестации ``source_scope`` (I2b).
SCOPE_ATTESTATION_FILE = "scope_attestation.yaml"


@dataclass
class RunInputs:
    """Нормализованные входы прогона: все артефакты пакета + ``run.yaml``.

    :param golden: эталонные строки, **участвующие в сверке**. Пусто при
        ``golden_status.state ∈ {absent, declined}``: набор, объявленный непригодным или
        отсутствующий, не является входом сверки (см.
        :mod:`core.pipeline.golden_status`).
    :param golden_status: объявленное состояние эталона (``absent``/``present``/
        ``declined`` + причина и автор для ``declined``). ``None`` — состояние не
        определялось (входы собраны в обход :func:`load_inputs`, напр. в тестах): линтер
        тогда работает по прежнему правилу «пустой список = нет сети».
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
    crawl_sources: CrawlSourcesConfig = field(default_factory=CrawlSourcesConfig)
    scope_attestation_procedure: str | None = None
    golden_status: GoldenStatus | None = None


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


def _known_product_ids(package_dir: Path) -> set[str] | None:
    """Продукты пакета из ``cohort.yaml``; ``None`` — когорты нет (проверять не по чему)."""
    cohort_path = package_dir / "cohort.yaml"
    if not cohort_path.is_file():
        return None
    try:
        cohort = load_cohort(cohort_path)
    except (ValueError, OSError):
        # Когорта не читается — её разберёт обязательный путь загрузки; здесь не
        # подменяем ошибку и не выдумываем «продукт известен».
        return None
    return {entry.product_id for entry in cohort.entries}


def load_scope_attestations(
    package_dir: str | Path,
) -> dict[str, ScopeAttestation]:
    """Читает опциональные подписи аттестации охвата из ``scope_attestation.yaml`` (I2b/I2c).

    Формат (обратная совместимость: ``procedure`` остаётся, ``attestations`` опционален)::

        procedure: "описание процедуры аттестации полноты источника"
        attestations:
          - product_id: p1
            attested_by: analyst
            attested_at: "2026-01-01T00:00:00+00:00"
            snapshot_date: "2026-01-01"
            notes: "сверено со справочником"

    Отсутствие файла или ключа ``attestations`` → ``{}``: механизм опционален, его
    отсутствие не ошибка (ровно как у ``load_scope_attestation``).

    :raises ValueError: корень не словарь; ``attestations`` не список; элемент не
        словарь/невалиден; пустой ``attested_by``; дубликат ``product_id``; продукт,
        которого нет в ``cohort.yaml`` (если когорта читается).
    """
    path = Path(package_dir) / SCOPE_ATTESTATION_FILE
    if not path.exists():
        return {}
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
    raw = data.get("attestations")
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise ValueError(
            f"{SCOPE_ATTESTATION_FILE}: attestations обязан быть списком, "
            f"получено {type(raw).__name__}"
        )

    known = _known_product_ids(Path(package_dir))
    result: dict[str, ScopeAttestation] = {}
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(
                f"{SCOPE_ATTESTATION_FILE}: attestations[{index}] обязан быть словарём, "
                f"получено {type(item).__name__}"
            )
        try:
            attestation = ScopeAttestation.model_validate(item)
        except ValueError as exc:
            raise ValueError(
                f"{SCOPE_ATTESTATION_FILE}: attestations[{index}]: {exc}"
            ) from exc
        if attestation.product_id in result:
            raise ValueError(
                f"{SCOPE_ATTESTATION_FILE}: дубликат product_id "
                f"{attestation.product_id!r} в attestations"
            )
        if known is not None and attestation.product_id not in known:
            raise ValueError(
                f"{SCOPE_ATTESTATION_FILE}: неизвестный product_id "
                f"{attestation.product_id!r} (нет в cohort.yaml)"
            )
        result[attestation.product_id] = attestation
    return result


def load_inputs(run_yaml_path: str | Path, package_dir: str | Path) -> RunInputs:
    """Нормализует входы прогона: ``run.yaml`` + артефакты каталога пакета.

    Обязательны ``run.yaml`` и четыре артефакта пакета (``package.yaml``,
    ``catalog.csv``, ``baselines.csv``, ``cohort.yaml``); ``domain_profiles.yaml``,
    ``crawl_sources.yaml``, ``scope_attestation.yaml`` и ``sources/golden_status.yaml`` —
    опциональны.

    ``golden.csv`` читается **только** если эталон объявлен действующим
    (``golden_status.state == present``, в т.ч. выведенным из наличия файла при
    отсутствии объявления). При ``absent``/``declined`` сверка не выполняется, поэтому
    строки не читаются вовсе: непригодный файл может быть и не разбираемым, а его форма
    не является входом прогона. Состояние при этом объявлено и видно вызывающему.

    :raises ValueError: артефакт отсутствует или не разбирается/не валиден; объявление
        состояния эталона противоречиво (:mod:`core.pipeline.golden_status`).
    """
    package_path = Path(package_dir)
    run = load_run_config(run_yaml_path)
    package = load_package_manifest(package_path / "package.yaml")
    catalog = load_catalog(package_path / "catalog.csv")
    baselines = load_baselines(package_path / "baselines.csv")
    cohort = load_cohort(package_path / "cohort.yaml")
    golden_status = read_golden_status(package_path)
    golden = (
        load_golden(package_path / "golden.csv")
        if golden_status.comparison_available
        else []
    )

    profiles_path = package_path / "domain_profiles.yaml"
    domain_profiles = load_domain_profiles(profiles_path) if profiles_path.exists() else {}

    crawl_path = package_path / "crawl_sources.yaml"
    crawl_sources = load_crawl_sources(crawl_path) if crawl_path.exists() else CrawlSourcesConfig()

    return RunInputs(
        run=run,
        package=package,
        catalog=catalog,
        baselines=baselines,
        cohort=cohort,
        golden=golden,
        domain_profiles=domain_profiles,
        crawl_sources=crawl_sources,
        scope_attestation_procedure=load_scope_attestation(package_path),
        golden_status=golden_status,
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
