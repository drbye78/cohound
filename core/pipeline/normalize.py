"""coHound — нормализация входов прогона (W5/T5.4, ``03`` §D.2/D.3).

Собирает все артефакты пакета категории в один объект :class:`RunInputs`, который
потребляют линтер (:mod:`core.pipeline.lint`) и планер (:mod:`core.pipeline.plan`).
Парсинг делегирован загрузчикам ``core.config.run_config`` (T5.1) — здесь нет
дублирования разбора YAML/CSV.

Раскладка пакета (``03`` §A.1): ``package.yaml``, ``catalog.csv``, ``baselines.csv``,
``cohort.yaml``, ``golden.csv``, ``domain_profiles.yaml``. ``scope_attestation.yaml``
больше **не читается**: per-cell подпись снята методологией 0.4.0 (публикацию ``❌``
авторизует гейт G4), а файл в пакете остаётся данными, а не входом прогона.

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
from typing import Any

from core.config.crawl_policy import CrawlPolicyConfig, load_crawl_policy
from core.config.crawl_sources import CrawlSourcesConfig, load_crawl_sources
from core.config.domain_profiles import DomainProfile, load_domain_profiles
from core.config.run_config import (
    cross_check_products,
    load_baseline_facts,
    load_baselines,
    load_catalog,
    load_cohort,
    load_golden,
    load_package_manifest,
    load_reference_products,
    load_run_config,
)
from core.domain.models import (
    BaselineFact,
    BaselineRow,
    CatalogRow,
    Cohort,
    GoldenRow,
    PackageManifest,
    ReferenceProducts,
    RunConfig,
)
from core.pipeline.golden_status import GoldenStatus, read_golden_status

__all__ = [
    "RunInputs",
    "cross_check",
    "load_inputs",
]


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
    """

    run: RunConfig
    package: PackageManifest
    catalog: list[CatalogRow]
    baselines: list[BaselineRow]
    cohort: Cohort
    golden: list[GoldenRow]
    domain_profiles: dict[str, DomainProfile] = field(default_factory=dict)
    crawl_sources: CrawlSourcesConfig = field(default_factory=CrawlSourcesConfig)
    #: Дефолты политики краула из ``crawl_policy.yaml`` (план 19 §4). Пустой конфиг —
    #: «пакет политики не объявлял»; эффективная политика всё равно резолвится (пакет +
    #: ``state.db``), но без этого поля ``plan`` и ``run`` видели бы разные дефолты.
    crawl_policy: CrawlPolicyConfig = field(default_factory=CrawlPolicyConfig)
    golden_status: GoldenStatus | None = None
    #: План 27 §3.1: декларация reference-продуктов профиля
    #: (``sources/reference_products.yaml``). ``None`` — файла нет: reference-продуктов
    #: профиль не объявлял, и наследование по фактам невозможно (линтер это проверит,
    #: если ``baselines.csv`` непуст).
    reference_products: ReferenceProducts | None = None
    #: План 27 §3.3: наблюдения о документации предка (``sources/baseline_facts.csv``).
    #: Пусто — фактов ещё не разведали: наследования по фактам не будет, и это не
    #: наблюдение о продукте (§1.1 плана).
    baseline_facts: list[BaselineFact] = field(default_factory=list)
    #: Паспорта продуктов (``product_id → Passport``) — заполняет composition root, когда
    #: каталог паспортов известен из раскладки. Нужны линтеру для проверки, что предки
    #: паспорта объявлены в ``reference_products.yaml`` и что их число соответствует
    #: архетипу (план 27 §3.2/§10). ``None``/пусто — проверка не выполнена (неизвестность
    #: не выдаётся за «нарушений нет»).
    passports: dict[str, Any] = field(default_factory=dict)


def load_inputs(run_yaml_path: str | Path, package_dir: str | Path) -> RunInputs:
    """Нормализует входы прогона: ``run.yaml`` + артефакты каталога пакета.

    Обязательны ``run.yaml`` и четыре артефакта пакета (``package.yaml``,
    ``catalog.csv``, ``baselines.csv``, ``cohort.yaml``); ``domain_profiles.yaml``,
    ``crawl_sources.yaml``, ``crawl_policy.yaml`` и ``sources/golden_status.yaml`` —
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

    policy_path = package_path / "crawl_policy.yaml"
    crawl_policy = (
        load_crawl_policy(policy_path) if policy_path.exists() else CrawlPolicyConfig()
    )

    reference_path = package_path / "sources" / "reference_products.yaml"
    reference_products = (
        load_reference_products(reference_path) if reference_path.exists() else None
    )
    facts_path = package_path / "sources" / "baseline_facts.csv"
    baseline_facts = load_baseline_facts(facts_path) if facts_path.exists() else []

    return RunInputs(
        run=run,
        package=package,
        catalog=catalog,
        baselines=baselines,
        cohort=cohort,
        golden=golden,
        domain_profiles=domain_profiles,
        crawl_sources=crawl_sources,
        crawl_policy=crawl_policy,
        golden_status=golden_status,
        reference_products=reference_products,
        baseline_facts=baseline_facts,
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
