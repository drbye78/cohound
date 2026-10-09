"""coHound — конфигурация и входы (L1, W5/T5.1).

Пакет загрузки артефактов раскладки на диске (``03-data-config-cli.md`` §A.1/D.2):
``run.yaml``, ``package.yaml``, ``catalog.csv``, ``baselines.csv``, ``cohort.yaml``,
``golden.csv``, ``domain_profiles.yaml``. Загрузчики валидируют данные в **доменные
модели** (``core/domain/models.py``) и не несут предметной терминологии (§1.3).

Разрешённые зависимости слоя ``config`` — ``domain``/``config``/``util`` (01 §1).
Поэтому пакет чист от I/O-портов и конвейера: он читает файлы, но не ходит в сеть
и не пишет в хранилище.
"""

from __future__ import annotations

from core.config.run_config import (
    cross_check_products,
    load_baselines,
    load_catalog,
    load_cohort,
    load_domain_profiles,
    load_golden,
    load_package_manifest,
    load_run_config,
)
from core.config.deploy import (
    DeployConfig,
    is_split,
    load_deploy_config,
)
from core.config.passports import (
    PassportLoadError,
    load_passport,
    load_passports,
    passport_urls,
)
from core.config.paths import (
    DEFAULT_PATHS_FILE,
    ENV_CACHE_DIR,
    ENV_CORPUS_RAW_DIR,
    ENV_HOME,
    ENV_INDEX_STORE_DIR,
    ENV_PACKAGES_DIR,
    ENV_PASSPORTS_DIR,
    ENV_PATHS_FILE,
    ENV_REGISTRIES_DIR,
    ENV_REPORTS_DIR,
    ENV_RUNS_DIR,
    HARNESS_FIELDS,
    PathsConfig,
    load_paths_file,
    resolve_paths,
    with_home,
)

__all__ = [
    # run.yaml (§4.5)
    "load_run_config",
    # package.yaml (§4.2)
    "load_package_manifest",
    # catalog.csv (11 колонок, §4.2)
    "load_catalog",
    # baselines.csv (§4.2)
    "load_baselines",
    # cohort.yaml (§1.6)
    "load_cohort",
    # golden.csv (§4.2/§11.6)
    "load_golden",
    # domain_profiles.yaml (D.2)
    "load_domain_profiles",
    # кросс-артефактная проверка S11 (§4.5)
    "cross_check_products",
    # deploy.yaml (D.5, 07)
    "load_deploy_config",
    "DeployConfig",
    "is_split",
    # passports/{product_id}/{date}.yaml (§4.3)
    "load_passport",
    "load_passports",
    "passport_urls",
    "PassportLoadError",
    # paths.yaml — раскладка операционных каталогов (03 §A.1)
    "PathsConfig",
    "resolve_paths",
    "load_paths_file",
    "with_home",
    "HARNESS_FIELDS",
    "DEFAULT_PATHS_FILE",
    "ENV_HOME",
    "ENV_PATHS_FILE",
    "ENV_PACKAGES_DIR",
    "ENV_PASSPORTS_DIR",
    "ENV_RUNS_DIR",
    "ENV_INDEX_STORE_DIR",
    "ENV_CORPUS_RAW_DIR",
    "ENV_CACHE_DIR",
    "ENV_REGISTRIES_DIR",
    "ENV_REPORTS_DIR",
]
