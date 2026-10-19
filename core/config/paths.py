"""coHound — резолвер раскладки операционных каталогов (``config/paths.yaml``, 03 §A.1).

Модуль отделяет **операционные** каталоги (``packages/``, ``passports/``, ``runs/``,
``index_store/``, ``corpus_raw/``, ``cache/``, ``extract_cache/``, ``reports/``) от кода:
оператор может задать свои пути через ``config/paths.yaml``, env-переменные
``COHOUND_*_DIR`` и CLI-флаги, не редактируя исходники. Единый корень ``COHOUND_HOME``
позволяет унести весь рабочий каталог в произвольное место, оставив относительные
дефолты для CWD.

**Приоритет (для каждого поля):** ``explicit`` (CLI-флаг) > ``env``
(``COHOUND_<FIELD>_DIR``) > значение из ``paths.yaml`` > дефолт модели.

**Дом (``COHOUND_HOME``).** Разрешается как ``home`` (аргумент) > ``env[COHOUND_HOME]``
> ``home:`` из файла > ``None``. Если дом задан, все **относительные** пути (дефолты,
из файла, из env, из explicit) считаются от него и возвращаются абсолютными
(``home / p``); абсолютные пути остаются как есть. Без дома пути возвращаются как есть
(относительные — относительно CWD; поведение не меняется).

Модуль только **читает и типизирует** пути. Он не создаёт каталоги и не знает о
профиле развёртывания; семантика раскладки — данные. Env читается вызывающим и
передаётся в :func:`resolve_paths` явно (тестируемость); дефолт — ``os.environ``.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров; имена
каталогов — просто данные. Допустимые зависимости слоя ``config`` —
``domain``/``config``/``util`` (01 §1).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

__all__ = [
    # Env-константы
    "ENV_HOME",
    "ENV_PATHS_FILE",
    "ENV_PACKAGES_DIR",
    "ENV_PASSPORTS_DIR",
    "ENV_RUNS_DIR",
    "ENV_INDEX_STORE_DIR",
    "ENV_CORPUS_RAW_DIR",
    "ENV_CACHE_DIR",
    "ENV_EXTRACT_CACHE_DIR",
    "ENV_REPORTS_DIR",
    # Модель и API
    "PathsConfig",
    "HARNESS_FIELDS",
    "DEFAULT_PATHS_FILE",
    "load_paths_file",
    "with_home",
    "resolve_paths",
]

# ---------------------------------------------------------------------------
# Env-константы (единый префикс COHOUND_)
# ---------------------------------------------------------------------------

#: Единый корень операционной раскладки; относительные пути считаются от него.
ENV_HOME = "COHOUND_HOME"
#: Путь к ``paths.yaml`` (иначе канонический :data:`DEFAULT_PATHS_FILE`).
ENV_PATHS_FILE = "COHOUND_PATHS_FILE"
ENV_PACKAGES_DIR = "COHOUND_PACKAGES_DIR"
ENV_PASSPORTS_DIR = "COHOUND_PASSPORTS_DIR"
ENV_RUNS_DIR = "COHOUND_RUNS_DIR"
ENV_INDEX_STORE_DIR = "COHOUND_INDEX_STORE_DIR"
ENV_CORPUS_RAW_DIR = "COHOUND_CORPUS_RAW_DIR"
ENV_CACHE_DIR = "COHOUND_CACHE_DIR"
ENV_EXTRACT_CACHE_DIR = "COHOUND_EXTRACT_CACHE_DIR"
ENV_REPORTS_DIR = "COHOUND_REPORTS_DIR"

#: Канонический путь ``paths.yaml`` относительно CWD (раскладка A.1).
DEFAULT_PATHS_FILE: Path = Path("config/paths.yaml")

#: Поля раскладки в детерминированном порядке (порядок совпадает с документом).
HARNESS_FIELDS: tuple[str, ...] = (
    "packages",
    "passports",
    "runs",
    "index_store",
    "corpus_raw",
    "cache",
    "extract_cache",
    "reports",
)

#: Соответствие поля раскладки → env-переменная (``COHOUND_<FIELD>_DIR``).
ENV_BY_FIELD: dict[str, str] = {
    "packages": ENV_PACKAGES_DIR,
    "passports": ENV_PASSPORTS_DIR,
    "runs": ENV_RUNS_DIR,
    "index_store": ENV_INDEX_STORE_DIR,
    "corpus_raw": ENV_CORPUS_RAW_DIR,
    "cache": ENV_CACHE_DIR,
    "extract_cache": ENV_EXTRACT_CACHE_DIR,
    "reports": ENV_REPORTS_DIR,
}

#: Спец-ключ документа ``paths.yaml``: корень раскладки (не поле :class:`PathsConfig`).
_HOME_KEY = "home"


# ---------------------------------------------------------------------------
# Модель
# ---------------------------------------------------------------------------


class PathsConfig(BaseModel):
    """Раскладка операционных каталогов (03 §A.1).

    Все поля — :class:`pathlib.Path`; дефолты относительны CWD и совпадают с
    канонической раскладкой. ``extra="forbid"``: неизвестные ключи в ``paths.yaml`` —
    ошибка (не тихий игнор). Спец-ключ ``home`` отфильтровывается до валидации
    (:func:`load_paths_file`/:func:`resolve_paths`) и полем не является.
    """

    model_config = ConfigDict(extra="forbid")

    packages: Path = Path("packages")
    passports: Path = Path("passports")
    runs: Path = Path("runs")
    index_store: Path = Path("index_store")
    corpus_raw: Path = Path("corpus_raw")
    cache: Path = Path("cache")
    #: Межпрогонный кэш экстракции (``core/pipeline/extract_cache.py``): контент-адресуемые
    #: результаты извлечения документов. Отдельно от ``cache`` (SERP/HTTP), т.к. это не
    #: сетевой кэш и у него свой жизненный цикл/бюджет.
    extract_cache: Path = Path("extract_cache")
    reports: Path = Path("reports")


# ---------------------------------------------------------------------------
# Низкоуровневые помощники
# ---------------------------------------------------------------------------


def _read_yaml(path: Path) -> Any:
    """Читает YAML-файл; ошибки I/O и синтаксиса приводит к ``ValueError`` с путём."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {path}: {exc}") from exc


def _clean(value: Any) -> str | None:
    """Нормализует значение env/дома: пустое/пробельное → ``None``, иначе ``str``."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------


def load_paths_file(path: str | Path | None = None) -> dict[str, Any]:
    """Читает ``paths.yaml`` сырым разбором (для тестов и :func:`resolve_paths`).

    ``path=None`` → :data:`DEFAULT_PATHS_FILE`. Отсутствие файла — **не** ошибка:
    возвращается ``{}`` (действуют дефолты). Невалидный YAML или документ не-словарь —
    :class:`ValueError` с указанием файла. Спец-ключ ``home`` **сохраняется** в
    результате (сырой разбор); :func:`resolve_paths` отфильтровывает его до валидации.

    :raises ValueError: файл не читается / YAML-синтаксис нарушен / документ не словарь.
    """
    target = DEFAULT_PATHS_FILE if path is None else Path(path)
    if not target.exists():
        return {}
    data = _read_yaml(target)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"{target}: ожидался YAML-словарь, получено {type(data).__name__}"
        )
    return dict(data)


def with_home(config: PathsConfig, home: str | Path) -> PathsConfig:
    """Нормализует раскладку относительно ``home`` (относительные → абсолютные).

    Относительный путь ``p`` возвращается как ``home / p``; абсолютный — без изменений.
    Дом не создаётся и не проверяется — только приклеивается к относительным путям.
    """
    base = Path(home)
    values: dict[str, Path] = {}
    for field in HARNESS_FIELDS:
        current = getattr(config, field)
        values[field] = current if current.is_absolute() else base / current
    return PathsConfig(**values)


def resolve_paths(
    *,
    explicit: Mapping[str, str | Path] | None = None,
    file: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    home: str | Path | None = None,
) -> PathsConfig:
    """Резолвит раскладку операционных каталогов (03 §A.1).

    Приоритет для каждого поля: ``explicit[field]`` > ``env[COHOUND_<FIELD>_DIR]`` >
    значение из ``paths.yaml`` > дефолт модели. Дом разрешается как ``home`` >
    ``env[COHOUND_HOME]`` > ``home:`` из файла > ``None``; при заданном доме
    относительные пути становятся абсолютными от него (:func:`with_home`).

    :param explicit: переопределения (CLI-флаги) ``{field: path}``; неизвестные ключи
        игнорируются (набор фиксирован :data:`HARNESS_FIELDS`).
    :param file: путь к ``paths.yaml``; ``None`` → ``env[COHOUND_PATHS_FILE]`` либо
        :data:`DEFAULT_PATHS_FILE`. Отсутствие файла — не ошибка.
    :param env: отображение окружения; ``None`` → :data:`os.environ`.
    :param home: явный корень раскладки; ``None`` → env/файл.
    :raises ValueError: невалидный ``paths.yaml`` (синтаксис, не-словарь, неизвестные ключи).
    """
    resolved_env: Mapping[str, str] = os.environ if env is None else env

    file_target = (
        file
        if file is not None
        else _clean(resolved_env.get(ENV_PATHS_FILE)) or DEFAULT_PATHS_FILE
    )
    document = load_paths_file(file_target)
    file_home = _clean(document.pop(_HOME_KEY, None))
    # ``None``-значения полей (пустой ключ YAML) трактуем как «нет значения» → дефолт.
    document = {key: value for key, value in document.items() if value is not None}
    try:
        file_config = PathsConfig.model_validate(document)
    except ValidationError as exc:
        raise ValueError(f"некорректный {file_target}: {exc}") from exc

    resolved_home = _clean(home) or _clean(resolved_env.get(ENV_HOME)) or file_home

    explicit_map = explicit or {}
    updates: dict[str, Any] = {}
    for field in HARNESS_FIELDS:
        chosen = explicit_map.get(field)
        if chosen is not None:
            updates[field] = chosen
            continue
        env_value = _clean(resolved_env.get(ENV_BY_FIELD[field]))
        if env_value is not None:
            updates[field] = env_value

    if updates:
        merged = {field: getattr(file_config, field) for field in HARNESS_FIELDS}
        merged.update(updates)
        config = PathsConfig(**merged)
    else:
        config = file_config

    if resolved_home is not None:
        config = with_home(config, resolved_home)
    return config
