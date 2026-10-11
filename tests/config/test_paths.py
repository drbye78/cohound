"""Тесты резолвера раскладки операционных каталогов (``config/paths.yaml``, 03 §A.1).

Проверяют: дефолты без env/файла; приоритет ``explicit > env > file > default``;
единый корень ``COHOUND_HOME`` (относительные → абсолютные, абсолютные как есть);
разрешение файла (``file``/``COHOUND_PATHS_FILE``) и спец-ключ ``home``; негативные
случаи (невалидный YAML, документ не словарь, неизвестные ключи); ``load_paths_file``
и ``with_home``; детерминизм и категорийную нейтральность модуля (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from core.config.paths import (
    ENV_BY_FIELD,
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

EXAMPLE_PATH = Path(__file__).resolve().parents[2] / "config" / "paths.example.yaml"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, data: dict[str, Any], name: str = "paths.yaml") -> Path:
    """Пишет временный ``paths.yaml`` (UTF-8, без экранирования кириллицы)."""
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


def _absent(tmp_path: Path) -> Path:
    """Путь к заведомо отсутствующему ``paths.yaml`` (тесты без файла)."""
    return tmp_path / "does_not_exist.yaml"


# ---------------------------------------------------------------------------
# Дефолты
# ---------------------------------------------------------------------------


def test_defaults_without_env_or_file(tmp_path: Path) -> None:
    """Без env/файла поля — относительные дефолты (поведение не меняется)."""
    config = resolve_paths(env={}, file=_absent(tmp_path))
    for field in HARNESS_FIELDS:
        value = getattr(config, field)
        assert not value.is_absolute()
        assert value == Path(field)


def test_defaults_field_names() -> None:
    assert set(HARNESS_FIELDS) == {
        "packages", "passports", "runs", "index_store",
        "corpus_raw", "cache", "registries", "reports",
    }
    assert set(ENV_BY_FIELD) == set(HARNESS_FIELDS)


def test_missing_file_is_not_error(tmp_path: Path) -> None:
    assert load_paths_file(_absent(tmp_path)) == {}


# ---------------------------------------------------------------------------
# Дом (COHOUND_HOME)
# ---------------------------------------------------------------------------


def test_home_argument_makes_paths_absolute(tmp_path: Path) -> None:
    home = tmp_path / "x"
    config = resolve_paths(env={}, file=_absent(tmp_path), home=home)
    for field in HARNESS_FIELDS:
        assert getattr(config, field) == home / field
        assert getattr(config, field).is_absolute()


def test_home_from_env(tmp_path: Path) -> None:
    config = resolve_paths(
        env={ENV_HOME: str(tmp_path / "x")}, file=_absent(tmp_path)
    )
    assert config.packages == tmp_path / "x" / "packages"


def test_home_from_file(tmp_path: Path) -> None:
    path = _write(tmp_path, {"home": str(tmp_path / "x"), "packages": "mypkg"})
    config = resolve_paths(env={}, file=path)
    assert config.packages == tmp_path / "x" / "mypkg"
    assert config.runs == tmp_path / "x" / "runs"


def test_home_precedence_argument_over_env_over_file(tmp_path: Path) -> None:
    path = _write(tmp_path, {"home": str(tmp_path / "file")})
    # аргумент > env: env задан, но аргумент побеждает
    config = resolve_paths(
        home=tmp_path / "arg",
        env={ENV_HOME: str(tmp_path / "env")},
        file=path,
    )
    assert config.packages == tmp_path / "arg" / "packages"
    # env > file: без аргумента побеждает env
    config = resolve_paths(env={ENV_HOME: str(tmp_path / "env")}, file=path)
    assert config.packages == tmp_path / "env" / "packages"
    # file: без аргумента/env — из файла
    config = resolve_paths(env={}, file=path)
    assert config.packages == tmp_path / "file" / "packages"


def test_absolute_paths_kept_under_home(tmp_path: Path) -> None:
    path = _write(tmp_path, {"packages": "/abs/pkgs"})
    config = resolve_paths(env={}, file=path, home=tmp_path / "x")
    assert config.packages == Path("/abs/pkgs")
    assert config.runs == tmp_path / "x" / "runs"


def test_home_empty_string_is_none(tmp_path: Path) -> None:
    config = resolve_paths(env={ENV_HOME: "   "}, file=_absent(tmp_path))
    assert not config.packages.is_absolute()


# ---------------------------------------------------------------------------
# Приоритет explicit > env > file > default
# ---------------------------------------------------------------------------


def test_priority_explicit_over_env_over_file(tmp_path: Path) -> None:
    path = _write(
        tmp_path, {"packages": "from_file", "runs": "from_file_runs", "cache": "fc"}
    )
    env = {
        ENV_PACKAGES_DIR: "/env/pkgs",
        ENV_RUNS_DIR: "/env/runs",
    }
    explicit = {"packages": "/explicit/pkgs"}
    config = resolve_paths(explicit=explicit, env=env, file=path)

    assert config.packages == Path("/explicit/pkgs")  # explicit побеждает
    assert config.runs == Path("/env/runs")  # env побеждает file
    assert config.cache == Path("fc")  # file
    assert config.passports == Path("passports")  # дефолт


def test_explicit_none_falls_through(tmp_path: Path) -> None:
    explicit: dict[str, str | Path] = {"packages": None}  # type: ignore[dict-item]
    config = resolve_paths(explicit=explicit, env={ENV_PACKAGES_DIR: "/env"}, file=_absent(tmp_path))
    assert config.packages == Path("/env")


def test_unknown_explicit_keys_ignored(tmp_path: Path) -> None:
    config = resolve_paths(
        explicit={"unknown": "/x", "cache": "/c"}, env={}, file=_absent(tmp_path)
    )
    assert config.cache == Path("/c")


def test_relative_env_under_home(tmp_path: Path) -> None:
    config = resolve_paths(
        env={ENV_CACHE_DIR: "rel/cache"}, file=_absent(tmp_path), home=tmp_path / "x"
    )
    assert config.cache == tmp_path / "x" / "rel" / "cache"


def test_absolute_env_kept_under_home(tmp_path: Path) -> None:
    config = resolve_paths(
        env={ENV_CACHE_DIR: "/abs/cache"}, file=_absent(tmp_path), home=tmp_path / "x"
    )
    assert config.cache == Path("/abs/cache")


# ---------------------------------------------------------------------------
# Файл: разрешение и негативные случаи
# ---------------------------------------------------------------------------

_ENV_FIELDS = (
    ENV_PACKAGES_DIR,
    ENV_PASSPORTS_DIR,
    ENV_RUNS_DIR,
    ENV_INDEX_STORE_DIR,
    ENV_CORPUS_RAW_DIR,
    ENV_CACHE_DIR,
    ENV_REGISTRIES_DIR,
    ENV_REPORTS_DIR,
)


def test_file_from_env(tmp_path: Path) -> None:
    path = _write(tmp_path, {"packages": "envfile_pkgs"})
    config = resolve_paths(env={ENV_PATHS_FILE: str(path)})
    assert config.packages == Path("envfile_pkgs")


def test_file_argument_over_env(tmp_path: Path) -> None:
    arg = _write(tmp_path, {"packages": "from_arg"}, name="arg.yaml")
    other = _write(tmp_path, {"packages": "from_env"}, name="env.yaml")
    config = resolve_paths(file=arg, env={ENV_PATHS_FILE: str(other)})
    assert config.packages == Path("from_arg")


def test_unknown_key_in_file_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, {"packages": "p", "unexpected": "x"})
    with pytest.raises(ValueError, match="unexpected|extra"):
        resolve_paths(env={}, file=path)
    with pytest.raises(ValueError):
        resolve_paths(env={}, file=path)


def test_invalid_yaml_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("packages: [unclosed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML"):
        resolve_paths(env={}, file=path)


def test_non_mapping_file_rejected(tmp_path: Path) -> None:
    path = tmp_path / "list.yaml"
    path.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML-словарь"):
        resolve_paths(env={}, file=path)


def test_empty_yaml_file_is_defaults(tmp_path: Path) -> None:
    path = tmp_path / "empty.yaml"
    path.write_text("# только комментарий\n", encoding="utf-8")
    assert load_paths_file(path) == {}
    config = resolve_paths(env={}, file=path)
    assert config.packages == Path("packages")


def test_none_field_uses_default(tmp_path: Path) -> None:
    path = _write(tmp_path, {"packages": None})
    config = resolve_paths(env={}, file=path)
    assert config.packages == Path("packages")


def test_load_paths_file_keeps_home(tmp_path: Path) -> None:
    path = _write(tmp_path, {"home": "/h", "packages": "p"})
    document = load_paths_file(path)
    assert document["home"] == "/h"
    assert document["packages"] == "p"


# ---------------------------------------------------------------------------
# with_home
# ---------------------------------------------------------------------------


def test_with_home_normalizes_relative_and_keeps_absolute() -> None:
    config = PathsConfig(packages=Path("packages"), runs=Path("/abs/runs"))
    normalized = with_home(config, "/base")
    assert normalized.packages == Path("/base/packages")
    assert normalized.runs == Path("/abs/runs")


def test_with_home_returns_new_model() -> None:
    config = PathsConfig()
    normalized = with_home(config, "/base")
    assert normalized is not config
    assert config.packages == Path("packages")  # исходная не мутирована
    assert normalized.packages == Path("/base/packages")


# ---------------------------------------------------------------------------
# Модель
# ---------------------------------------------------------------------------


def test_model_forbids_extra() -> None:
    with pytest.raises(ValidationError):
        PathsConfig(unknown=Path("x"))  # type: ignore[call-arg]


def test_determinism(tmp_path: Path) -> None:
    path = _write(tmp_path, {"packages": "p"})
    first = resolve_paths(env={ENV_HOME: "/x"}, file=path)
    second = resolve_paths(env={ENV_HOME: "/x"}, file=path)
    assert first == second
    assert first.model_dump() == second.model_dump()


# ---------------------------------------------------------------------------
# Нейтральность (§1.3) и пример-файл
# ---------------------------------------------------------------------------


def test_example_file_parses_and_matches_fields() -> None:
    """Пример ``paths.example.yaml`` — валидный YAML; все 8 полей упомянуты."""
    text = EXAMPLE_PATH.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    assert data is None or isinstance(data, dict)
    for field in HARNESS_FIELDS:
        assert field in text, f"в примере не упомянуто поле {field!r}"
    assert "home" in text
