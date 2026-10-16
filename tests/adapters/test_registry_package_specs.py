"""Тесты package-scoped реестровых спецификаций и секретов ``headers_env`` (P3, 03 §D.4).

Проверяют новый дом спецификаций (``packages/{case_id}/registries/{registry_id}.yaml``):
нейтральный загрузчик ``load_package_registry_configs`` с path-конфайнментом по
``registry_id``, и механизм ``headers_env``, при котором коммиттируемая спецификация не
несёт токен — значение читается из окружения в момент запроса.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters.registry_generic import (
    GenericRegistryFetcher,
    RegistryConfig,
    RegistryConfigError,
    load_package_registry_configs,
    package_registry_dir,
    registry_spec_path,
    resolve_registry_headers,
)

_SPEC = (
    "registry_id: reg_a\n"
    "jurisdiction: XX\n"
    "endpoint: 'https://example.invalid/api/search/{query}'\n"
    "method: GET\n"
    "format: json\n"
    "records_path: items\n"
    "headers:\n"
    "  Accept: application/json\n"
    "headers_env:\n"
    "  Authorization: COHOUND_TEST_REGISTRY_TOKEN\n"
    "timeout: 30.0\n"
    "mapping:\n"
    "  name: name\n"
)


def _package(tmp_path: Path, spec: str = _SPEC, name: str = "reg_a.yaml") -> Path:
    pkg = tmp_path / "case_a"
    (pkg / "registries").mkdir(parents=True)
    (pkg / "registries" / name).write_text(spec, encoding="utf-8")
    return pkg


# ---------------------------------------------------------------------------
# Пути/загрузка
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["../evil", "a/b", "", "Reg-A", "reg.a"])
def test_spec_path_rejects_unsafe_registry_ids(tmp_path: Path, bad: str) -> None:
    with pytest.raises(RegistryConfigError):
        registry_spec_path(tmp_path / "case_a", bad)


def test_load_all_and_selected(tmp_path: Path) -> None:
    pkg = _package(tmp_path)
    (pkg / "registries" / "reg_b.yaml").write_text(
        _SPEC.replace("reg_a", "reg_b"), encoding="utf-8"
    )
    all_configs = load_package_registry_configs(pkg)
    assert set(all_configs) == {"reg_a", "reg_b"}
    only = load_package_registry_configs(pkg, ["reg_b"])
    assert set(only) == {"reg_b"}


def test_missing_dir_or_declared_spec(tmp_path: Path) -> None:
    pkg = tmp_path / "case_empty"
    pkg.mkdir()
    # Нет каталога и нет объявленных id → пусто (реестры опциональны).
    assert load_package_registry_configs(pkg) == {}
    # Объявленный, но отсутствующий реестр → понятная ошибка.
    with pytest.raises(RegistryConfigError, match="не найдена"):
        load_package_registry_configs(pkg, ["reg_missing"])


# ---------------------------------------------------------------------------
# Секреты: значение — из окружения, не из файла
# ---------------------------------------------------------------------------


def test_headers_env_resolves_from_environment(tmp_path: Path) -> None:
    cfg = load_package_registry_configs(_package(tmp_path))["reg_a"]
    resolved = resolve_registry_headers(cfg, env={"COHOUND_TEST_REGISTRY_TOKEN": "SEKRET"})
    assert resolved["Authorization"] == "SEKRET"
    assert resolved["Accept"] == "application/json"


def test_headers_env_unset_omits_header(tmp_path: Path) -> None:
    cfg = load_package_registry_configs(_package(tmp_path))["reg_a"]
    resolved = resolve_registry_headers(cfg, env={})
    assert "Authorization" not in resolved  # не выдумываем значение
    assert resolved["Accept"] == "application/json"


def test_headers_env_blank_omits_header(tmp_path: Path) -> None:
    cfg = load_package_registry_configs(_package(tmp_path))["reg_a"]
    resolved = resolve_registry_headers(cfg, env={"COHOUND_TEST_REGISTRY_TOKEN": "   "})
    assert "Authorization" not in resolved


def test_headers_env_overrides_static_header() -> None:
    cfg = RegistryConfig(
        registry_id="r",
        jurisdiction="XX",
        endpoint="https://example.invalid/{query}",
        format="json",
        mapping={"name": "name"},
        headers={"Authorization": "static"},
        headers_env={"Authorization": "TOK"},
    )
    assert resolve_registry_headers(cfg, env={"TOK": "dynamic"})["Authorization"] == "dynamic"


def test_committed_spec_carries_no_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Секрет не хранится в файле; fetcher берёт его из окружения в момент запроса."""
    monkeypatch.setenv("COHOUND_TEST_REGISTRY_TOKEN", "SEKRET-TOKEN")
    pkg = _package(tmp_path)
    spec_text = (pkg / "registries" / "reg_a.yaml").read_text(encoding="utf-8")
    assert "SEKRET-TOKEN" not in spec_text

    cfg = load_package_registry_configs(pkg)["reg_a"]
    captured: dict[str, dict[str, str]] = {}

    def _http_get(url: str, headers: dict[str, str], timeout: float | None = None) -> bytes:
        captured.update(headers)
        return b'{"items": [{"name": "x"}]}'

    from core.ports.types import ProductRef

    fetcher = GenericRegistryFetcher(cfg, _http_get, clock=lambda: "T")
    fetcher.fetch(ProductRef(product_id="p", canonical_name="P", vendor="V", jurisdiction="XX"), {})
    assert captured["Authorization"] == "SEKRET-TOKEN"


if __name__ == "__main__":  # pragma: no cover - локальный запуск
    raise SystemExit(pytest.main([__file__, "-q"]))
