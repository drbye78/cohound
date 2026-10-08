"""Тесты загрузчика конфига развёртывания (W13/T13.4).

Проверяют: безопасный дефолт при отсутствии файла, happy-path ``local``/``split``,
негативные случаи (профиль, ``base_url`` для split, auth, лишние поля), разбор
бюджета поиска, детерминизм и категорийную нейтральность модуля (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from core.config.deploy import (
    DeployConfig,
    is_split,
    load_deploy_config,
)

FIXTURES = Path(__file__).parent / "fixtures"
MODULE_PATH = Path(__file__).resolve().parents[2] / "core" / "config" / "deploy.py"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, text: str) -> Path:
    """Пишет временный ``deploy.yaml`` и возвращает путь."""
    path = tmp_path / "deploy.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def _dump(data: dict[str, Any]) -> str:
    """Сериализует словарь в YAML (UTF-8, без экранирования кириллицы)."""
    return yaml.safe_dump(data, allow_unicode=True)


# ---------------------------------------------------------------------------
# Happy-path и дефолт при отсутствии файла
# ---------------------------------------------------------------------------


def test_missing_file_returns_local_default(tmp_path: Path) -> None:
    """Отсутствующий файл → безопасный дефолт ``profile: local``, без исключения."""
    cfg = load_deploy_config(tmp_path / "does_not_exist.yaml")
    assert cfg.profile == "local"
    assert is_split(cfg) is False
    assert cfg.edge.base_url is None


def test_default_canonical_path_when_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``path=None`` ищет канонический ``config/deploy.yaml``; без файла — local-дефолт."""
    monkeypatch.chdir(tmp_path)
    cfg = load_deploy_config()
    assert cfg.profile == "local"
    assert cfg.extractor_zone == "core"


def test_load_local_profile(tmp_path: Path) -> None:
    cfg = load_deploy_config(_write(tmp_path, "profile: local\nextractor_zone: core\n"))
    assert cfg.profile == "local"
    assert not is_split(cfg)


def test_load_split_fixture_happy() -> None:
    cfg = load_deploy_config(FIXTURES / "deploy.yaml")
    assert is_split(cfg) is True
    assert cfg.edge.base_url == "https://edge.internal:8443"
    assert cfg.edge.auth is not None and cfg.edge.auth.kind == "mtls"
    assert cfg.edge.tls.ca == "certs/ca.pem"
    assert cfg.edge.tls.client_cert == "certs/core.pem"
    assert cfg.edge.tls.client_key == "secrets/core.key"
    assert cfg.edge.timeout_s == 30
    assert cfg.edge.max_retries == 2
    assert cfg.edge.max_response_bytes == 52_428_800
    assert cfg.edge.audit.sink == "file"
    assert cfg.edge.audit.target == "/var/log/cohound/audit.jsonl"
    assert cfg.extractor_zone == "edge"


def test_split_hardened_counts_as_split(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "profile: split-hardened\nedge:\n  base_url: https://edge.example:8443\n",
    )
    cfg = load_deploy_config(path)
    assert cfg.profile == "split-hardened"
    assert is_split(cfg) is True


def test_oidc_auth_mapping_parsed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "split",
                "edge": {
                    "base_url": "https://edge.example:8443",
                    "auth": {"kind": "oidc"},
                    "oidc": {"issuer": "https://idp.example/realms/x", "audience": "cohound-edge"},
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.edge.auth is not None and cfg.edge.auth.kind == "oidc"
    assert cfg.edge.oidc is not None and cfg.edge.oidc.audience == "cohound-edge"


def test_search_budget_parsed() -> None:
    cfg = load_deploy_config(FIXTURES / "deploy.yaml")
    assert cfg.search.budget == {"ru": 5, "en": 5}
    assert cfg.search.adapter == "example_adapter"


# ---------------------------------------------------------------------------
# Негативные случаи
# ---------------------------------------------------------------------------


def test_split_without_base_url_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: split\nedge:\n  auth: mtls\n")
    with pytest.raises(ValidationError, match="base_url"):
        load_deploy_config(path)


def test_bad_profile_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: cluster\n")
    with pytest.raises(ValidationError, match="profile"):
        load_deploy_config(path)


def test_bad_auth_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "profile: split\nedge:\n  base_url: https://edge.example:8443\n  auth: password\n",
    )
    with pytest.raises(ValidationError, match="auth"):
        load_deploy_config(path)


def test_bad_extractor_zone_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: local\nextractor_zone: dmz\n")
    with pytest.raises(ValidationError, match="extractor_zone"):
        load_deploy_config(path)


def test_bad_audit_sink_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "profile: split\nedge:\n  base_url: https://edge.example:8443\n  audit: { sink: kafka }\n",
    )
    with pytest.raises(ValidationError, match="audit.sink"):
        load_deploy_config(path)


def test_extra_top_level_field_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: local\nunexpected: 1\n")
    with pytest.raises(ValidationError):
        load_deploy_config(path)


def test_extra_edge_field_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "profile: split\nedge:\n  base_url: https://edge.example:8443\n  unknown: 1\n",
    )
    with pytest.raises(ValidationError):
        load_deploy_config(path)


def test_empty_document_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "# только комментарий\n")
    with pytest.raises(ValueError, match="пустой"):
        load_deploy_config(path)


def test_malformed_yaml_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: [unclosed\n")
    with pytest.raises(ValueError, match="YAML"):
        load_deploy_config(path)


# ---------------------------------------------------------------------------
# Детерминизм
# ---------------------------------------------------------------------------


def test_loading_is_deterministic() -> None:
    first = load_deploy_config(FIXTURES / "deploy.yaml")
    second = load_deploy_config(FIXTURES / "deploy.yaml")
    assert first == second
    assert first.model_dump() == second.model_dump()


# ---------------------------------------------------------------------------
# Нейтральность (§1.3)
# ---------------------------------------------------------------------------


def test_module_is_category_neutral() -> None:
    """Исходник модуля не содержит предметных/вендорских терминов."""
    source = MODULE_PATH.read_text(encoding="utf-8").lower()
    forbidden = ("postgres", "pangolin", "sber", "сбер", "copywala", "платформа")
    for token in forbidden:
        assert token not in source, f"нейтральность §1.3 нарушена: «{token}»"


def test_adapter_name_is_data_not_code(tmp_path: Path) -> None:
    """Произвольное имя адаптера принимается как данные (ядро провайдеро-нейтрально)."""
    path = _write(tmp_path, "profile: local\nsearch:\n  adapter: any_adapter\n")
    cfg: DeployConfig = load_deploy_config(path)
    assert cfg.search.adapter == "any_adapter"
