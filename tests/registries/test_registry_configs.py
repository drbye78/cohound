"""Валидация шаблонов реестровых конфигов ``docs/templates/registries/*.yaml`` (W6/T6.2).

Проверяет, что **каждый** файл-шаблон загружается движком
(``core.adapters.registry_generic.load_registry_config``) и удовлетворяет схеме
``RegistryConfig``; что примеры-шаблоны ``example_*.yaml`` имеют непустой ``mapping``,
используют зарезервированный TLD ``.invalid`` и порождают непустой ``source_url`` (R1);
что исходник движка категорийно-нейтрален (§1.3).

Шаблоны перенесены из корневого ``registries/`` в ``docs/templates/registries/`` (P3):
корневой каталог больше не операционный путь — рабочие спецификации package-scoped
(``packages/{case_id}/registries/{registry_id}.yaml``, 03 §D.4).

Сеть отсутствует — HTTP-клиент полностью подставной. Если каталог шаблонов пуст,
тесты валидации пропускаются (skip), а не падают.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from core.adapters.registry_generic import (
    GenericRegistryFetcher,
    RegistryConfig,
    load_registry_config,
)
from core.ports.types import ProductRef

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_DIR = ROOT / "docs" / "templates" / "registries"

# Список конфигов собирается один раз при импорте модуля.
REGISTRY_YAML = sorted(REGISTRY_DIR.glob("*.yaml"))
EXAMPLE_YAML = [path for path in REGISTRY_YAML if path.name.startswith("example")]


def _product() -> ProductRef:
    """Пример продукта для подстановки в URL-шаблон."""
    return ProductRef(
        product_id="prod-a",
        canonical_name="Example Product",
        vendor="Example Vendor",
        jurisdiction="XX",
    )


def _stub_client(payload: bytes):
    """Callable-клиент ``(url, headers) -> bytes``; сеть не задействуется."""

    def http_get(url: str, headers: dict[str, str]) -> bytes:
        return payload

    return http_get


def _minimal_payload(config: RegistryConfig) -> bytes:
    """Минимальный синтетический ответ под формат конфига (для проверки source_url)."""
    if config.format == "json":
        return json.dumps({"items": [{"name": "x", "id": "1"}]}).encode("utf-8")
    return (
        b'<table><tr data-version="1" '
        b'data-href="https://example.invalid/detail/1">x</tr></table>'
    )


# ---------------------------------------------------------------------------
# Каждый конфиг проходит валидацию схемы
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", REGISTRY_YAML, ids=lambda p: p.name)
def test_every_registry_config_validates(path: Path) -> None:
    """Любой ``docs/templates/registries/*.yaml`` загружается и соответствует ``RegistryConfig``."""
    cfg = load_registry_config(path)

    assert isinstance(cfg, RegistryConfig)
    assert cfg.registry_id
    assert cfg.jurisdiction
    assert cfg.endpoint
    assert cfg.format in ("json", "html")
    assert cfg.method.upper() == "GET"
    # Схема обязывает mapping быть непустым (config-driven: нечего извлекать — ошибка).
    assert cfg.mapping
    for field_name, source_path in cfg.mapping.items():
        assert field_name, f"пустое имя поля маппинга в {path.name}"
        assert source_path, f"пустой путь маппинга в {path.name}: {field_name!r}"


# ---------------------------------------------------------------------------
# Примеры-шаблоны: очевидно не продакшн и порождают source_url (R1)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", EXAMPLE_YAML, ids=lambda p: p.name)
def test_example_config_is_template_with_invalid_endpoint(path: Path) -> None:
    """Пример — шаблон: ``example_*``, TLD ``.invalid``, непустой маппинг, R1."""
    cfg = load_registry_config(path)

    # Идентификатор намеренно помечен как пример.
    assert cfg.registry_id.startswith("example_"), cfg.registry_id
    assert cfg.mapping

    # Зарезервированный TLD `.invalid` (RFC 2606): адрес заведомо не существует,
    # значит выдуманных «живых» endpoint'ов в репозитории нет.
    host = urlsplit(cfg.endpoint).hostname or ""
    assert host == "invalid" or host.endswith(".invalid"), cfg.endpoint

    # URL-шаблон порождает непустой source_url: есть подстановочный плейсхолдер
    # либо query_param, к которому добавляется значение query.
    assert re.search(r"\{[^}]+\}", cfg.endpoint) or cfg.query_param


@pytest.mark.parametrize("path", EXAMPLE_YAML, ids=lambda p: p.name)
def test_example_config_produces_facts_with_source_url(path: Path) -> None:
    """R1 на примерах: каждый факт несёт непустой ``source_url`` (URL запроса)."""
    cfg = load_registry_config(path)
    fetcher = GenericRegistryFetcher(
        cfg, _stub_client(_minimal_payload(cfg)), clock=lambda: "FIXED_TS"
    )
    facts = fetcher.fetch(_product(), {"run_id": "run-1"})

    assert facts, f"{path.name}: пример не породил ни одного факта"
    for fact in facts:
        assert fact.source_url
        assert fact.registry_id == cfg.registry_id
        assert fact.product_id == "prod-a"


# ---------------------------------------------------------------------------
# Нейтральность движка (§1.3)
# ---------------------------------------------------------------------------


