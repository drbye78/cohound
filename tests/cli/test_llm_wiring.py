"""Тесты проводки LLM-слотов в composition root Z0 (§7.4, ``03`` §E.3).

Проверяется, что выбор активного слота (``--llm-slot``/``COHOUND_LLM_SLOT``/``deploy.llm.slot``)
доходит до билдеров LLM:

* :func:`build_discovery_llm` — приоритет полей: CLI > env > слот > дефолт;
* :func:`build_evaluator_llm` — ``None`` без ``base_url`` слота и ``LocalEvaluator`` с ним;
* :func:`_resolve_ports` — CLI-выбор слота доходит до оценщика ``ports.evaluator``;
* парсер CLI принимает ``--llm-slot`` на ``run``/``passport-scout``/``package scout``.

Данные категорийно-нейтральны (§1.3): endpoint-хосты — вымышленные ``*.test``.
"""

from __future__ import annotations

import argparse

import pytest

from core.adapters.llm_discovery import DiscoveryLLM
from core.adapters.llm_local import LocalEvaluator
from core.cli.context import _resolve_ports
from core.cli.parser import build_parser
from core.cli.wiring import (
    ENV_LLM_API_KEY,
    ENV_LLM_BASE_URL,
    ENV_LLM_DECODER,
    ENV_LLM_MAX_TOKENS,
    ENV_LLM_MODEL,
    ENV_LLM_SLOT,
    ENV_LLM_TIMEOUT,
    ENV_SCOUT_CRAWLER,
    build_discovery_llm,
    build_evaluator_llm,
)
from core.config.deploy import DeployConfig, LLMDeployConfig, LLMSlotConfig
from tests.helpers.scout import clear_env

_ENV_NAMES = (
    ENV_LLM_SLOT,
    ENV_LLM_BASE_URL,
    ENV_LLM_API_KEY,
    ENV_LLM_MODEL,
    ENV_LLM_DECODER,
    ENV_LLM_TIMEOUT,
    ENV_LLM_MAX_TOKENS,
    ENV_SCOUT_CRAWLER,
)


def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_env(monkeypatch, _ENV_NAMES)


def _slots() -> LLMDeployConfig:
    """Слоты с разными endpoint-ами/моделями (данные-примеры)."""
    return LLMDeployConfig(
        slot="flagship",
        cheap=LLMSlotConfig(
            base_url="http://cheap.test/v1",
            model_id="m-cheap",
            decoder="json_object",
            timeout_s=30,
            max_tokens=1024,
        ),
        flagship=LLMSlotConfig(
            base_url="http://flag.test/v1",
            model_id="m-flag",
            decoder="json_schema",
            timeout_s=200,
            max_tokens=8192,
        ),
    )


# ---------------------------------------------------------------------------
# build_discovery_llm: приоритет слот/env/CLI
# ---------------------------------------------------------------------------


def test_build_discovery_llm_uses_active_slot_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без env активный слот из deploy-конфига задаёт все поля адаптера."""
    _clear(monkeypatch)
    llm = build_discovery_llm(deploy_llm=_slots())
    assert llm.base_url == "http://flag.test/v1"
    assert llm.model_id == "m-flag"
    assert llm.decoder == "json_schema"
    assert llm.timeout == 200
    assert llm.max_tokens == 8192


def test_build_discovery_llm_env_slot_overrides_deploy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``COHOUND_LLM_SLOT`` переопределяет ``deploy.llm.slot``."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_SLOT, "cheap")
    llm = build_discovery_llm(deploy_llm=_slots())
    assert llm.model_id == "m-cheap"
    assert llm.base_url == "http://cheap.test/v1"


def test_build_discovery_llm_cli_slot_overrides_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Явный ``slot`` (CLI ``--llm-slot``) побеждает env и deploy."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_SLOT, "cheap")
    llm = build_discovery_llm(deploy_llm=_slots(), slot="flagship")
    assert llm.model_id == "m-flag"


def test_build_discovery_llm_env_fields_override_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env ``COHOUND_LLM_MODEL`` побеждает поле слота (CLI > env > слот)."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_MODEL, "env-model")
    llm = build_discovery_llm(deploy_llm=_slots())
    assert llm.model_id == "env-model"


def test_build_discovery_llm_without_deploy_backward_compatible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без ``deploy_llm`` env > дефолт модуля; base_url пуст (нет внешнего дефолта)."""
    _clear(monkeypatch)
    llm = build_discovery_llm()
    assert llm.base_url == ""
    assert llm.model_id == "deepseek-v4.1-flash:cxbc"


# ---------------------------------------------------------------------------
# build_evaluator_llm: None без base_url / LocalEvaluator с ним
# ---------------------------------------------------------------------------


def test_build_evaluator_llm_none_without_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пустые слоты (дефолт) → оценщик не поднимается (``None``)."""
    _clear(monkeypatch)
    assert build_evaluator_llm(deploy_llm=LLMDeployConfig()) is None
    assert build_evaluator_llm() is None


def test_build_evaluator_llm_from_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Непустой ``base_url`` слота → сконфигурированный ``LocalEvaluator``."""
    _clear(monkeypatch)
    ev = build_evaluator_llm(deploy_llm=_slots())
    assert isinstance(ev, LocalEvaluator)
    assert ev.base_url == "http://flag.test/v1"
    assert ev.model_id == "m-flag"
    assert ev.decoder == "json_schema"
    assert ev.timeout == 200
    assert ev.endpoint_kind == "api"


def test_build_evaluator_llm_slot_selected_by_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``COHOUND_LLM_SLOT`` выбирает слот и для оценщика."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_SLOT, "cheap")
    ev = build_evaluator_llm(deploy_llm=_slots())
    assert isinstance(ev, LocalEvaluator)
    assert ev.base_url == "http://cheap.test/v1"
    assert ev.model_id == "m-cheap"


def test_build_evaluator_llm_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ключ берётся только из ``COHOUND_LLM_API_KEY`` (внешних фолбэков нет)."""
    _clear(monkeypatch)
    ev = build_evaluator_llm(deploy_llm=_slots())
    assert ev is not None and ev.api_key is None
    monkeypatch.setenv(ENV_LLM_API_KEY, "primary")
    ev2 = build_evaluator_llm(deploy_llm=_slots())
    assert ev2 is not None and ev2.api_key == "primary"


def test_build_evaluator_llm_run_slot_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run_slot`` (run.yaml) ниже env/CLI, но выше ``deploy.llm.slot`` (§7.4)."""
    _clear(monkeypatch)
    # deploy.slot=flagship; run.yaml просит cheap → cheap побеждает deploy.
    ev = build_evaluator_llm(deploy_llm=_slots(), run_slot="cheap")
    assert isinstance(ev, LocalEvaluator)
    assert ev.model_id == "m-cheap"
    # env всё ещё выше run.yaml.
    monkeypatch.setenv(ENV_LLM_SLOT, "flagship")
    ev2 = build_evaluator_llm(deploy_llm=_slots(), run_slot="cheap")
    assert ev2 is not None and ev2.model_id == "m-flag"
    # CLI (slot=) выше env и run.yaml.
    ev3 = build_evaluator_llm(deploy_llm=_slots(), slot="cheap", run_slot="cheap")
    assert ev3 is not None and ev3.model_id == "m-cheap"


# ---------------------------------------------------------------------------
# _resolve_ports: выбор слота CLI доходит до оценщика
# ---------------------------------------------------------------------------


def _ports_args(llm_slot: str | None = "flagship") -> argparse.Namespace:
    """Аргументы ``run``, минимально нужные :func:`_resolve_ports`."""
    return argparse.Namespace(
        profile="local",
        ports=None,
        gateway_factory=None,
        deploy_transport=None,
        paths=None,
        llm_slot=llm_slot,
    )


def test_resolve_ports_builds_evaluator_from_cli_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--llm-slot flagship`` на ``run`` даёт оценщик с endpoint-ом слота ``flagship``."""
    _clear(monkeypatch)
    deploy = DeployConfig(profile="local", llm=_slots())

    ports = _resolve_ports(_ports_args("flagship"), deploy)

    assert isinstance(ports.evaluator, LocalEvaluator)
    assert ports.evaluator.base_url == "http://flag.test/v1"


def test_resolve_ports_without_slot_base_url_keeps_evaluator_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Дефолтный deploy (пустые слоты) → ``ports.evaluator`` остаётся ``None``."""
    _clear(monkeypatch)
    deploy = DeployConfig(profile="local")

    ports = _resolve_ports(_ports_args(), deploy)

    assert ports.evaluator is None


# ---------------------------------------------------------------------------
# Парсер CLI: флаг --llm-slot на discovery/run-командах
# ---------------------------------------------------------------------------


def test_cli_parses_llm_slot_for_run() -> None:
    args = build_parser().parse_args(
        ["run", "--run-yaml", "r.yaml", "--package", "pkg", "--llm-slot", "flagship"]
    )
    assert args.llm_slot == "flagship"


def test_cli_parses_llm_slot_for_passport_scout() -> None:
    args = build_parser().parse_args(
        ["passport-scout", "--product-name", "X", "--url", "https://x.test", "--llm-slot", "cheap"]
    )
    assert args.llm_slot == "cheap"


def test_cli_parses_llm_slot_for_package_scout() -> None:
    args = build_parser().parse_args(
        ["package", "scout", "--package", "pkg", "--subject-name", "s", "--llm-slot", "flagship"]
    )
    assert args.llm_slot == "flagship"


def test_cli_rejects_unknown_llm_slot() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["run", "--run-yaml", "r.yaml", "--package", "pkg", "--llm-slot", "turbo"]
        )


# ---------------------------------------------------------------------------
# DiscoveryLLM-шейп не сломан
# ---------------------------------------------------------------------------


def test_discovery_llm_is_discovery_port(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear(monkeypatch)
    assert isinstance(build_discovery_llm(deploy_llm=_slots()), DiscoveryLLM)
