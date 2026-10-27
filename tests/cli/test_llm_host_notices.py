"""Машиночитаемые замечания о хостах endpoint'а оценщика (§10, V6).

Проверяется: замечания строятся из **объявленных** входов (флаг + хосты), env-расхождение
переиспользует текст ``llm_env_notices`` (второй формулировки нет), порядок детерминирован,
и объявление стенда читается из ``deploy.yaml: llm.rnd_endpoint`` тремя состояниями.
"""

from __future__ import annotations

import pytest

from core.cli.wiring import ENV_LLM_BASE_URL, llm_env_notices, llm_host_notices
from core.config.deploy import DeployConfig, LLMDeployConfig, LLMSlotConfig


def _clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        ENV_LLM_BASE_URL,
        "COHOUND_LLM_API_KEY",
        "COHOUND_LLM_MODEL",
        "COHOUND_LLM_DECODER",
        "COHOUND_LLM_SLOT",
    ):
        monkeypatch.delenv(name, raising=False)


def test_declared_stand_is_a_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """``rnd_endpoint: true`` — видимое предупреждение «карта не производственная»."""
    _clear(monkeypatch)
    deploy = LLMDeployConfig(rnd_endpoint=True)

    notices = llm_host_notices(deploy_llm=deploy, hosts=["llm.stand.test"])

    assert [notice["code"] for notice in notices] == ["rnd_endpoint_declared"]
    assert notices[0]["level"] == "warning"
    assert notices[0]["hosts"] == ["llm.stand.test"]


def test_undeclared_host_names_the_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Хост объявлен, принадлежность периметру — нет: замечание, а не молчание."""
    _clear(monkeypatch)

    notices = llm_host_notices(
        deploy_llm=LLMDeployConfig(),  # rnd_endpoint: null
        hosts=["llm.stand.test"],
    )

    assert [notice["code"] for notice in notices] == ["rnd_endpoint_not_declared"]
    assert "llm.stand.test" in notices[0]["message"]


def test_declared_but_no_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Исключение объявлено, а endpoint'а нет — признак лишний, и это сказано."""
    _clear(monkeypatch)

    notices = llm_host_notices(deploy_llm=LLMDeployConfig(rnd_endpoint=True), hosts=[])

    assert [notice["code"] for notice in notices] == ["rnd_endpoint_without_endpoint"]
    assert notices[0]["level"] == "notice"


def test_env_endpoint_text_is_reused_not_duplicated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Endpoint из env: текст берётся у ``llm_env_notices`` — одна формулировка на факт."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_BASE_URL, "https://env.test/v1")
    deploy = LLMDeployConfig(cheap=LLMSlotConfig(model_id="m"))

    notices = llm_host_notices(deploy_llm=deploy, hosts=[])

    codes = [notice["code"] for notice in notices]
    assert codes == ["endpoint_from_env"]
    # Тот же текст, что печатает существующее предупреждение, — не второй формулировки.
    assert notices[0]["message"] in llm_env_notices(deploy_llm=deploy)


def test_order_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Порядок замечаний и хостов не зависит от порядка входа."""
    _clear(monkeypatch)
    deploy = LLMDeployConfig(rnd_endpoint=True)

    first = llm_host_notices(deploy_llm=deploy, hosts=["llm.b.test", "llm.a.test"])
    second = llm_host_notices(deploy_llm=deploy, hosts=["llm.a.test", "llm.b.test"])

    assert [notice["code"] for notice in first] == [notice["code"] for notice in second]
    assert first[0]["hosts"] == ["llm.a.test", "llm.b.test"]


def test_deploy_config_accepts_three_states() -> None:
    """Три состояния декларации различимы в конфиге (а не «флаг есть/нет»)."""
    assert DeployConfig(profile="local").llm.rnd_endpoint is None
    assert LLMDeployConfig(rnd_endpoint=True).rnd_endpoint is True
    assert LLMDeployConfig(rnd_endpoint=False).rnd_endpoint is False
