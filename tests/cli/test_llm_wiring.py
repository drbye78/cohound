"""Тесты проводки LLM-слотов в composition root Z0 (§7.4, ``03`` §E.3).

Проверяется, что выбор активного слота (``--llm-slot``/``COHOUND_LLM_SLOT``/``deploy.llm.slot``)
доходит до билдеров LLM:

* :func:`build_discovery_llm` — приоритет полей: CLI > env > слот > дефолт;
* :func:`build_evaluator_llm` — ``None`` без ``base_url`` слота и ``LocalEvaluator`` с ним;
* идентичность модели оценщика — **объявленный** вход (флаг/слот), env игнорируется
  (регресс на измеренную ложь: контракт называл одну модель, работала другая);
* :func:`_resolve_ports` — CLI-выбор слота доходит до оценщика ``ports.evaluator``;
* парсер CLI принимает ``--llm-slot`` на ``run``/``passport-scout``/``package scout`` и
  ``--llm-model`` на ``run``/``plan``/``crawl``.

Данные категорийно-нейтральны (§1.3): endpoint-хосты — вымышленные ``*.test``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from core.adapters.llm_discovery import DiscoveryLLM
from core.adapters.llm_local import LocalEvaluator
from core.cli.context import _resolve_ports
from core.cli.errors import EXIT_USAGE, CliError
from core.cli.parser import build_parser
from core.cli.wiring import (
    DEFAULT_LLM_MODEL,
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
    llm_env_notices,
    resolve_evaluator_model_identity,
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
    """Без ``deploy_llm`` env > дефолт модуля; base_url пуст (нет внешнего дефолта).

    Дефолт сверяется с константой, а не с литералом: идентификатор модели — данные
    ``config/deploy.yaml``, и его пин живёт в ``tests/config/test_deploy.py``.
    """
    _clear(monkeypatch)
    llm = build_discovery_llm()
    assert llm.base_url == ""
    assert llm.model_id == DEFAULT_LLM_MODEL


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


def _ports_args(
    llm_slot: str | None = "flagship",
    *,
    llm_model: str | None = None,
    runs_dir: str | None = None,
) -> argparse.Namespace:
    """Аргументы ``run``, минимально нужные :func:`_resolve_ports`."""
    return argparse.Namespace(
        profile="local",
        ports=None,
        gateway_factory=None,
        deploy_transport=None,
        paths=None,
        llm_slot=llm_slot,
        llm_model=llm_model,
        runs_dir=runs_dir,
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


# ---------------------------------------------------------------------------
# Идентичность модели оценщика — объявленный вход, не env (§7.4/I21)
# ---------------------------------------------------------------------------

def test_evaluator_model_and_decoder_ignore_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Регресс на измеренную ложь: env не подменяет объявленную модель/декодер.

    Измерение: слот объявлял ``m-flag``/``json_schema``, env — ``from-env``/``none``;
    контракт (``_resolve_llm_scenarios``) называл слот, а рантайм работал на env-модели.
    """
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_MODEL, "from-env")
    monkeypatch.setenv(ENV_LLM_DECODER, "none")

    ev = build_evaluator_llm(deploy_llm=_slots())

    assert ev is not None
    assert ev.model_id == "m-flag"  # объявлено слотом, не env
    assert ev.decoder == "json_schema"
    assert ev.base_url == "http://flag.test/v1"


def test_evaluator_operational_fields_still_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint/ключ/таймаут — операционные данные хоста: env по-прежнему действует."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_BASE_URL, "http://env.test/v1")
    monkeypatch.setenv(ENV_LLM_API_KEY, "primary")
    monkeypatch.setenv(ENV_LLM_TIMEOUT, "7")

    ev = build_evaluator_llm(deploy_llm=_slots())

    assert ev is not None
    assert ev.base_url == "http://env.test/v1"
    assert ev.api_key == "primary"
    assert ev.timeout == 7
    # Модель при этом остаётся объявленной слотом.
    assert ev.model_id == "m-flag"


def test_evaluator_undeclared_model_with_active_endpoint_is_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Активный endpoint без объявленной модели — ошибка, а не тихий дефолт."""
    _clear(monkeypatch)
    # Слот активен (base_url есть), model_id не объявлен; env модель «подсказывает» — но
    # env оценщиком не читается, значит модель не объявлена вовсе.
    monkeypatch.setenv(ENV_LLM_MODEL, "from-env")
    deploy = LLMDeployConfig(
        slot="cheap", cheap=LLMSlotConfig(base_url="http://cheap.test/v1")
    )

    with pytest.raises(CliError) as excinfo:
        build_evaluator_llm(deploy_llm=deploy)

    message = str(excinfo.value)
    assert excinfo.value.code == "llm"
    assert excinfo.value.exit_code == EXIT_USAGE
    assert "model_id" in message
    assert "--llm-model" in message
    assert "deploy.llm.cheap" in message
    # Env-значение не выдаётся за объявленное.
    assert "from-env" not in message


def test_evaluator_flag_model_wins_over_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Явный ``--llm-model`` побеждает поле слота (аргумент > слот > дефолт)."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_MODEL, "from-env")

    ev = build_evaluator_llm(deploy_llm=_slots(), model_id="from-flag")

    assert ev is not None
    assert ev.model_id == "from-flag"
    assert ev.decoder == "json_schema"  # декодер — из слота


def test_evaluator_inactive_slot_stays_none_even_with_undeclared_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Неактивный слот (пустой base_url) → ``None``; ошибки «модель не объявлена» нет."""
    _clear(monkeypatch)
    deploy = LLMDeployConfig(cheap=LLMSlotConfig())  # ни base_url, ни model_id
    assert build_evaluator_llm(deploy_llm=deploy) is None


def test_resolve_evaluator_model_identity_returns_declared_pair() -> None:
    """Чистый резолвер: (model_id, decoder) из объявленных входов."""
    slot = LLMSlotConfig(model_id="slot-model", decoder="json_object")

    assert resolve_evaluator_model_identity(slot_config=slot) == (
        "slot-model",
        "json_object",
    )
    assert resolve_evaluator_model_identity(
        slot_config=slot, model_id="flag-model", decoder="json_schema"
    ) == ("flag-model", "json_schema")
    # Пустой decoder слота → дефолт модуля оценщика.
    assert resolve_evaluator_model_identity(
        slot_config=LLMSlotConfig(model_id="m")
    )[1] == "none"


def test_resolve_ports_passes_llm_model_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--llm-model`` доходит до ``ports.evaluator`` (wiring-плечо флага)."""
    _clear(monkeypatch)
    deploy = DeployConfig(profile="local", llm=_slots())

    ports = _resolve_ports(_ports_args("flagship", llm_model="from-flag"), deploy)

    assert isinstance(ports.evaluator, LocalEvaluator)
    assert ports.evaluator.model_id == "from-flag"


# ---------------------------------------------------------------------------
# Видимость env-расхождений: не игнорируем env молча
# ---------------------------------------------------------------------------

def test_llm_env_notices_report_ignored_model_and_decoder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Заданные env model/decoder дают ровно по одному сообщению (не по одному на вызов)."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_MODEL, "from-env")
    monkeypatch.setenv(ENV_LLM_DECODER, "none")

    first = llm_env_notices(deploy_llm=_slots())
    second = llm_env_notices(deploy_llm=_slots())

    assert len(first) == 2
    assert first == second  # чистая функция: сообщения не накапливаются
    assert any("COHOUND_LLM_MODEL=from-env" in note for note in first)
    assert any("COHOUND_LLM_DECODER=none" in note for note in first)
    assert all("deploy.llm." in note for note in first)
    # Endpoint объявлен слотом → сообщения «вне контракта» нет.
    assert not any("COHOUND_LLM_BASE_URL" in note for note in first)


def test_llm_env_notices_env_endpoint_is_outside_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint из env при неактивном слоте → явное «вне execution contract»."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_BASE_URL, "http://env.test/v1")
    deploy = LLMDeployConfig(cheap=LLMSlotConfig(model_id="m-cheap"))

    notices = llm_env_notices(deploy_llm=deploy)

    assert len(notices) == 1
    note = notices[0]
    assert "COHOUND_LLM_BASE_URL" in note
    assert "llm_scenarios" in note
    assert "execution contract" in note


def test_llm_env_notices_quiet_when_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Без env — ни одного сообщения (предупреждение не фабрикуется)."""
    _clear(monkeypatch)
    assert llm_env_notices(deploy_llm=_slots()) == ()


def test_llm_env_notices_quiet_when_env_endpoint_equals_declared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env-значение того же endpoint'а — не «расхождение», а тот же адрес.

    Сравнение идёт по канонической базе (``normalize_api_base``): хвост ``/v1`` в env —
    обычная запись того же адреса. Предупреждение о подмене endpoint'а, которое горит
    всегда, обесценивает себя: оператор перестаёт отличать настоящую подмену.
    """
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_BASE_URL, "https://llm.test/v1")
    deploy = LLMDeployConfig(
        cheap=LLMSlotConfig(base_url="https://llm.test", model_id="m-cheap")
    )

    assert llm_env_notices(deploy_llm=deploy) == ()

    # А другой хост — по-прежнему подмена, и она названа.
    monkeypatch.setenv(ENV_LLM_BASE_URL, "https://other.test/v1")
    notices = llm_env_notices(deploy_llm=deploy)
    assert len(notices) == 1
    assert "переопределяет слот" in notices[0]


def test_resolve_ports_writes_env_notice_to_wiring_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Расхождение env видно в ``logs/wiring.jsonl`` (событие ``llm_env_note``), один раз."""
    _clear(monkeypatch)
    monkeypatch.setenv(ENV_LLM_MODEL, "from-env")
    deploy = DeployConfig(profile="local", llm=_slots())

    _resolve_ports(_ports_args("flagship", runs_dir=str(tmp_path)), deploy, run_id="run-x")

    log = tmp_path / "run-x" / "logs" / "wiring.jsonl"
    events = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    notes = [event for event in events if event.get("ctx", {}).get("event") == "llm_env_note"]
    assert len(notes) == 1
    assert notes[0]["level"] == "WARN"
    assert "COHOUND_LLM_MODEL=from-env" in notes[0]["message"]


# ---------------------------------------------------------------------------
# Парсер: --llm-model на входах, считающих identity
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "argv",
    [
        ["run", "--run-yaml", "r.yaml", "--package", "pkg"],
        ["plan", "--run-yaml", "r.yaml", "--package", "pkg"],
        ["crawl", "run", "--run-yaml", "r.yaml", "--package", "pkg"],
        ["crawl", "resume", "--run-yaml", "r.yaml", "--package", "pkg", "--run-id", "run-x"],
        ["crawl", "restart", "--run-yaml", "r.yaml", "--package", "pkg", "--run-id", "run-x"],
    ],
)
def test_cli_parses_llm_model_flag(argv: list[str]) -> None:
    args = build_parser().parse_args([*argv, "--llm-model", "declared-model"])
    assert args.llm_model == "declared-model"
