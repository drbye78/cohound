"""Тесты загрузчика конфига развёртывания (W13/T13.4).

Проверяют: безопасный дефолт при отсутствии файла, happy-path ``local``/``split``,
негативные случаи (профиль, ``base_url`` для split, auth, лишние поля), разбор
бюджета поиска, детерминизм и категорийную нейтральность модуля (§1.3).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from core.config.deploy import (
    DEFAULT_LLM_MODEL_ID,
    DEFAULT_LLM_SLOT,
    SPLIT_HARDENED_WARNING,
    CrawlBrowserConfig,
    CrawlConfig,
    CrawlShardConfig,
    DeployConfig,
    EmbedConfig,
    LLMDeployConfig,
    LLMSlotConfig,
    SearchDeployConfig,
    is_split,
    load_deploy_config,
    parse_serp_switch,
    profile_notices,
    resolve_host_shard,
    resolve_llm_slot,
    resolve_search_provider,
    resolve_serp_rounds,
    shard_notices,
)
from core.domain.enums import EmbedDevice
from core.domain.models import EmbedRunConfig
from core.pipeline.crawl_frontier import resolve_crawl_limits, resolve_embed_device

FIXTURES = Path(__file__).parent / "fixtures"
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


def _profile_config(directory: Path, profile: str) -> Path:
    """Пишет ``deploy.yaml`` с заданным профилем (и ``edge.base_url`` для split-ветки)."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "deploy.yaml"
    path.write_text(
        f"profile: {profile}\nedge:\n  base_url: https://edge.example:8443\n",
        encoding="utf-8",
    )
    return path


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


def test_split_hardened_warns_it_adds_nothing_over_split(tmp_path: Path) -> None:
    """D-8: значение принимается, но громко предупреждает, что хардненинга нет.

    Принятие без предупреждения было бы ложным утверждением о безопасности: Z2 PARSE,
    egress-прокси и TLS-инспекция не реализованы — поведение идентично ``split``.
    """
    hardened = load_deploy_config(_profile_config(tmp_path / "h", "split-hardened"))
    split = load_deploy_config(_profile_config(tmp_path / "s", "split"))
    assert profile_notices(hardened) == (SPLIT_HARDENED_WARNING,)
    assert profile_notices(split) == ()
    # Текст обязан называть и «ничего не добавляет», и отсутствующие механизмы.
    assert "ничего не добавляет" in SPLIT_HARDENED_WARNING
    for mechanism in ("Z2 PARSE", "egress-прокси", "TLS-инспекция"):
        assert mechanism in SPLIT_HARDENED_WARNING


def test_split_hardened_behaviour_is_identical_to_split(tmp_path: Path) -> None:
    """Алиас, а не другой профиль: различие только в имени (иначе предупреждение лгало бы)."""
    hardened = load_deploy_config(_profile_config(tmp_path / "h", "split-hardened"))
    split = load_deploy_config(_profile_config(tmp_path / "s", "split"))
    assert is_split(hardened) is is_split(split) is True
    assert hardened.edge == split.edge
    assert hardened.crawl == split.crawl
    assert hardened.extractor_zone == split.extractor_zone


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


def test_search_serp_rounds_parsed() -> None:
    """``search.serp.rounds`` — низший уровень переключения контура (бюджета здесь нет).

    Бюджет объявляется только входом прогона (``run.yaml: serp.budget``), потому что
    определяет число ушедших запросов, то есть состав корпуса и ``run_id``.
    """
    cfg = load_deploy_config(FIXTURES / "deploy.yaml")
    assert cfg.search.serp.rounds == 2
    assert cfg.search.adapter == "example_adapter"
    assert not hasattr(cfg.search, "budget")


def test_search_budget_key_rejected() -> None:
    """Удалённый ``search.budget`` отвергается (extra="forbid"), а не игнорируется молча."""
    with pytest.raises(ValidationError):
        SearchDeployConfig.model_validate({"budget": {"ru": 5, "en": 5}})


# ---------------------------------------------------------------------------
# SERP-контур: переключатель на четырёх уровнях (I3/§5)
# ---------------------------------------------------------------------------


def test_parse_serp_switch_forms() -> None:
    """Переключатель: «не задано» ≠ «выключено», off/0 выключают, мусор — отказ."""
    assert parse_serp_switch(None) is None
    assert parse_serp_switch("") is None
    assert parse_serp_switch("off") == 0
    assert parse_serp_switch(" OFF ") == 0
    assert parse_serp_switch("0") == 0
    assert parse_serp_switch(0) == 0
    assert parse_serp_switch("3") == 3
    assert parse_serp_switch(2) == 2
    for bad in ("-1", -1, True, "maybe", 1.5):
        with pytest.raises(ValueError):
            parse_serp_switch(bad)


def test_resolve_serp_rounds_priority_and_source() -> None:
    """Приоритет ``cli > env > run.yaml > deploy.yaml > default`` и уровень-источник.

    Источник обязателен: «почему поиск не работал» должно быть отличимо от «поиск не
    объявлен», а ``run_id`` обязан описывать тот контур, который реально исполнялся.
    """
    search = SearchDeployConfig(serp={"rounds": 1})
    assert resolve_serp_rounds(None) == (0, "default")
    assert resolve_serp_rounds(search) == (1, "deploy.yaml")
    assert resolve_serp_rounds(search, run_rounds=0) == (0, "run.yaml")
    assert resolve_serp_rounds(search, run_rounds=0, env="2") == (2, "env")
    assert resolve_serp_rounds(
        search, run_rounds=0, env="2", cli=4
    ) == (4, "cli")
    # env «off» выключает объявленный контур — и это видно как уровень env.
    assert resolve_serp_rounds(search, env="off") == (0, "env")


# ---------------------------------------------------------------------------
# Провайдер поиска: by_language + resolve_search_provider (I16/I21/I23, 03 D.5)
# ---------------------------------------------------------------------------


def test_search_by_language_parsed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "search": {
                    "adapter": "yandex",
                    "by_language": {"ru": "yandex", "en": "duckduckgo"},
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.search.adapter == "yandex"
    assert cfg.search.by_language == {"ru": "yandex", "en": "duckduckgo"}


def test_search_by_language_empty_key_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump({"profile": "local", "search": {"by_language": {"": "yandex"}}}),
    )
    with pytest.raises(ValidationError, match="by_language"):
        load_deploy_config(path)


def test_search_by_language_empty_value_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump({"profile": "local", "search": {"by_language": {"ru": "  "}}}),
    )
    with pytest.raises(ValidationError, match="by_language"):
        load_deploy_config(path)


def test_resolve_search_provider_priority() -> None:
    """explicit > by_language > adapter > default."""
    search = SearchDeployConfig(
        adapter="glm", by_language={"ru": "yandex", "en": "duckduckgo"}
    )
    # explicit побеждает всё
    assert resolve_search_provider(search, lang="ru", explicit="google") == "google"
    # by_language побеждает adapter
    assert resolve_search_provider(search, lang="ru") == "yandex"
    assert resolve_search_provider(search, lang="en") == "duckduckgo"
    # язык без совпадения → adapter
    assert resolve_search_provider(search, lang="de") == "glm"
    # explicit пустой/пробельный → не считается
    assert resolve_search_provider(search, lang="ru", explicit="  ") == "yandex"


def test_resolve_search_provider_casefold_language() -> None:
    search = SearchDeployConfig(by_language={"RU": "yandex", "En": "duckduckgo"})
    assert resolve_search_provider(search, lang="ru") == "yandex"
    assert resolve_search_provider(search, lang="EN") == "duckduckgo"


def test_resolve_search_provider_adapter_and_default() -> None:
    assert resolve_search_provider(SearchDeployConfig(adapter="glm"), lang="ru") == "glm"
    # нет конфига → default
    assert resolve_search_provider(None, lang="ru") == "yandex"
    assert resolve_search_provider(None, lang="ru", default="duckduckgo") == "duckduckgo"
    # конфиг без adapter/by_language → default
    assert resolve_search_provider(SearchDeployConfig(), lang="ru") == "yandex"


# ---------------------------------------------------------------------------
# Секция crawl (операционная политика HTTP-краулинга, 03 D.5/D.6)
# ---------------------------------------------------------------------------


def test_crawl_defaults_preserve_http_crawler_behaviour(tmp_path: Path) -> None:
    """Без секции ``crawl`` поля дефолтны: ``None``/``None``/``True`` (обратная совместимость)."""
    cfg = load_deploy_config(_write(tmp_path, "profile: local\n"))
    assert cfg.crawl.user_agent is None
    assert cfg.crawl.timeout_s is None
    assert cfg.crawl.follow_redirects is True


def test_crawl_section_parsed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "user_agent": "cohound-bot/1.0",
                    "timeout_s": 12.5,
                    "follow_redirects": False,
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.crawl.user_agent == "cohound-bot/1.0"
    assert cfg.crawl.timeout_s == 12.5
    assert cfg.crawl.follow_redirects is False


def test_crawl_non_positive_timeout_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: local\ncrawl:\n  timeout_s: 0\n")
    with pytest.raises(ValidationError, match="timeout_s"):
        load_deploy_config(path)


def test_crawl_robots_override_parsed(tmp_path: Path) -> None:
    """N1: ``crawl.robots`` — явный bool-override; дефолт ``None`` (auto-by-discovery)."""
    assert CrawlConfig().robots is None
    on_dir = tmp_path / "on"
    on_dir.mkdir()
    off_dir = tmp_path / "off"
    off_dir.mkdir()
    on = load_deploy_config(
        _write(on_dir, "profile: local\ncrawl:\n  robots: true\n")
    )
    assert on.crawl.robots is True
    off = load_deploy_config(
        _write(off_dir, "profile: local\ncrawl:\n  robots: false\n")
    )
    assert off.crawl.robots is False


# ---------------------------------------------------------------------------
# Секция crawl.browser (браузерный краулер; режимы cdp/persistent)
# ---------------------------------------------------------------------------


def test_crawl_browser_defaults() -> None:
    """Дефолтный ``browser`` — плейсхолдер ``mode=None`` без явной конфигурации."""
    browser = CrawlConfig().browser
    assert browser.mode is None
    assert browser.cdp_url is None
    assert browser.user_data_dir is None
    assert browser.headless is False
    assert browser.channel is None
    assert browser.timeout_ms == 30000


def test_crawl_browser_cdp_parsed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "cdp",
                        "cdp_url": "http://127.0.0.1:9222",
                        "headless": True,
                        "channel": "chrome",
                        "timeout_ms": 15000,
                    }
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.crawl.browser.mode == "cdp"
    assert cfg.crawl.browser.cdp_url == "http://127.0.0.1:9222"
    assert cfg.crawl.browser.headless is True
    assert cfg.crawl.browser.channel == "chrome"
    assert cfg.crawl.browser.timeout_ms == 15000


def test_crawl_browser_persistent_parsed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "persistent",
                        "user_data_dir": "/var/lib/cohound/profile",
                    }
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.crawl.browser.mode == "persistent"
    assert cfg.crawl.browser.user_data_dir == "/var/lib/cohound/profile"
    assert cfg.crawl.browser.cdp_url is None


def test_crawl_browser_empty_strings_normalized_to_none(tmp_path: Path) -> None:
    """Пустые/пробельные строки нормализуются: ``strip`` + ``""`` → ``None``."""
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "cdp",
                        "cdp_url": "  http://127.0.0.1:9222  ",
                        "channel": "   ",
                    }
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.crawl.browser.cdp_url == "http://127.0.0.1:9222"
    assert cfg.crawl.browser.channel is None


def test_crawl_browser_unknown_mode_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump({"profile": "local", "crawl": {"browser": {"mode": "attach"}}}),
    )
    with pytest.raises(ValidationError, match="mode"):
        load_deploy_config(path)


def test_crawl_browser_cdp_without_cdp_url_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {"browser": {"mode": "cdp", "headless": True}},
            }
        ),
    )
    with pytest.raises(ValidationError, match="cdp_url"):
        load_deploy_config(path)


def test_crawl_browser_persistent_without_user_data_dir_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump({"profile": "local", "crawl": {"browser": {"mode": "persistent"}}}),
    )
    with pytest.raises(ValidationError, match="user_data_dir"):
        load_deploy_config(path)


def test_crawl_browser_cdp_with_user_data_dir_rejected(tmp_path: Path) -> None:
    """Режим ``cdp`` несовместим с ``user_data_dir`` (взаимоисключение)."""
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "cdp",
                        "cdp_url": "http://127.0.0.1:9222",
                        "user_data_dir": "/tmp/profile",
                    }
                },
            }
        ),
    )
    with pytest.raises(ValidationError, match="взаимоисключение"):
        load_deploy_config(path)


def test_crawl_browser_persistent_with_cdp_url_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "persistent",
                        "user_data_dir": "/tmp/profile",
                        "cdp_url": "http://127.0.0.1:9222",
                    }
                },
            }
        ),
    )
    with pytest.raises(ValidationError, match="взаимоисключение"):
        load_deploy_config(path)


def test_crawl_browser_none_mode_allows_cross_fields(tmp_path: Path) -> None:
    """``mode=None`` — плейсхолдер: перекрёстных требований нет (валиден)."""
    cfg = load_deploy_config(
        _write(tmp_path, "profile: local\ncrawl:\n  browser:\n    headless: true\n")
    )
    assert cfg.crawl.browser.mode is None
    assert cfg.crawl.browser.headless is True


def test_crawl_browser_non_positive_timeout_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "cdp",
                        "cdp_url": "http://127.0.0.1:9222",
                        "timeout_ms": 0,
                    }
                },
            }
        ),
    )
    with pytest.raises(ValidationError, match="timeout_ms"):
        load_deploy_config(path)


def test_crawl_without_browser_section_still_loads(tmp_path: Path) -> None:
    """Минимальный/дефолтный YAML по-прежнему грузится; ``browser`` — плейсхолдер."""
    cfg = load_deploy_config(_write(tmp_path, "profile: local\ncrawl:\n  user_agent: ua\n"))
    assert cfg.crawl.user_agent == "ua"
    assert cfg.crawl.browser == CrawlBrowserConfig()
    assert cfg.crawl.browser.mode is None


# ---------------------------------------------------------------------------
# Идемпотентность под dump→validate (persist/reload, model_copy, JSON-схема)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "browser",
    [
        CrawlBrowserConfig(),  # плейсхолдер (mode=None)
        CrawlBrowserConfig(mode="cdp", cdp_url="http://127.0.0.1:9222"),
        CrawlBrowserConfig(mode="persistent", user_data_dir="/tmp/profile"),
    ],
    ids=["placeholder", "cdp", "persistent"],
)
def test_crawl_browser_dump_validate_idempotent(
    browser: CrawlBrowserConfig,
) -> None:
    """``model_validate(model_dump())`` не падает и сохраняет значения."""
    again = CrawlBrowserConfig.model_validate(browser.model_dump())
    assert again == browser


@pytest.mark.parametrize(
    "browser",
    [
        CrawlBrowserConfig(),
        CrawlBrowserConfig(mode="cdp", cdp_url="http://127.0.0.1:9222"),
        CrawlBrowserConfig(mode="persistent", user_data_dir="/tmp/profile"),
    ],
    ids=["placeholder", "cdp", "persistent"],
)
def test_deploy_config_dump_validate_idempotent(
    browser: CrawlBrowserConfig,
) -> None:
    """``DeployConfig.model_validate(cfg.model_dump())`` — без ошибок, значения сохранены.

    Регрессия: плейсхолдер ``CrawlBrowserConfig()`` раньше проваливал повторную
    валидацию («режим cdp требует непустой cdp_url»), т.к. ``model_dump()`` терял
    состояние «поле не задавалось» (``model_fields_set``). Теперь «не сконфигурировано» —
    явное ``mode=None``.
    """
    cfg = DeployConfig(profile="local", crawl=CrawlConfig(browser=browser))
    reloaded = DeployConfig.model_validate(cfg.model_dump())
    assert reloaded == cfg
    assert reloaded.crawl.browser == browser


def test_deploy_config_dump_validate_default_is_idempotent() -> None:
    """Дефолтный ``DeployConfig()`` (без явной секции) переваривает dump→validate."""
    cfg = DeployConfig()
    reloaded = DeployConfig.model_validate(cfg.model_dump())
    assert reloaded == cfg
    assert reloaded.crawl.browser.mode is None


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
    with pytest.raises(ValidationError, match=re.escape("audit.sink")):
        load_deploy_config(path)


def test_empty_document_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "# только комментарий\n")
    with pytest.raises(ValueError, match="пустой"):
        load_deploy_config(path)


def test_malformed_yaml_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: [unclosed\n")
    with pytest.raises(ValueError, match="YAML"):
        load_deploy_config(path)


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("profile: local\nunexpected: 1\n", id="top-level"),
        pytest.param(
            "profile: split\nedge:\n  base_url: https://edge.example:8443\n  unknown: 1\n",
            id="edge",
        ),
        pytest.param("profile: local\ncrawl:\n  user_agent: ua\n  bogus: true\n", id="crawl"),
        pytest.param(
            _dump({"profile": "local", "llm": {"cheap": {"endpoint": "x"}}}), id="llm-slot"
        ),
        pytest.param(
            _dump({"profile": "local", "embed": {"backend": "remote"}}), id="embed"
        ),
        pytest.param(
            _dump({"profile": "local", "embed": {"device": "auto"}}), id="embed-auto"
        ),
    ],
)
def test_extra_fields_rejected(tmp_path: Path, text: str) -> None:
    """Строгость схем ``extra="forbid"`` (AGENTS «Конвенции кода») на всех уровнях deploy.yaml."""
    with pytest.raises(ValidationError):
        load_deploy_config(_write(tmp_path, text))


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


def test_adapter_name_is_data_not_code(tmp_path: Path) -> None:
    """Произвольное имя адаптера принимается как данные (ядро провайдеро-нейтрально)."""
    path = _write(tmp_path, "profile: local\nsearch:\n  adapter: any_adapter\n")
    cfg: DeployConfig = load_deploy_config(path)
    assert cfg.search.adapter == "any_adapter"


# ---------------------------------------------------------------------------
# Секция llm: слоты моделей (commodity/execution-слой §7.4)
# ---------------------------------------------------------------------------


def test_default_llm_config() -> None:
    """Без секции ``llm`` активный слот — ``cheap``, обе слоты пусты (None)."""
    cfg = DeployConfig()
    assert cfg.llm.slot == DEFAULT_LLM_SLOT == "cheap"
    assert cfg.llm.cheap == LLMSlotConfig()
    assert cfg.llm.flagship == LLMSlotConfig()
    assert cfg.llm.cheap.base_url is None


def test_llm_section_parsed(tmp_path: Path) -> None:
    """Обе слоты парсятся: поля каждой сохранены как данные."""
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "llm": {
                    "slot": "flagship",
                    "cheap": {
                        "base_url": "http://cheap.test/v1",
                        "model_id": "m-cheap",
                        "decoder": "json_object",
                        "timeout_s": 30,
                        "max_tokens": 1024,
                    },
                    "flagship": {
                        "base_url": "http://flag.test/v1",
                        "model_id": "m-flag",
                        "model_version": "1.2",
                        "decoder": "json_schema",
                        "timeout_s": 240,
                        "max_tokens": 8192,
                    },
                },
            }
        ),
    )
    cfg = load_deploy_config(path)
    assert cfg.llm.slot == "flagship"
    assert cfg.llm.cheap.base_url == "http://cheap.test/v1"
    assert cfg.llm.cheap.model_id == "m-cheap"
    assert cfg.llm.cheap.timeout_s == 30
    assert cfg.llm.flagship.model_version == "1.2"
    assert cfg.llm.flagship.max_tokens == 8192


def test_canonical_llm_both_slots_same_default_model() -> None:
    """Оба слота канонического ``config/deploy.yaml`` по умолчанию — одна модель.

    Значение **провизионное** (идентификатор, используемый сегодня), а не измеренное:
    финальный выбор даёт ``hound experiment evaluator-benchmark``. Пин держит оба слота
    согласованными, чтобы прогон не зависел от того, какой слот активен.
    """
    cfg = load_deploy_config(None)
    assert cfg.llm.slot == DEFAULT_LLM_SLOT
    assert cfg.llm.cheap.model_id == DEFAULT_LLM_MODEL_ID
    assert cfg.llm.flagship.model_id == DEFAULT_LLM_MODEL_ID
    assert cfg.llm.cheap.model_id == "deepseek-v4.1-flash:purple"


def test_default_model_constants_track_deploy_config() -> None:
    """Пин против дрейфа идентификатора: конфиг — источник, константы обязаны совпадать.

    Идентификатор модели живёт в трёх местах кода (конфиг-дефолт, fallback адаптера
    оценщика, fallback LLM-разведки). Правка одного места без остальных давала ровно тот
    дефект, ради которого введена эта граница: манифест/докстринг называют одну модель,
    работает другая. Тест не «проверяет авторитетность спеки» — он вынуждает править все
    три значения вместе с `config/deploy.yaml`.
    """
    from core.adapters.llm_local import DEFAULT_MODEL_ID as EVALUATOR_ADAPTER_FALLBACK
    from core.cli.wiring import DEFAULT_LLM_MODEL as DISCOVERY_FALLBACK

    cfg = load_deploy_config(None)
    canonical = cfg.llm.cheap.model_id

    assert canonical == cfg.llm.flagship.model_id  # оба слота — одна модель
    assert canonical == DEFAULT_LLM_MODEL_ID  # конфиг-дефолт
    assert canonical == EVALUATOR_ADAPTER_FALLBACK  # fallback прямой конструкции
    assert canonical == DISCOVERY_FALLBACK  # последний фолбэк разведки


def test_llm_scenarios_empty_when_slot_inactive_so_model_id_is_not_in_run_id() -> None:
    """Пустой ``base_url`` → ``llm_scenarios`` пуста: неактивный слот не объявляет модель.

    Это условие активности, а не свойство канонического конфига: слот без endpoint'а
    оценщика не поднимает, поэтому выдумывать маршрут нельзя, и смена провизионного
    ``model_id`` такого слота на идентичность прогона не влияет. Проверяется вычислением:
    две разные модели дают одинаковый ``()``.
    """
    from types import SimpleNamespace

    from core.cli.execution_axes import _resolve_llm_scenarios

    args = SimpleNamespace(llm_slot=None, llm_model=None)
    inputs = SimpleNamespace(run=None)

    old = DeployConfig(
        llm=LLMDeployConfig(
            cheap=LLMSlotConfig(model_id="deepseek-v4.1-flash:cxbc"),
            flagship=LLMSlotConfig(model_id="deepseek-v4.1-flash:cxbc"),
        )
    )
    new = DeployConfig(
        llm=LLMDeployConfig(
            cheap=LLMSlotConfig(model_id="deepseek-v4.1-flash:purple"),
            flagship=LLMSlotConfig(model_id="deepseek-v4.1-flash:purple"),
        )
    )

    assert _resolve_llm_scenarios(args=args, inputs=inputs, deploy=old) == ()
    assert _resolve_llm_scenarios(args=args, inputs=inputs, deploy=new) == ()


def test_canonical_llm_slot_declares_evaluator_so_model_id_is_in_run_id() -> None:
    """Объявленный ``base_url`` → модель оценщика **входит** в идентичность прогона.

    Обратная сторона предыдущего теста и причина, по которой канонический конфиг обязан
    объявлять endpoint: ``llm_scenarios`` входит в ``execution_contract_digest`` (→
    ``run_id``). Пока слот ссылался на endpoint только из env, прогон вызывал модель,
    которой в ``run_id`` не было, — вердикты нельзя было воспроизвести по идентификатору
    прогона. Теперь объявленная смена модели меняет контракт, а не проходит молча.
    """
    from types import SimpleNamespace

    from core.cli.execution_axes import _resolve_llm_scenarios

    args = SimpleNamespace(llm_slot=None, llm_model=None)
    inputs = SimpleNamespace(run=None)

    canonical = load_deploy_config(None)
    assert canonical.llm.cheap.base_url, "канонический слот cheap обязан объявлять endpoint"
    scenarios = _resolve_llm_scenarios(args=args, inputs=inputs, deploy=canonical)
    assert len(scenarios) == 1
    assert scenarios[0]["scenario"] == "evaluator"
    assert scenarios[0]["model_id"] == canonical.llm.cheap.model_id

    swapped = DeployConfig(
        llm=LLMDeployConfig(
            cheap=LLMSlotConfig(
                base_url=canonical.llm.cheap.base_url,
                model_id="deepseek-v4.1-flash:cxbc",
                decoder=canonical.llm.cheap.decoder,
            )
        )
    )
    assert _resolve_llm_scenarios(args=args, inputs=inputs, deploy=swapped) != scenarios


def test_resolve_llm_slot_precedence() -> None:
    """explicit > llm.slot > default."""
    llm = LLMDeployConfig(slot="flagship")
    assert resolve_llm_slot(llm, explicit="cheap") == "cheap"
    assert resolve_llm_slot(llm) == "flagship"
    assert resolve_llm_slot(None) == DEFAULT_LLM_SLOT
    assert resolve_llm_slot(None, default="flagship") == "flagship"
    # пустой/пробельный explicit не считается (падаем на llm.slot)
    assert resolve_llm_slot(llm, explicit="  ") == "flagship"


def test_resolve_llm_slot_strips() -> None:
    assert resolve_llm_slot(None, explicit="  cheap  ") == "cheap"


def test_invalid_llm_slot_rejected() -> None:
    with pytest.raises(ValidationError, match=re.escape("llm.slot")):
        LLMDeployConfig(slot="turbo")
    with pytest.raises(ValueError, match="слот"):
        resolve_llm_slot(None, explicit="turbo")
    with pytest.raises(ValueError, match="слот"):
        resolve_llm_slot(None, default="turbo")


def test_llm_slot_non_positive_timeout_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, _dump({"profile": "local", "llm": {"cheap": {"timeout_s": 0}}}))
    with pytest.raises(ValidationError, match="timeout_s"):
        load_deploy_config(path)


# ---------------------------------------------------------------------------
# Секция embed: устройство эмбеддера (ось ``embed.device``, 16 §4.1)
# ---------------------------------------------------------------------------


def test_embed_defaults_to_cpu_without_section(tmp_path: Path) -> None:
    """Отсутствие секции ``embed`` = ``cpu``: объявленный дефолт, не выбор torch."""
    cfg = load_deploy_config(_write(tmp_path, "profile: local\n"))
    assert cfg.embed == EmbedConfig()
    assert cfg.embed.device is EmbedDevice.CPU


def test_embed_section_parsed(tmp_path: Path) -> None:
    cfg = load_deploy_config(_write(tmp_path, "profile: local\nembed:\n  device: cuda\n"))
    assert cfg.embed.device is EmbedDevice.CUDA


def test_embed_rejects_unknown_key_inside_section() -> None:
    """``extra="forbid"`` внутри ``embed:``: ключ без модели отвергается, а не игнорируется.

    Иначе ``embed: {backend: remote}`` создавал бы впечатление объявленного backend'а,
    которого никто не исполняет (``14`` §6 п. 5 — backend остаётся недекларированным).
    """
    with pytest.raises(ValidationError):
        EmbedConfig.model_validate({"backend": "remote"})


def test_embed_run_layer_default_is_none() -> None:
    """Дефолт run-слоя — ``None``: иначе он перекрыл бы app-слой в ``_pick``."""
    assert EmbedRunConfig().device is None


def test_embed_device_resolves_app_then_run() -> None:
    """Цепочка app < run и **никакого** per-site слоя (устройство — свойство хоста)."""
    assert resolve_embed_device(EmbedConfig(), None) is EmbedDevice.CPU
    assert (
        resolve_embed_device(EmbedConfig(device="cuda"), None) is EmbedDevice.CUDA
    )
    assert (
        resolve_embed_device(
            EmbedConfig(device="cpu"), EmbedRunConfig(device="cuda")
        )
        is EmbedDevice.CUDA
    )
    # run-слой ``None`` не отменяет app: «не задано» ≠ «cpu».
    assert (
        resolve_embed_device(
            EmbedConfig(device="cuda"), EmbedRunConfig(device=None)
        )
        is EmbedDevice.CUDA
    )
    # Контейнер тоже принимается: вызов с ``deploy`` не должен молча дать ``cpu``.
    assert (
        resolve_embed_device(
            DeployConfig(embed=EmbedConfig(device="cuda")), None
        )
        is EmbedDevice.CUDA
    )


def test_embed_device_has_no_site_layer() -> None:
    """Профиль домена не объявляет устройство эмбеддера (``extra="forbid"``)."""
    from core.config.domain_profiles import DomainProfile

    assert not hasattr(DomainProfile(), "embed_device")
    with pytest.raises(ValidationError):
        DomainProfile(embed_device="cuda")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# F7 (17 §2): реестр шардов узлов краула — ``crawl.shards``
# ---------------------------------------------------------------------------


def test_overlapping_host_shards_are_refused() -> None:
    """T9 (17 §7.1): пересечение host'ов между шардами — отказ, называющий host и узлы.

    Два узла на одном host'е ломают политику вежливости §10 **без единой ошибки в коде**:
    каждый получит свой бюджет, и частота к сайту удвоится. Поэтому пересечение — ошибка
    входа (у CLI — ``EXIT_USAGE``), а сообщение обязано назвать **и host, и узлы**: иначе
    оператор ищет пересечение руками.
    """
    with pytest.raises(ValidationError) as excinfo:
        CrawlConfig(
            shards=[
                CrawlShardConfig(node="node-a", hosts=["docs.test"]),
                CrawlShardConfig(node="node-b", hosts=["other.test", "docs.test"]),
            ]
        )
    message = str(excinfo.value)
    assert "docs.test" in message
    assert "node-a" in message and "node-b" in message
    assert "пересекаются" in message


def test_shard_disjointness_is_case_insensitive_by_limiter_rule() -> None:
    """``Example.COM`` и ``example.com`` — **один** ключ, значит пересечение (T9).

    Host'ы нормализуются тем же правилом, что ключ лимитера
    (:func:`core.util.rate_limit.normalize_host`): иначе «дизъюнктно по построению»
    было бы ложью, и два узла получили бы независимые бюджеты на один сайт.
    """
    with pytest.raises(ValidationError) as excinfo:
        CrawlConfig(
            shards=[
                CrawlShardConfig(node="node-a", hosts=["Example.COM"]),
                CrawlShardConfig(node="node-b", hosts=["example.com"]),
            ]
        )
    assert "example.com" in str(excinfo.value)


def test_shard_hosts_are_normalized_by_the_limiter_rule() -> None:
    """Шард хранит ключ лимитера: нижний регистр, без схемы/порта/хвостовой точки."""
    config = CrawlConfig(
        shards=[
            CrawlShardConfig(
                node=" node-a ",
                hosts=["Example.COM", "https://Docs.Test/path", "other.test.:8443"],
            )
        ]
    )
    assert config.shards is not None
    shard = config.shards[0]
    assert shard.node == "node-a"
    assert shard.hosts == ["example.com", "docs.test", "other.test"]


def test_duplicate_shard_node_is_refused() -> None:
    """Дубликат ``node`` — отказ: карта обязана описывать то, что исполняется."""
    with pytest.raises(ValidationError) as excinfo:
        CrawlConfig(
            shards=[
                CrawlShardConfig(node="node-a", hosts=["docs.test"]),
                CrawlShardConfig(node="node-a", hosts=["other.test"]),
            ]
        )
    assert "node-a" in str(excinfo.value)
    assert "дважды" in str(excinfo.value)


def test_empty_shard_is_a_declaration_not_an_error() -> None:
    """Пустой шард допустим как объявление «узел ничего не ведёт» — но он **видим**.

    Ошибки конфига здесь нет (узел может быть объявлен заранее), тишины — тоже:
    «host'ов ноль и работы ноль» оператор обязан увидеть строкой, а не выяснять из
    отсутствия артефактов.
    """
    config = CrawlConfig(shards=[CrawlShardConfig(node="node-idle", hosts=[])])
    assert shard_notices(config.shards) == (
        "shards: узел «node-idle» объявлен без host'ов — этот узел не будет краулить "
        "(видимый простой, а не тихий: 17 §2)",
    )
    shard = resolve_host_shard(config.shards, "node-idle")
    assert shard is not None and shard.empty is True


def test_resolve_host_shard_refuses_missing_and_unknown_node() -> None:
    """Карта есть, узел не выбран/не объявлен — ``ValueError`` (у CLI ``EXIT_USAGE``).

    «Взял все host'ы молча» дало бы два процесса на одних host'ах, а опечатка в имени
    узла не должна означать «все host'ы».
    """
    shards = [
        CrawlShardConfig(node="node-a", hosts=["docs.test"]),
        CrawlShardConfig(node="node-b", hosts=["other.test"]),
    ]
    with pytest.raises(ValueError, match="узел не выбран") as missing:
        resolve_host_shard(shards, None)
    assert "node-a" in str(missing.value) and "node-b" in str(missing.value)

    with pytest.raises(ValueError, match="не объявлен") as unknown:
        resolve_host_shard(shards, "node-typo")
    assert "node-typo" in str(unknown.value)

    # Без карты фильтра нет: ``None`` — «весь мир мой» (поведение прежнее).
    assert resolve_host_shard(None, None) is None
    assert resolve_host_shard(None, "node-a") is None


def test_shard_map_is_absent_from_digest_until_declared() -> None:
    """Карта шардов входит в ``CrawlLimits.as_dict`` **только при объявлении**.

    Иначе прогон без шардов сдвинул бы ``run_id`` у всех существующих пакетов — то есть
    объявил бы ``execution_change`` там, где измерение не менялось (A3/§13).
    """
    plain = resolve_crawl_limits("https://docs.test/", app=CrawlConfig())
    assert "host_shards" not in plain.as_dict()

    declared = resolve_crawl_limits(
        "https://docs.test/",
        app=CrawlConfig(
            shards=[
                CrawlShardConfig(node="node-b", hosts=["other.test", "docs.test"]),
                CrawlShardConfig(node="node-a", hosts=["third.test"]),
            ]
        ),
    )
    payload = declared.as_dict()
    assert payload["host_shards"] == [
        {"node": "node-a", "hosts": ["third.test"]},
        {"node": "node-b", "hosts": ["docs.test", "other.test"]},
    ]
