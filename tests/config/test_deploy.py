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
    DEFAULT_LLM_MODEL_ID,
    DEFAULT_LLM_SLOT,
    LLM_SLOTS,
    CrawlBrowserConfig,
    CrawlConfig,
    DeployConfig,
    LLMDeployConfig,
    LLMSlotConfig,
    SearchDeployConfig,
    is_split,
    load_deploy_config,
    resolve_llm_slot,
    resolve_search_provider,
)

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


def test_search_by_language_defaults_empty() -> None:
    cfg = load_deploy_config(FIXTURES / "deploy.yaml")
    assert cfg.search.by_language == {}


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


def test_resolve_search_provider_local_scope_independent_of_product() -> None:
    """I16: маршрут — функция от lang, не от продукта (одинаков для всех продуктов)."""
    search = SearchDeployConfig(by_language={"ru": "yandex", "en": "duckduckgo"})
    # Один и тот же (search, lang) → один провайдер независимо от вызова.
    assert resolve_search_provider(search, lang="ru") == resolve_search_provider(
        search, lang="ru"
    )


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


def test_extra_crawl_field_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "profile: local\ncrawl:\n  user_agent: ua\n  robots: true\n")
    with pytest.raises(ValidationError):
        load_deploy_config(path)


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


def test_crawl_browser_default_equals_fresh_instance() -> None:
    """Плейсхолдер конструируется без обязательного ``cdp_url`` (не сконфигурирован)."""
    assert CrawlBrowserConfig() == CrawlConfig().browser
    assert CrawlBrowserConfig().mode is None


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


def test_crawl_browser_extra_field_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        _dump(
            {
                "profile": "local",
                "crawl": {
                    "browser": {
                        "mode": "cdp",
                        "cdp_url": "http://127.0.0.1:9222",
                        "stealth": True,
                    }
                },
            }
        ),
    )
    with pytest.raises(ValidationError):
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
    """Оба слота канонического ``config/deploy.yaml`` по умолчанию — одна модель."""
    cfg = load_deploy_config(None)
    assert cfg.llm.slot == DEFAULT_LLM_SLOT
    assert cfg.llm.cheap.model_id == DEFAULT_LLM_MODEL_ID
    assert cfg.llm.flagship.model_id == DEFAULT_LLM_MODEL_ID


def test_llm_slots_constant() -> None:
    assert LLM_SLOTS == ("cheap", "flagship")


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
    with pytest.raises(ValidationError, match="llm.slot"):
        LLMDeployConfig(slot="turbo")
    with pytest.raises(ValueError, match="слот"):
        resolve_llm_slot(None, explicit="turbo")
    with pytest.raises(ValueError, match="слот"):
        resolve_llm_slot(None, default="turbo")


def test_extra_llm_slot_field_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, _dump({"profile": "local", "llm": {"cheap": {"endpoint": "x"}}}))
    with pytest.raises(ValidationError):
        load_deploy_config(path)


def test_llm_slot_non_positive_timeout_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, _dump({"profile": "local", "llm": {"cheap": {"timeout_s": 0}}}))
    with pytest.raises(ValidationError, match="timeout_s"):
        load_deploy_config(path)
