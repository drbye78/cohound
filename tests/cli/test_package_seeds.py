"""Тесты ``hound package seeds …`` (09 §2.3/§7): авторский seed-набор пакета.

Покрывают три уровня, которые обязаны сойтись:

* чистые операции :mod:`core.config.seeds` (``add_seed``/``set_seed_enabled``/
  ``render_corpus_urls``) — включая идемпотентность и пересчёт ``ProductSeeds.state``;
* сценарный слой :mod:`core.pipeline.package_seeds` — ``dry_run``, запись только при
  изменении байтов, аудит;
* CLI ``hound package seeds …`` — коды выхода, ``--actor``, ``seeds list/export`` и
  проводку до :func:`core.pipeline.onboarding.load_effective_seeds`.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, main
from core.config.seeds import (
    CORPUS_URLS_RELPATH,
    SEEDS_RELPATH,
    add_seed,
    load_corpus_urls,
    load_seeds,
    render_corpus_urls,
    render_seeds,
    set_seed_enabled,
)
from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import SeedsConfig
from core.pipeline.digests import seeds_digest
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.package_admin import PackageAdminError
from core.pipeline.package_seeds import (
    seeds_add,
    seeds_disable,
    seeds_enable,
    seeds_export,
)

_AT = _dt.date(2026, 1, 2)
_PRODUCT = "product_anchor"


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    """Запускает CLI и возвращает ``(код, stdout, stderr)``."""
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _new(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> Path:
    """Создаёт пакет из шаблона в ``tmp_path`` (доменные профили шаблона на месте)."""
    code, out, _ = _run(
        capsys,
        [
            "package", "new", "--case-id", "seeds_pkg", "--archetype", "B",
            "--dir", str(tmp_path), "--json",
        ],
    )
    assert code == EXIT_OK, out
    return tmp_path / "seeds_pkg"


def _add(
    package: Path,
    url: str,
    *,
    product: str = _PRODUCT,
    actor: str = "roger",
    dry_run: bool = False,
):
    """Добавляет seed через сценарный слой (фиксированная дата — байты детерминированы)."""
    return seeds_add(
        package, product, url, actor=actor, at=_AT, dry_run=dry_run
    )


def _seed_file(package: Path) -> Path:
    return package / SEEDS_RELPATH


def _write_legacy_corpus_urls(package: Path, mapping: dict[str, list[str]]) -> Path:
    """Кладёт legacy ``sources/corpus_urls.yaml`` (пакет «старого» формата)."""
    lines: list[str] = []
    for product_id, urls in mapping.items():
        lines.append(f"{product_id}:")
        lines.extend(f"  - {url}" for url in urls)
    path = package / CORPUS_URLS_RELPATH
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# A. Чистые операции над SeedsConfig
# ---------------------------------------------------------------------------


def _config_with(urls: list[str], product: str = _PRODUCT) -> SeedsConfig:
    """Конфиг с seed'ами продукта (порядок = порядок списка)."""
    config = SeedsConfig(products={})
    for url in urls:
        config = add_seed(
            config,
            product,
            url,
            role=SeedRole.PRIMARY,
            source=SeedSource.OPERATOR,
            actor="roger",
            at=_AT,
        )
    return config


def test_add_seed_appends_to_end_and_changes_digest() -> None:
    """Новый URL дописывается в конец: позиция = ``order`` в ``seeds_digest``."""
    first = _config_with(["https://a.test/"])
    second = add_seed(
        first, _PRODUCT, "https://b.test/", role=SeedRole.DOCS, actor="roger", at=_AT
    )
    seeds = second.products[_PRODUCT].seeds
    assert [seed.normalized_url for seed in seeds] == [
        "https://a.test/",
        "https://b.test/",
    ]
    assert seeds[1].role is SeedRole.DOCS
    assert seeds_digest(second) != seeds_digest(first)


def test_add_seed_normalizes_and_binds_profile() -> None:
    """Нормализация §7.2 + автопривязка к самому длинному ключу профиля."""
    config = add_seed(
        SeedsConfig(products={}),
        _PRODUCT,
        "https://Docs.Example.COM:443/path/#frag",
        actor="roger",
        at=_AT,
        profiles={"example.com": {}, "docs.example.com": {}},
    )
    seed = config.products[_PRODUCT].seeds[0]
    assert seed.normalized_url == "https://docs.example.com/path/"
    assert seed.url == "https://Docs.Example.COM:443/path/#frag"
    assert seed.profile_key == "docs.example.com"


def test_add_existing_enabled_seed_is_byte_identical_noop() -> None:
    """Повторное добавление активного seed'а: no-op, байты и дайджест те же."""
    config = _config_with(["https://a.test/"])
    again = add_seed(
        config, _PRODUCT, "https://A.TEST/", role=SeedRole.SATELLITE, actor="other", at=_AT
    )
    assert render_seeds(again) == render_seeds(config)
    assert seeds_digest(again) == seeds_digest(config)
    # Провенанс не перетёрт: побеждает существующая запись.
    assert again.products[_PRODUCT].seeds[0].added_by == "roger"
    assert again.products[_PRODUCT].seeds[0].role is SeedRole.PRIMARY


def test_add_existing_disabled_seed_reenables_in_place() -> None:
    """Выключенный seed включается на месте: порядок и провенанс сохраняются."""
    config = _config_with(["https://a.test/", "https://b.test/"])
    disabled = set_seed_enabled(
        config, _PRODUCT, "https://a.test/", enabled=False, actor="roger", at=_AT
    )
    assert disabled.products[_PRODUCT].seeds[0].state is SeedState.DISABLED
    restored = add_seed(disabled, _PRODUCT, "https://a.test/", actor="roger", at=_AT)
    seeds = restored.products[_PRODUCT].seeds
    assert [seed.normalized_url for seed in seeds] == [
        "https://a.test/",
        "https://b.test/",
    ]
    assert seeds[0].is_enabled
    assert seeds[0].added_by == "roger"
    # Включение через add и через enable сходятся в одно состояние.
    assert seeds[0].last_verified_at == _AT


def test_disable_last_enabled_seed_recomputes_product_state() -> None:
    """``ready`` без активного seed'а — противоречие O1: состояние понижается."""
    config = _config_with(["https://a.test/"])
    assert config.products[_PRODUCT].state is OnboardingState.READY
    disabled = set_seed_enabled(
        config, _PRODUCT, "https://a.test/", enabled=False, actor="roger", at=_AT
    )
    product = disabled.products[_PRODUCT]
    assert product.state is OnboardingState.PENDING_SEEDS
    assert product.seeds[0].state is SeedState.DISABLED
    assert not product.enabled_for()
    # Модель валидна (валидатор ``ready`` требует активный seed) — перечитывание проходит.
    SeedsConfig.model_validate(disabled.model_dump(mode="json"))


def test_disable_keeps_ready_when_another_seed_enabled() -> None:
    """Выключение не последнего seed'а оставляет продукт ``ready``."""
    config = _config_with(["https://a.test/", "https://b.test/"])
    disabled = set_seed_enabled(
        config, _PRODUCT, "https://a.test/", enabled=False, actor="roger", at=_AT
    )
    assert disabled.products[_PRODUCT].state is OnboardingState.READY
    assert [seed.normalized_url for seed in disabled.products[_PRODUCT].enabled_for()] == [
        "https://b.test/"
    ]


def test_disable_unknown_url_raises() -> None:
    """Неизвестный URL — ``ValueError`` с понятным сообщением (не тихий no-op)."""
    config = _config_with(["https://a.test/"])
    with pytest.raises(ValueError, match="не найден"):
        set_seed_enabled(
            config, _PRODUCT, "https://b.test/", enabled=False, actor="roger", at=_AT
        )
    with pytest.raises(ValueError, match="product_missing"):
        set_seed_enabled(
            config, "product_missing", "https://a.test/", enabled=False, actor="roger"
        )


def test_set_enabled_is_idempotent() -> None:
    """Повторное включение/выключение того же состояния — байт-идентичный no-op."""
    config = _config_with(["https://a.test/"])
    disabled = set_seed_enabled(
        config, _PRODUCT, "https://a.test/", enabled=False, actor="roger", at=_AT
    )
    disabled2 = set_seed_enabled(
        disabled, _PRODUCT, "https://a.test/", enabled=False, actor="roger", at=_AT
    )
    assert render_seeds(disabled2) == render_seeds(disabled)
    enabled = set_seed_enabled(
        disabled, _PRODUCT, "https://a.test/", enabled=True, actor="roger", at=_AT
    )
    enabled2 = set_seed_enabled(
        enabled, _PRODUCT, "https://a.test/", enabled=True, actor="roger", at=_AT
    )
    assert render_seeds(enabled2) == render_seeds(enabled)
    assert enabled.products[_PRODUCT].seeds[0].last_verified_at == _AT


def test_mutations_require_actor() -> None:
    """``actor`` обязателен и на чистом слое (провенанс не выдумывается)."""
    with pytest.raises(ValueError, match="actor"):
        add_seed(SeedsConfig(products={}), _PRODUCT, "https://a.test/", actor=" ")
    with pytest.raises(ValueError, match="actor"):
        set_seed_enabled(
            _config_with(["https://a.test/"]),
            _PRODUCT,
            "https://a.test/",
            enabled=False,
            actor="",
        )


# ---------------------------------------------------------------------------
# A. render_corpus_urls — точная обратная сторона load_corpus_urls
# ---------------------------------------------------------------------------


def test_render_corpus_urls_round_trip(tmp_path: Path) -> None:
    """Экспорт читается обратно тем же загрузчиком (главный тест экспорта)."""
    config = _config_with(
        ["https://b.test/docs/", "https://a.test/", "https://c.test/x"]
    )
    config = add_seed(
        config, "product_other", "https://other.test/", actor="roger", at=_AT
    )
    text = render_corpus_urls(config)
    path = tmp_path / "corpus_urls.yaml"
    path.write_text(text, encoding="utf-8")
    assert load_corpus_urls(path) == {
        # Порядок внутри продукта — авторский (он же ``order`` в ``seeds_digest``).
        _PRODUCT: ["https://b.test/docs/", "https://a.test/", "https://c.test/x"],
        "product_other": ["https://other.test/"],
    }
    # Повторный рендер байт-идентичен (идемпотентность).
    assert render_corpus_urls(config) == text


def test_render_corpus_urls_exports_only_enabled(tmp_path: Path) -> None:
    """Выключенные seed'ы не экспортируются; продукт без активных — не эмитится."""
    config = _config_with(["https://a.test/", "https://b.test/"])
    config = add_seed(config, "product_other", "https://other.test/", actor="roger", at=_AT)
    config = set_seed_enabled(
        config, _PRODUCT, "https://a.test/", enabled=False, actor="roger", at=_AT
    )
    config = set_seed_enabled(
        config, "product_other", "https://other.test/", enabled=False, actor="roger", at=_AT
    )
    path = tmp_path / "corpus_urls.yaml"
    path.write_text(render_corpus_urls(config), encoding="utf-8")
    assert load_corpus_urls(path) == {_PRODUCT: ["https://b.test/"]}
    assert render_corpus_urls(SeedsConfig(products={})) == "{}\n"
    empty = tmp_path / "empty.yaml"
    empty.write_text(render_corpus_urls(SeedsConfig(products={})), encoding="utf-8")
    assert load_corpus_urls(empty) == {}


# ---------------------------------------------------------------------------
# B. Сценарный слой: запись только при изменении, dry_run не пишет
# ---------------------------------------------------------------------------


def test_scenario_add_writes_only_on_change(tmp_path: Path) -> None:
    """Первый add пишет файл, повторный — нет (байты те же, ``written`` пуст)."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    first = _add(package, "https://a.test/")
    assert first.changed is True
    assert first.written == [_seed_file(package)]
    before = _seed_file(package).read_bytes()

    again = _add(package, "https://a.test/")
    assert again.changed is False
    assert again.written == []
    assert _seed_file(package).read_bytes() == before
    assert again.seeds_digest == again.seeds_digest_before


def test_scenario_dry_run_writes_nothing(tmp_path: Path) -> None:
    """``dry_run`` не пишет ни ``seeds.yaml``, ни журнал авторских правок."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    result = _add(package, "https://a.test/", dry_run=True)
    assert result.dry_run is True
    assert result.changed is True  # «было бы изменено»
    assert result.written == []
    assert not _seed_file(package).exists()
    assert not (package / ".authoring" / "apply_log.jsonl").exists()


def test_scenario_audit_log_records_mutation(tmp_path: Path) -> None:
    """Мутация пишет строку в ``.authoring/apply_log.jsonl`` (аудит авторских правок)."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _add(package, "https://a.test/")
    log = package / ".authoring" / "apply_log.jsonl"
    assert log.is_file()
    assert '"op": "seeds_add"' in log.read_text(encoding="utf-8")


def test_scenario_requires_actor(tmp_path: Path) -> None:
    """Без ``--actor`` мутация отказывает до записи (``PackageAdminError``)."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    with pytest.raises(PackageAdminError, match="actor"):
        seeds_add(package, _PRODUCT, "https://a.test/", actor=None)
    assert not _seed_file(package).exists()


def test_scenario_disable_then_enable(tmp_path: Path) -> None:
    """disable всех seed'ов → ``pending_seeds``, enable → ``ready`` (порядок тот же)."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _add(package, "https://a.test/")
    _add(package, "https://b.test/")
    partial = seeds_disable(package, _PRODUCT, "https://a.test/", actor="roger", at=_AT)
    # Один активный seed остался — продукт по-прежнему готов к измерению.
    assert partial.product_state == OnboardingState.READY.value
    disabled = seeds_disable(package, _PRODUCT, "https://b.test/", actor="roger", at=_AT)
    assert disabled.product_state == OnboardingState.PENDING_SEEDS.value
    assert [seed["enabled"] for seed in disabled.product_seeds] == [False, False]
    enabled = seeds_enable(package, _PRODUCT, "https://a.test/", actor="roger", at=_AT)
    assert enabled.product_state == OnboardingState.READY.value
    assert [seed["normalized_url"] for seed in enabled.product_seeds] == [
        "https://a.test/",
        "https://b.test/",
    ]


def test_scenario_export_round_trip_and_out_only_on_change(tmp_path: Path) -> None:
    """Экспорт в файл: пишется при изменении, повторный — нет; формат читается обратно."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _add(package, "https://a.test/")
    out = tmp_path / "corpus_urls.yaml"
    first = seeds_export(package, out=out)
    assert first.written is True
    assert first.products == 1 and first.urls == 1
    assert load_corpus_urls(out) == {_PRODUCT: ["https://a.test/"]}
    second = seeds_export(package, out=out)
    assert second.written is False
    with pytest.raises(PackageAdminError, match="формат"):
        seeds_export(package, fmt="json")


# ---------------------------------------------------------------------------
# C. CLI
# ---------------------------------------------------------------------------


def test_cli_seeds_add_list_export(capsys, tmp_path: Path) -> None:
    """add → list → export в stdout (без ``--out``)."""
    package = _new(capsys, tmp_path)
    code, out, _ = _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://docs.template.example/a",
            "--role", "docs", "--actor", "roger", "--json",
        ],
    )
    assert code == EXIT_OK, out
    payload = json.loads(out)
    assert payload["changed"] is True
    assert payload["product_state"] == OnboardingState.READY.value
    assert payload["url"] == "https://docs.template.example/a"
    # Автопривязка к ключу профиля шаблона.
    assert payload["seeds"][0]["profile_key"] == "template.example"

    code, out, _ = _run(
        capsys,
        ["package", "seeds", "list", "--package", str(package), "--json"],
    )
    assert code == EXIT_OK
    listed = json.loads(out)
    assert listed["products"][0]["product_id"] == _PRODUCT
    assert listed["products"][0]["enabled"] == 1

    code, out, _ = _run(
        capsys,
        ["package", "seeds", "export", "--package", str(package)],
    )
    assert code == EXIT_OK
    assert out == f"{_PRODUCT}:\n- https://docs.template.example/a\n"


def test_cli_seeds_list_filters_by_product(capsys, tmp_path: Path) -> None:
    """``--product`` фильтрует; неизвестный продукт → ``not found``."""
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://a.test/", "--actor", "roger",
        ],
    )
    code, out, _ = _run(
        capsys,
        ["package", "seeds", "list", "--package", str(package), "--product", _PRODUCT, "--json"],
    )
    assert code == EXIT_OK
    assert len(json.loads(out)["products"]) == 1
    code, out, _ = _run(
        capsys,
        ["package", "seeds", "list", "--package", str(package), "--product", "nope", "--json"],
    )
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


def test_cli_seeds_mutation_without_actor_is_usage_error(capsys, tmp_path: Path) -> None:
    """Мутация без ``--actor`` — ``EXIT_USAGE``, файл не создан."""
    package = _new(capsys, tmp_path)
    for argv in (
        ["package", "seeds", "add", "--package", str(package),
         "--product", _PRODUCT, "--url", "https://a.test/"],
        ["package", "seeds", "disable", "--package", str(package),
         "--product", _PRODUCT, "--url", "https://a.test/"],
        ["package", "seeds", "enable", "--package", str(package),
         "--product", _PRODUCT, "--url", "https://a.test/"],
    ):
        code, out, _ = _run(capsys, [*argv, "--json"])
        assert code == EXIT_USAGE, out
        assert json.loads(out)["error"] == "usage"
    assert not _seed_file(package).exists()


def test_cli_seeds_disable_unknown_url_is_usage_error(capsys, tmp_path: Path) -> None:
    """Неизвестный URL при disable — ``EXIT_USAGE`` и файл не изменён."""
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://a.test/", "--actor", "roger",
        ],
    )
    before = _seed_file(package).read_bytes()
    code, out, _ = _run(
        capsys,
        [
            "package", "seeds", "disable", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://other.test/",
            "--actor", "roger", "--json",
        ],
    )
    assert code == EXIT_USAGE
    assert json.loads(out)["ok"] is False
    assert _seed_file(package).read_bytes() == before


def test_cli_seeds_dry_run_does_not_write(capsys, tmp_path: Path) -> None:
    """``--dry-run`` на CLI: ``written`` пуст, файл не появился."""
    package = _new(capsys, tmp_path)
    code, out, _ = _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://a.test/",
            "--actor", "roger", "--dry-run", "--json",
        ],
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["changed"] is True
    assert payload["written"] == []
    assert not _seed_file(package).exists()


def test_cli_artifact_get_seeds(capsys, tmp_path: Path) -> None:
    """``artifact get --name seeds`` отдаёт seed-набор (raw и структурно)."""
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://a.test/", "--actor", "roger",
        ],
    )
    code, out, _ = _run(
        capsys, ["package", "artifact", "get", "--package", str(package), "--name", "seeds", "--raw"]
    )
    assert code == EXIT_OK
    assert out.startswith("schema: seeds/1")
    code, out, _ = _run(
        capsys,
        ["package", "artifact", "get", "--package", str(package), "--name", "seeds", "--json"],
    )
    assert code == EXIT_OK
    data = json.loads(out)["data"]
    assert data["products"][_PRODUCT]["seeds"][0]["normalized_url"] == "https://a.test/"


def test_cli_artifact_list_counts_seeds(capsys, tmp_path: Path) -> None:
    """``artifact list`` показывает seed-набор отдельной строкой со счётчиком."""
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://a.test/", "--actor", "roger",
        ],
    )
    code, out, _ = _run(
        capsys, ["package", "artifact", "list", "--package", str(package), "--json"]
    )
    assert code == EXIT_OK
    items = {item["name"]: item for item in json.loads(out)["artifacts"]}
    assert items["seeds"]["present"] is True
    assert items["seeds"]["count"] == 1
    assert items["seeds"]["file"] == SEEDS_RELPATH


def test_cli_seeds_export_out_file(capsys, tmp_path: Path) -> None:
    """``--out`` пишет файл и сообщает об этом; повторный экспорт не перезаписывает."""
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        [
            "package", "seeds", "add", "--package", str(package),
            "--product", _PRODUCT, "--url", "https://a.test/", "--actor", "roger",
        ],
    )
    out_path = tmp_path / "corpus_urls.yaml"
    code, out, _ = _run(
        capsys,
        [
            "package", "seeds", "export", "--package", str(package),
            "--format", "corpus-urls", "--out", str(out_path), "--json",
        ],
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["written"] is True
    assert payload["format"] == "corpus-urls"
    assert load_corpus_urls(out_path) == {_PRODUCT: ["https://a.test/"]}
    code, out, _ = _run(
        capsys,
        [
            "package", "seeds", "export", "--package", str(package),
            "--out", str(out_path), "--json",
        ],
    )
    assert code == EXIT_OK
    assert json.loads(out)["written"] is False


def test_cli_seeds_export_unknown_format_is_usage_error(capsys, tmp_path: Path) -> None:
    """Второго формата нет: ``--format`` отсекается ``argparse`` (``EXIT_USAGE``)."""
    package = _new(capsys, tmp_path)
    code, _, err = _run(
        capsys,
        ["package", "seeds", "export", "--package", str(package), "--format", "json"],
    )
    assert code == EXIT_USAGE
    assert "format" in err


def test_cli_seeds_export_json_without_out_is_usage_error(capsys, tmp_path: Path) -> None:
    """``--json`` без ``--out`` — отказ: stdout экспорта не JSON-документ."""
    package = _new(capsys, tmp_path)
    code, out, _ = _run(
        capsys,
        ["package", "seeds", "export", "--package", str(package), "--json"],
    )
    assert code == EXIT_USAGE
    assert json.loads(out)["error"] == "usage"


# ---------------------------------------------------------------------------
# Проводка: прогон видит результат ``seeds add``
# ---------------------------------------------------------------------------


def test_load_effective_seeds_sees_cli_add(tmp_path: Path) -> None:
    """``load_effective_seeds`` читает ровно то, что записал ``seeds add`` (§7.5)."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _add(package, "https://a.test/")
    _add(package, "https://b.test/")
    seeds_disable(package, _PRODUCT, "https://b.test/", actor="roger", at=_AT)
    config, warnings = load_effective_seeds(package, [_PRODUCT])
    assert config.products[_PRODUCT].state is OnboardingState.READY
    assert [seed.normalized_url for seed in config.products[_PRODUCT].enabled_for()] == [
        "https://a.test/"
    ]
    assert not [w for w in warnings if w.startswith("seeds_legacy_fallback")]


def test_seeds_add_overrides_legacy_corpus_urls(tmp_path: Path) -> None:
    """``seeds.yaml`` приоритетнее legacy ``corpus_urls.yaml`` для того же продукта."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text(
        f"{_PRODUCT}:\n- https://legacy.test/\n", encoding="utf-8"
    )
    _add(package, "https://new.test/")
    config, _ = load_effective_seeds(package, [_PRODUCT])
    assert [seed.normalized_url for seed in config.products[_PRODUCT].enabled_for()] == [
        "https://new.test/"
    ]


def test_seed_file_written_canonically(tmp_path: Path) -> None:
    """Запись канонична: ``load_seeds`` → ``render_seeds`` даёт те же байты."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _add(package, "https://a.test/")
    text = _seed_file(package).read_text(encoding="utf-8")
    assert text == render_seeds(load_seeds(_seed_file(package)))
    assert text.startswith("schema: seeds/1\n")


def test_scenario_warns_when_profile_unbound(tmp_path: Path) -> None:
    """Seed вне объявленной зоны профиля — законный режим, но **видимый** (09 §2.1 п. 2).

    Fallback на дефолтные оси ``fetch``/``extract`` не выдаётся за привязку: ``seeds add``
    не создаёт ключ профиля (синтез осей по A3 требует замера, а не догадки) и называет
    пробел предупреждением, а не молчанием.
    """
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    result = _add(package, "https://nowhere.test/deep/path/")
    assert any("profile_unbound" in line for line in result.warnings)
    seed = load_seeds(_seed_file(package)).products[_PRODUCT].seeds[0]
    assert seed.profile_key is None


def test_scenario_no_profile_warning_when_key_resolved(tmp_path: Path) -> None:
    """Привязанный seed предупреждения не даёт: молчание здесь — факт, а не пропуск."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "domain_profiles.yaml").write_text(
        "example.com:\n  fetch: http\n  extract: auto\n", encoding="utf-8"
    )
    result = _add(package, "https://example.com/docs/")
    assert not any("profile_unbound" in line for line in result.warnings)
    seed = load_seeds(_seed_file(package)).products[_PRODUCT].seeds[0]
    assert seed.profile_key == "example.com"


def test_scenario_export_without_seeds_file_refuses(tmp_path: Path) -> None:
    """Нет ``seeds.yaml`` — экспорт отказывает, а не печатает пустой ``{}``.

    Пустой словарь читался бы как «seed'ов у пакета нет», хотя эффективный набор может
    приходить из legacy ``corpus_urls.yaml`` (09 §7.5): это разные утверждения, и
    производный экспорт нового носителя не вправе выдавать одно за другое.
    """
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text(
        f"{_PRODUCT}:\n  - https://legacy.test/\n", encoding="utf-8"
    )
    with pytest.raises(PackageAdminError, match=r"seeds\.yaml отсутствует"):
        seeds_export(package)
    assert not _seed_file(package).exists()

def test_add_seed_warns_when_it_shadows_legacy_corpus_urls(tmp_path: Path) -> None:
    """Первый seed продукта вытесняет его legacy-набор — это названо, а не молчаливо.

    ``load_effective_seeds`` берёт ``seeds.yaml[product]`` приоритетнее
    ``corpus_urls.yaml[product]``. Значит одна запись в новый носитель отменяет весь
    прежний корпус продукта; без предупреждения оператор увидел бы «добавил seed»,
    а получил «корпус из одного URL».
    """
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    legacy = _write_legacy_corpus_urls(
        package, {_PRODUCT: ["https://legacy.test/a", "https://legacy.test/b"]}
    )
    assert legacy.is_file()

    result = _add(package, "https://example.com/docs/")

    (warning,) = [w for w in result.warnings if w.startswith("legacy_seeds_shadowed")]
    assert _PRODUCT in warning
    assert CORPUS_URLS_RELPATH in warning
    assert "seeds.yaml" in warning
    # Файл записан — предупреждение описывает реальное следствие, а не гипотезу.
    assert _seed_file(package).is_file()


def test_add_seed_does_not_warn_when_legacy_untouched(tmp_path: Path) -> None:
    """Legacy есть, но продукт уже объявлен в ``seeds.yaml`` — не новость, шума нет."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _write_legacy_corpus_urls(package, {_PRODUCT: ["https://legacy.test/a"]})
    assert _add(package, "https://example.com/docs/").warnings  # первый раз — предупредил

    second = _add(package, "https://example.com/docs/2")

    assert not any(w.startswith("legacy_seeds_shadowed") for w in second.warnings)


def test_add_seed_ignores_legacy_of_other_products(tmp_path: Path) -> None:
    """Вытесняется только тот продукт, который попал в набор; чужой legacy продолжает жить."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _write_legacy_corpus_urls(
        package, {"other_product": ["https://legacy.test/a"]}
    )

    result = _add(package, "https://example.com/docs/")

    assert not any(w.startswith("legacy_seeds_shadowed") for w in result.warnings)


def test_add_seed_dry_run_also_warns_about_shadowing(tmp_path: Path) -> None:
    """``--dry-run`` показывает то же следствие: предупреждение — про план, не про запись."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    _write_legacy_corpus_urls(package, {_PRODUCT: ["https://legacy.test/a"]})

    result = _add(package, "https://example.com/docs/", dry_run=True)

    assert any(w.startswith("legacy_seeds_shadowed") for w in result.warnings)
    assert not _seed_file(package).exists()
