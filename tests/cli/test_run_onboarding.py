"""Тесты onboarding-префлайта ``run`` (09 §2.2/§7.5, MVP-1).

Проверяют: продукт без seed'ов блокирует прогон (``EXIT_BLOCKED``) и не пишет
corpus/cells; ``--dry-run`` показывает ``blocked``/``mode``; ``--urls`` полностью
заменяет пакетный набор; legacy ``corpus_urls.yaml`` даёт warning
``seeds_legacy_fallback``. Сеть не используется.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from core.cli.main import EXIT_BLOCKED, EXIT_OK, main
from core.cli.wiring import build_store
from tests.helpers.output import json_out
from tests.helpers.packages import run_argv, valid_package


def _blocked_product(package: Path, product_id: str = "p3") -> None:
    """Добавляет продукт в когорту/run без seed'а (``pending_seeds``-эквивалент)."""
    cohort = yaml.safe_load((package / "cohort.yaml").read_text(encoding="utf-8"))
    cohort["entries"].append(
        {
            "product_id": product_id,
            "role_overlap": 0.5,
            "selection_reason": "новый продукт (тест)",
            "anchor_relation": "analog",
        }
    )
    (package / "cohort.yaml").write_text(
        yaml.safe_dump(cohort, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    run = yaml.safe_load((package / "run.yaml").read_text(encoding="utf-8"))
    run["products"] = [*run["products"], product_id]
    (package / "run.yaml").write_text(
        yaml.safe_dump(run, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def test_run_blocks_product_without_seeds(tmp_path: Path) -> None:
    """Продукт без seed'ов → ``EXIT_BLOCKED``, без corpus/cells (O1)."""
    package = valid_package(tmp_path)
    _blocked_product(package)
    db = tmp_path / "cohound.db"

    code = main(run_argv(package, run_id="no-seeds", db=db))

    assert code == EXIT_BLOCKED
    with build_store(db) as store:
        assert list(store.iter_cells("no-seeds")) == []
        assert store.get_crawl_session("crawl:no-seeds") is None
    # MVP-1: durable state.db ещё не существует — блокировка его не создаёт.
    assert not (tmp_path / "state").exists()


def test_run_blocked_payload_lists_product_and_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    _blocked_product(package)
    db = tmp_path / "cohound.db"

    assert main(run_argv(package, run_id="no-seeds-json", db=db)) == EXIT_BLOCKED
    payload = json_out(capsys)

    assert payload["status"] == "blocked"
    assert payload["stopped_at"] == "onboarding"
    assert payload["blocked"] == ["p3"]
    assert payload["onboarding_missing"]["p3"] == ["no_seeds"]
    assert any("product_not_ready: p3" in error for error in payload["errors"])


def test_dry_run_reports_blocked_without_writing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--dry-run`` тоже прогоняет префлайт и показывает ``blocked``/``mode``."""
    package = valid_package(tmp_path)
    _blocked_product(package)
    db = tmp_path / "cohound.db"

    code = main(run_argv(package, run_id="dry", db=db, dry_run=True))
    payload = json_out(capsys)

    assert code == EXIT_BLOCKED
    assert payload["dry_run"] is True
    assert payload["blocked"] == ["p3"]
    assert not db.exists()
    plans = {plan["product_id"]: plan for plan in payload["products"]}
    assert plans["p3"]["mode"] == "new"
    assert plans["p3"]["onboarding_state"] == "pending_seeds"
    assert plans["p1"]["onboarding_state"] == "ready"


def test_urls_override_replaces_package_seeds(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--urls`` — полная замена: даже пустой продукт в seeds.yaml не блокирует."""
    package = valid_package(tmp_path)
    _blocked_product(package)
    db = tmp_path / "cohound.db"
    urls = tmp_path / "urls.json"
    urls.write_text(
        json.dumps({"p1": ["https://p1.test/"], "p2": [], "p3": ["https://p3.test/"]}),
        encoding="utf-8",
    )

    code = main(
        [*run_argv(package, run_id="urls", db=db, until="plan"), "--urls", str(urls)]
    )

    assert code == EXIT_OK


def test_urls_override_emits_warning(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    urls = tmp_path / "urls.json"
    urls.write_text(
        json.dumps({"p1": ["https://p1.test/"], "p2": ["https://p2.test/"]}),
        encoding="utf-8",
    )

    assert (
        main(
            run_argv(package, run_id="urls-warn", db=db, dry_run=True, extra=["--urls", str(urls)])
        )
        == EXIT_OK
    )
    payload = json_out(capsys)
    assert any("seeds_operator_override" in warning for warning in payload["warnings"])


def test_legacy_corpus_urls_fallback_warns(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет seeds.yaml → legacy fallback по продукту с warning ``seeds_legacy_fallback``."""
    package = valid_package(tmp_path)
    (package / "sources" / "seeds.yaml").unlink()
    (package / "sources" / "corpus_urls.yaml").write_text(
        "p1:\n- https://p1.test/\np2:\n- https://p2.test/\n", encoding="utf-8"
    )
    db = tmp_path / "cohound.db"

    code = main(run_argv(package, run_id="legacy", db=db, dry_run=True))
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["blocked"] == []
    assert any("seeds_legacy_fallback: p1" in w for w in payload["warnings"])
    assert payload["seeds"] == {"p1": 1, "p2": 1}


def test_seeds_file_empty_product_blocks_even_if_legacy_has_urls(tmp_path: Path) -> None:
    """``seeds.yaml[product]`` приоритетнее legacy: пустой набор блокирует (09 §7.5)."""
    package = valid_package(tmp_path)
    (package / "sources" / "corpus_urls.yaml").write_text(
        "p2:\n- https://p2.test/\n", encoding="utf-8"
    )
    seeds = yaml.safe_load((package / "sources" / "seeds.yaml").read_text(encoding="utf-8"))
    seeds["products"]["p2"] = {"state": "pending_seeds", "seeds": []}
    (package / "sources" / "seeds.yaml").write_text(
        yaml.safe_dump(seeds, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    db = tmp_path / "cohound.db"

    assert main(run_argv(package, run_id="empty-p2", db=db)) == EXIT_BLOCKED


def test_seed_without_profile_blocks_with_profile_unbound(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Seed, чей ``profile_key`` исчез из domain_profiles.yaml → ``profile_unbound``."""
    package = valid_package(tmp_path)
    seeds = yaml.safe_load((package / "sources" / "seeds.yaml").read_text(encoding="utf-8"))
    seeds["products"]["p1"]["seeds"][0]["profile_key"] = "missing.test"
    (package / "sources" / "seeds.yaml").write_text(
        yaml.safe_dump(seeds, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    db = tmp_path / "cohound.db"

    code = main(run_argv(package, run_id="unbound", db=db, dry_run=True))
    payload = json_out(capsys)

    assert code == EXIT_BLOCKED
    assert payload["blocked"] == ["p1"]
    reasons = payload["onboarding_missing"]["p1"]
    assert any(reason.startswith("profile_unbound:") for reason in reasons)
