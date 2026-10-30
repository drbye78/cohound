"""Retirement продукта через ``run``: авторский ``state: retired`` → durable ``retired``.

09 §5.1/§15.1 (T15). Retirement — **решение человека в данных** (``sources/seeds.yaml``),
а не отдельная команда, поэтому единственный путь записи — сверка seed-набора. Прогон
отражает объявленный ``retired`` **до** выхода на блокировку: продукт, у которого убрали
seed'ы, блокирует прогон (O1), и откладывать запись «до разблокировки» значило бы не
записать её никогда — а без неё переход «выведен → возвращён» не наблюдаем и
``re_added_at`` остаётся без производителя.

Проверяется и обратное: §4.1 («заблокированный прогон state не создаёт») не нарушен —
без существующего ``state.db`` команда ничего не создаёт и не падает.

Сеть не используется.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from core.cli.main import EXIT_BLOCKED, main
from core.domain.enums import OnboardingState
from core.ports.state import ProductStateRow
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore
from tests.helpers.packages import run_argv, valid_package

pytestmark = pytest.mark.usefixtures("isolated_state_dir")

PRODUCT_ID = "p3"


def _declare_retired(package: Path) -> None:
    """Продукт в когорте/run, объявленный ``retired`` без единого seed'а."""
    cohort = yaml.safe_load((package / "cohort.yaml").read_text(encoding="utf-8"))
    cohort["entries"].append(
        {
            "product_id": PRODUCT_ID,
            "role_overlap": 0.5,
            "selection_reason": "выведен из работы (тест)",
            "anchor_relation": "analog",
        }
    )
    (package / "cohort.yaml").write_text(
        yaml.safe_dump(cohort, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    run = yaml.safe_load((package / "run.yaml").read_text(encoding="utf-8"))
    run["products"] = [*run["products"], PRODUCT_ID]
    (package / "run.yaml").write_text(
        yaml.safe_dump(run, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    seeds_path = package / "sources" / "seeds.yaml"
    seeds = yaml.safe_load(seeds_path.read_text(encoding="utf-8"))
    seeds["products"][PRODUCT_ID] = {"state": "retired", "seeds": []}
    seeds_path.write_text(
        yaml.safe_dump(seeds, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def _seed_ready_state(state_dir: Path, package_id: str) -> Path:
    """Существующее durable-состояние продукта: ``ready`` с рубежом корпуса."""
    db_path = state_db_path(state_dir, package_id)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteStateStore(db_path)
    try:
        store.upsert_product_state(
            ProductStateRow(
                product_id=PRODUCT_ID,
                canonical_name="Product Three",
                onboarding_state=OnboardingState.READY,
                added_at="2026-01-01T00:00:00+00:00",
                added_by="tester",
                last_corpus_run_id="prev-run",
            )
        )
    finally:
        store.close()
    return db_path


def _product_state(db_path: Path) -> ProductStateRow | None:
    store = SqliteStateStore(db_path)
    try:
        return store.get_product_state(PRODUCT_ID)
    finally:
        store.close()


def test_declared_retired_reaches_durable_state(
    tmp_path: Path, isolated_state_dir: Path
) -> None:
    """``state: retired`` в seeds.yaml → ``product_state.onboarding_state='retired'``."""
    package = valid_package(tmp_path)
    _declare_retired(package)
    db_path = _seed_ready_state(isolated_state_dir, package.name)
    assert _product_state(db_path).onboarding_state is OnboardingState.READY

    code = main(run_argv(package, run_id="retire-p3", db=tmp_path / "cohound.db"))

    assert code == EXIT_BLOCKED  # продукт без активных seed'ов блокирует прогон (O1)
    row = _product_state(db_path)
    assert row is not None
    assert row.onboarding_state is OnboardingState.RETIRED
    assert row.retired_at is not None
    # Возврата не было: метка «вернулся» не выставляется выдуманно.
    assert row.re_added_at is None


def test_blocked_run_still_does_not_create_state(
    tmp_path: Path, isolated_state_dir: Path
) -> None:
    """§4.1: объявленный ``retired`` не заставляет заблокированный прогон создать state."""
    package = valid_package(tmp_path)
    _declare_retired(package)
    assert not state_db_path(isolated_state_dir, package.name).exists()

    code = main(run_argv(package, run_id="retire-nostate", db=tmp_path / "cohound.db"))

    assert code == EXIT_BLOCKED
    assert not state_db_path(isolated_state_dir, package.name).exists()


def test_declared_retired_with_live_seeds_does_not_flip_flop(
    tmp_path: Path, isolated_state_dir: Path
) -> None:
    """``state: retired`` при живом seed'е — противоречие в данных, а не повод писать.

    Иначе на одном и том же файле режим мигал бы: сверка помечает ``retired``, а
    следующая ветка ``if rows:`` возвращает набор и выставляет ``re_added_at`` → ``new``.
    """
    package = valid_package(tmp_path)
    _declare_retired(package)
    seeds_path = package / "sources" / "seeds.yaml"
    seeds = yaml.safe_load(seeds_path.read_text(encoding="utf-8"))
    seeds["products"][PRODUCT_ID]["seeds"] = [
        {
            "url": "https://example.test/p3",
            "role": "primary",
            "source": "operator",
            "added_at": "2026-01-01T00:00:00+00:00",
            "added_by": "tester",
        }
    ]
    seeds_path.write_text(
        yaml.safe_dump(seeds, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    db_path = _seed_ready_state(isolated_state_dir, package.name)

    main(run_argv(package, run_id="retire-live", db=tmp_path / "cohound.db"))

    row = _product_state(db_path)
    assert row is not None
    assert row.onboarding_state is OnboardingState.READY
    assert row.retired_at is None
    assert row.re_added_at is None
