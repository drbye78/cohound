"""Пин: ярлык методологии в ``packages/*/run.yaml`` равен семантике кода (§13).

``methodology_version`` — объявленный вход, а не отдельная настройка пакета: правила
берутся из кода. Пакет, объявивший другую версию, заставит манифест записать чужую
методологию, а ``core/pipeline/state_adopt.py`` уже сегодня не примет такой прогон при
переносе состояния (``methodology_matches``).

Тест намеренно **падает при следующем бампе** :data:`METHODOLOGY_VERSION` без правки
пакетов: это и есть цель пина — расхождение должно быть видно в CI, а не в манифесте
живого прогона. Шаблон ``_template`` (имя с подчёркивания) ярлык не объявляет и не
проверяется: он не кейс, дефолт кода для него и есть версия.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from core.domain.models import METHODOLOGY_VERSION

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"


def _declared_run_yamls() -> list[Path]:
    """``run.yaml`` реальных пакетов (шаблоны, начинающиеся с ``_``, — не кейсы)."""
    return sorted(
        path
        for path in PACKAGES.glob("*/run.yaml")
        if not path.parent.name.startswith("_")
    )


@pytest.mark.parametrize(
    "run_yaml", _declared_run_yamls(), ids=lambda path: path.parent.name
)
def test_package_declares_current_methodology_version(run_yaml: Path) -> None:
    """Объявленный ярлык равен версии кода — иначе ложный провенанс в манифесте."""
    data = yaml.safe_load(run_yaml.read_text(encoding="utf-8"))
    declared = data.get("methodology_version")
    assert declared is not None, (
        f"{run_yaml.relative_to(ROOT)}: пакет обязан объявлять methodology_version "
        f"(семантика кода сейчас {METHODOLOGY_VERSION})"
    )
    assert declared == METHODOLOGY_VERSION, (
        f"{run_yaml.relative_to(ROOT)}: methodology_version={declared!r}, "
        f"а код исполняет семантику {METHODOLOGY_VERSION!r}"
    )


def test_pin_covers_real_packages() -> None:
    """Список не должен опустеть: пустая параметризация прошла бы «зелёной» молча."""
    assert _declared_run_yamls(), "в packages/ не нашлось ни одного run.yaml кейса"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))