"""Регрессионный тест генератора пакета ``ru-pg-dbms`` (фаза P3, Deliverable B).

Генератор ``packages/ru-pg-dbms/sources/build_package.py`` обязан выдавать артефакты,
которые принимает строгий загрузчик ``core.config.domain_profiles`` (``extra="forbid"``).
Исторически он писал мёртвую схему ``render: {mode: …}`` / ``index: {partition_key: …}``,
из-за чего повторная генерация давала невалидный ``domain_profiles.yaml`` .

Тест вызывает **чистый** эмиттер ``build_domain_profiles()`` (без I/O; ``main()`` требует
внешних xlsx) и прогоняет его выход через ``load_domain_profiles``. Дополнительно
сверяет общие ключи с ручной (эталонной) версией на диске: генератор не должен
расходиться с ней по уже зафиксированным доменам.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from core.config.domain_profiles import DomainProfile, load_domain_profiles

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "packages" / "ru-pg-dbms" / "sources" / "build_package.py"
ON_DISK_PROFILES = ROOT / "packages" / "ru-pg-dbms" / "domain_profiles.yaml"


def _load_generator():
    """Загружает генератор по пути (каталог ``packages/`` не является пакетом).

    Модуль импортируется **без** extra ``extract``: ``openpyxl`` теперь ленивый (только
    внутри читателей xlsx), иначе этот регрессионный тест молча скипался бы в каноническом
    dev/CI-окружении, а дрейф схемы ``domain_profiles`` вернулся бы незамеченным.
    """
    spec = importlib.util.spec_from_file_location("_ru_pg_dbms_build_package", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_generator_emits_domain_profiles_accepted_by_strict_loader(tmp_path: Path) -> None:
    """Выход ``build_domain_profiles()`` проходит ``load_domain_profiles`` (D.2)."""
    module = _load_generator()
    profiles = module.build_domain_profiles()
    assert profiles, "генератор обязан выдавать хотя бы один доменный профиль"

    # Строгая схема: неизвестные поля (render/index) были бы отвергнуты.
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(yaml.safe_dump(profiles, allow_unicode=True, sort_keys=True), encoding="utf-8")
    loaded = load_domain_profiles(path)

    assert set(loaded) == set(profiles)
    for key, profile in loaded.items():
        assert isinstance(profile, DomainProfile)
    # XML-версия мёртвой схемы не должна присутствовать в выходе.
    for key, raw in profiles.items():
        assert set(raw) <= {"fetch", "extract", "wait_for", "strategy", "rate_limit"}, key


def test_generator_matches_reference_on_disk() -> None:
    """Выход генератора совпадает с ручной (эталонной) ``domain_profiles.yaml``.

    Регрессия на потерю данных: повторный прогон генератора не должен затирать
    hand-corrected набор доменов (иначе пропадут ``platformv.sbertech.ru`` и др.).
    """
    module = _load_generator()
    generated = module.build_domain_profiles()
    reference = load_domain_profiles(ON_DISK_PROFILES)

    assert set(generated) == set(reference), (
        f"расхождение ключей: только в генераторе {sorted(set(generated) - set(reference))}, "
        f"только в эталоне {sorted(set(reference) - set(generated))}"
    )
    for key in sorted(generated):
        assert DomainProfile.model_validate(generated[key]) == reference[key], key


def test_generator_output_is_lint_clean_when_written(tmp_path: Path) -> None:
    """Записанный эмиттером профиль грузится строгим загрузчиком без исключений.

    Прямая регрессия на баг P3: раньше ``DomainProfile(extra="forbid")`` падал на
    ``render``/``index``; теперь выход валиден.
    """
    module = _load_generator()
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(
        yaml.safe_dump(module.build_domain_profiles(), allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )
    # Не должно быть исключений валидации.
    load_domain_profiles(path)


if __name__ == "__main__":  # pragma: no cover - локальный запуск
    raise SystemExit(pytest.main([__file__, "-q"]))
