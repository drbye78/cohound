"""Хелперы пакетов и ``run``-argv для тестов CLI и конвейера.

Объединяет идентичные ``_valid_package``/``_capturing_factory`` и сводит различавшиеся
лишь набором флагов ``_run_argv`` к единому параметризованному билдеру. Порядок
аргументов сохранён (argparse к порядку нечувствителен).
"""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.pipeline import build_phases

#: Общий каталог фикстур пакета (``tests/config/fixtures``).
CONFIG_FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
#: Имя run-манифеста в пакете.
RUN_YAML_NAME = "run.yaml"
#: Тело процедуры аттестации полноты источника (снимает блок I2b).
_SCOPE_ATTESTATION = 'procedure: "ручная аттестация полноты источника (тест)"\n'

__all__ = [
    "CONFIG_FIXTURES",
    "RUN_YAML_NAME",
    "capturing_factory",
    "run_argv",
    "valid_package",
    "write_text",
]


def valid_package(
    tmp_path: Path,
    *,
    fixtures: Path = CONFIG_FIXTURES,
    scope_attestation: bool = True,
) -> Path:
    """Копия ``fixtures`` → ``tmp_path/package`` (линт-чистый пакет при аттестации).

    ``scope_attestation=False`` повторяет вариант без ``scope_attestation.yaml`` —
    каталог/веса/когорта восстанавливаются из пакета без процедуры аттестации.
    """
    package = tmp_path / "package"
    package.mkdir()
    for item in fixtures.iterdir():
        if item.is_file():
            shutil.copy(item, package / item.name)
    if scope_attestation:
        (package / "scope_attestation.yaml").write_text(_SCOPE_ATTESTATION, encoding="utf-8")
    return package


def run_argv(
    package: Path,
    *,
    run_id: str,
    db: Path | None = None,
    runs_dir: Path | None = None,
    until: str | None = None,
    dry_run: bool = False,
    run_yaml_name: str = RUN_YAML_NAME,
    extra: Sequence[str] = (),
) -> list[str]:
    """Базовый ``run``-argv: пакет + опциональные ``--db``/``--runs-dir``/``--until``."""
    argv = [
        "run",
        "--run-yaml",
        str(package / run_yaml_name),
        "--package",
        str(package),
    ]
    if db is not None:
        argv += ["--db", str(db)]
    if runs_dir is not None:
        argv += ["--runs-dir", str(runs_dir)]
    argv += ["--run-id", run_id]
    if until is not None:
        argv += ["--until", until]
    argv += ["--json"]
    if dry_run:
        argv.append("--dry-run")
    argv += list(extra)
    return argv


def write_text(tmp_path: Path, name: str, text: str) -> Path:
    """Пишет ``text`` (UTF-8) в ``tmp_path/name`` и возвращает путь."""
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def capturing_factory() -> tuple[dict[str, Any], Any]:
    """Возвращает ``(captured, factory)``: фабрика сохраняет kwargs и строит фазы."""
    captured: dict[str, Any] = {}

    def factory(**kwargs: Any) -> dict:
        captured.update(kwargs)
        return build_phases(**kwargs)

    return captured, factory
