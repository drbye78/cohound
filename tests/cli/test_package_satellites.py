"""CLI ``hound package satellites``: список, разведка корней, объявление в паспорте.

Пины держат три вещи, которые иначе расходятся молча:

1. **`list` показывает расхождение с seed-набором.** Корень, объявленный в паспорте, но не
   попавший в `corpus_urls.yaml`, означает непересобранный пакет: прогон его не увидит, а
   `list` скажет «объявлено» — то есть соврёт о готовности.
2. **`roots --apply` правит паспорт узко** — корни сателлита встают в его собственный
   элемент, комментарии и прочие поля остаются дословно (см. `test_passport_patch`).
3. **`roots --apply` без `--actor` отказывает**: правка объявленного корпуса — действие
   человека, и аудит обязан знать, кто его сделал.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.cli.errors import EXIT_USAGE
from core.cli.main import main
from core.cli.package.commands import cmd_package_satellites_roots

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "packages" / "ru-pg-dbms"
PASSPORTS = ROOT / "passports"


def _capture(capsys: pytest.CaptureFixture[str]) -> str:
    return capsys.readouterr().out


def test_list_shows_declared_roots_and_seed_coverage(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "package",
            "satellites",
            "list",
            "--package",
            str(PACKAGE),
            "--product",
            "pangolin",
            "--passports-dir",
            str(PASSPORTS),
        ]
    )
    out = _capture(capsys)
    assert code == 0
    assert "management_console: Kintsugi" in out
    assert "backup: CopyWala" in out
    # Корни объявлены у сателлитов и попали в seed-набор пакета.
    assert "pangolin.sbertech.ru/center/docs/kintsugi" in out
    assert "pangolin.sbertech.ru/center/docs/copywala" in out
    assert "не в corpus_urls" not in out


def test_list_reports_root_missing_from_corpus_urls(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text("pangolin: []\n", encoding="utf-8")
    code = main(
        [
            "package",
            "satellites",
            "list",
            "--package",
            str(package),
            "--product",
            "pangolin",
            "--passports-dir",
            str(PASSPORTS),
        ]
    )
    out = _capture(capsys)
    assert code == 0
    assert "не в corpus_urls" in out


@dataclass
class _RawPage:
    url: str
    content: bytes
    status_code: int = 200


@dataclass
class _Doc:
    text: str = "текст страницы сателлита"
    content_hash: str | None = None
    extractor_version: str = "test-1"
    error: str | None = None


class _Crawler:
    def __init__(self, links: int = 5) -> None:
        self._links = links

    def fetch(self, url: str, *args: Any, **kwargs: Any) -> _RawPage:
        anchors = "".join(
            f'<a href="/center/docs/backup/page-{index}.html">p{index}</a>'
            for index in range(self._links)
        )
        return _RawPage(url=url, content=f"<html><body>{anchors}</body></html>".encode())


class _Extractor:
    def extract(self, page: Any, *args: Any, **kwargs: Any) -> _Doc:
        return _Doc()


class _Robots:
    def allowed(self, url: str) -> bool:
        return True


@dataclass
class _ProbePorts:
    http: Any
    extractor: Any
    browser: Any = None
    robots: Any = None


def _args(
    passports_dir: Path,
    *,
    package_dir: Path | None = None,
    apply: bool,
    actor: str | None,
    out: str | None = None,
) -> Any:
    """Аргументы хендлера. ``package_dir`` обязателен для ``--apply``: иначе артефакт
    разведки лёг бы в **настоящий** пакет (побочный эффект теста)."""
    import argparse

    return argparse.Namespace(
        package=str(package_dir or PACKAGE),
        product="pangolin",
        passports_dir=str(passports_dir),
        role=["backup"],
        sitemap=None,
        apply=apply,
        actor=actor,
        out=out,
        json=False,
        profile="local",
        crawl_probe_ports=_ProbePorts(http=_Crawler(), extractor=_Extractor(), robots=_Robots()),
    )


def test_roots_apply_writes_root_into_satellite_element(tmp_path: Path) -> None:
    passports = tmp_path / "passports"
    shutil.copytree(PASSPORTS / "pangolin", passports / "pangolin")
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    target = next((passports / "pangolin").glob("*.yaml"))
    before = target.read_text(encoding="utf-8")

    code = cmd_package_satellites_roots(
        _args(passports, package_dir=package, apply=True, actor="tester")
    )
    assert code == 0
    after = target.read_text(encoding="utf-8")
    assert after != before
    data = yaml.safe_load(after)
    by_role = {item["role"]: item for item in data["satellites"]}
    # Корень выведен из наблюдения: префикс /center/docs/backup покрыт пятью ссылками.
    assert by_role["backup"]["doc_domains"] == ["pangolin.sbertech.ru/center/docs/backup"]
    # Второй сателлит не тронут, комментарии на месте.
    assert by_role["management_console"]["doc_domains"] == [
        "pangolin.sbertech.ru/center/docs/kintsugi"
    ]
    assert "  # Установлен: бэкапы с FUSE — роль backup.\n" in after


def test_roots_writes_plan_artifact_without_touching_passport(tmp_path: Path) -> None:
    """``--out`` фиксирует **наблюдение** (числа кандидатов), паспорт не меняется.

    Разведка и объявление разделены: без ``--apply`` команда не имеет права расширять
    корпус, но её вывод обязан быть воспроизводимым артефактом — иначе «прибор сказал
    такой корень» проверить нечем.
    """
    passports = tmp_path / "passports"
    shutil.copytree(PASSPORTS / "pangolin", passports / "pangolin")
    target = next((passports / "pangolin").glob("*.yaml"))
    before = target.read_text(encoding="utf-8")
    out = tmp_path / "roots.yaml"

    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    code = cmd_package_satellites_roots(
        _args(passports, package_dir=package, apply=False, actor=None, out=str(out))
    )
    assert code == 0
    assert target.read_text(encoding="utf-8") == before
    plan = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert isinstance(plan, list) and plan
    assert plan[0]["role"] == "backup"
    assert plan[0]["candidates"][0]["key"] == "pangolin.sbertech.ru/center/docs/backup"

def test_roots_apply_requires_actor(tmp_path: Path) -> None:
    passports = tmp_path / "passports"
    shutil.copytree(PASSPORTS / "pangolin", passports / "pangolin")
    # Без --apply команда работает (dry-run); отказ — только при --apply без --actor.
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    assert (
        cmd_package_satellites_roots(
            _args(passports, package_dir=package, apply=False, actor=None)
        )
        == 0
    )
    with pytest.raises(Exception) as apply_error:
        cmd_package_satellites_roots(
            _args(passports, package_dir=package, apply=True, actor=None)
        )
    assert getattr(apply_error.value, "exit_code", None) == EXIT_USAGE
