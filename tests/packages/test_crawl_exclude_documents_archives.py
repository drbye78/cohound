"""Регрессия приёма архивов (срез A): блэклист пакета не режет допущенные контейнеры.

Субстрат приёма уже в ядре (оси ``crawl.attachment_formats`` {zip,7z,rar},
``crawl.max_artifact_bytes`` 256 MiB, потоковый staging, ``classify_archive_bytes``), но
**данные** пакета резали архивы до постановки в очередь: ``exclude_documents`` содержал
``*.zip``/``*.rar``/``*.7z``. Измеренный контейнер
``https://jatoba.ru/download/documentationJatoba18_4.zip`` (125 727 643 B, 47 PDF-членов,
3 629 страниц) был увидён в ретененном raw главной, но отброшен блэклистом по basename.

Инвариант среза:

* в ``exclude_documents`` **нет** форматов, для которых есть читатель-контейнер
  (:data:`SUPPORTED_ARCHIVE_FORMATS` — позитивная вселенная ``attachment_formats``);
* остаются расширения без читателя (``*.gz``/``*.tar``/``*.tgz`` — читателя нет), образы
  (``*.ova``/``*.iso``/``*.vmdk``/``*.qcow2`` — не admitted вовсе), инсталляторы, медиа и
  статика;
* список правится в двух местах — on-disk ``run.yaml`` и генератор
  ``sources/build_package.py`` — иначе повторная генерация молча вернёт дрейф;
* ось ``attachment_formats`` в ``run.yaml`` **не** прописывается: app-дефолт уже равен
  всем поддерживаемым форматам, а явное значение сделало бы ось explicit и изменило
  ``CrawlLimits.as_dict()``/дайджест.

Смысловая (сквозная) проверка идёт через реальные ``resolve_crawl_limits`` /
``is_blacklisted`` на загруженном ``run.yaml`` пакета — не на выдуманных константах.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from core.config import load_run_config
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import load_domain_profiles
from core.pipeline.crawl_frontier import is_blacklisted, resolve_crawl_limits
from core.util.document_formats import SUPPORTED_ARCHIVE_FORMATS

ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = ROOT / "packages" / "ru-pg-dbms"
GENERATOR = PACKAGE_DIR / "sources" / "build_package.py"
ON_DISK_RUN = PACKAGE_DIR / "run.yaml"
ON_DISK_PROFILES = PACKAGE_DIR / "domain_profiles.yaml"

#: Форматы-контейнеры: позитивная вселенная допуска (``attachment_formats``), блэклистом
#: не режутся — их судьбу решает классификатор/OOXML-маркер, а не basename.
ADMITTED_CONTAINER_GLOBS = frozenset({"*.zip", "*.rar", "*.7z"})

#: Расширения, которые обязаны остаться в блэклисте: нет читателя (``.gz``/``.tar``/``.tgz``),
#: не admitted (образы ``.ova``/``.iso``/``.vmdk``/``.qcow2``), инсталляторы, медиа, статика.
RETAINED_BLACKLIST_GLOBS = frozenset(
    {
        "*.gz",
        "*.tar",
        "*.tgz",
        "*.exe",
        "*.msi",
        "*.dmg",
        "*.iso",
        "*.ova",
        "*.vmdk",
        "*.qcow2",
        "*.mp4",
        "*.avi",
        "*.css",
        "*.js",
        "*.png",
        "*.woff2",
    }
)

#: Измеренный владельцем контейнер корпусного корпуса (байты) — см. docstring модуля.
MEASURED_ARCHIVE_URL = "https://jatoba.ru/download/documentationJatoba18_4.zip"
MEASURED_ARCHIVE_BYTES = 125_727_643
#: Образ без читателя и вне позитивной вселенной допуска — обязан остаться отсечённым.
IMAGE_URL = "https://jatoba.ru/download/DEV1-student-12.ova"


def _load_generator():
    """Загружает генератор по пути (каталог ``packages/`` не является пакетом)."""
    spec = importlib.util.spec_from_file_location(
        "_ru_pg_dbms_build_package_exclude_docs", GENERATOR
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _on_disk_crawl() -> dict:
    """Секция ``crawl`` on-disk ``run.yaml`` пакета (как данные, не как текст)."""
    return yaml.safe_load(ON_DISK_RUN.read_text(encoding="utf-8"))["crawl"]


def _limits(url: str):
    """Эффективные лимиты для ``url`` по реальной цепочке app < run < site."""
    cfg = load_run_config(ON_DISK_RUN)
    profiles = load_domain_profiles(ON_DISK_PROFILES)
    return resolve_crawl_limits(
        url, app=CrawlConfig(), run=cfg.crawl, profiles=profiles
    )


def test_run_yaml_blacklist_keeps_containers_and_drops_readerless_suffixes() -> None:
    """``exclude_documents`` пакета: контейнеры допущены, остальное сохранено (срез A)."""
    crawl = _on_disk_crawl()
    globs = set(crawl["exclude_documents"])

    assert not (globs & ADMITTED_CONTAINER_GLOBS), (
        "форматы с читателем (.zip/.rar/.7z) не должны резаться блэклистом: "
        f"лишние {sorted(globs & ADMITTED_CONTAINER_GLOBS)}"
    )
    assert RETAINED_BLACKLIST_GLOBS <= globs, (
        "потеряны расширения без читателя/вне допуска: "
        f"{sorted(RETAINED_BLACKLIST_GLOBS - globs)}"
    )
    # Границы среза: правка данных не расширяет допуск — ``attachment_formats`` не
    # прописывается в run.yaml (app-дефолт = все поддерживаемые, ось не explicit).
    assert "attachment_formats" not in crawl, (
        "attachment_formats в run.yaml не нужен: дефолт app-слоя уже равен "
        f"{list(SUPPORTED_ARCHIVE_FORMATS)}, а явное значение меняет as_dict()/дайджест"
    )


def test_generator_emits_same_exclude_documents_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Генератор эмитит тот же список, что лежит в ``run.yaml`` (нет ``generator_drift``).

    ``main()`` запускается с подменёнными I/O-границами (пустые источники, ``PACKAGE_DIR``
    → ``tmp_path``), поэтому внешние xlsx не нужны и пакет не мутируется.
    """
    module = _load_generator()
    (tmp_path / "sources").mkdir()
    monkeypatch.setattr(module, "PACKAGE_DIR", tmp_path)
    monkeypatch.setattr(module, "read_psq", lambda: [])
    monkeypatch.setattr(module, "read_golden", lambda: {})
    monkeypatch.setattr(module, "_sha256", lambda path: "0" * 64)

    assert module.main() == 0

    emitted = yaml.safe_load((tmp_path / "run.yaml").read_text(encoding="utf-8"))
    emitted_globs = list(emitted["crawl"]["exclude_documents"])
    on_disk_globs = list(_on_disk_crawl()["exclude_documents"])

    assert emitted_globs == on_disk_globs
    # Тот же инвариант на выходе генератора — регрессия читается по имени списка.
    assert not (set(emitted_globs) & ADMITTED_CONTAINER_GLOBS)
    assert RETAINED_BLACKLIST_GLOBS <= set(emitted_globs)


def test_measured_zip_is_not_blacklisted_while_image_stays_excluded() -> None:
    """Сквозной смысл: корпусный ``.zip`` проходит блэклист, ``.ova`` — по-прежнему нет."""
    zip_limits = _limits(MEASURED_ARCHIVE_URL)
    assert is_blacklisted(MEASURED_ARCHIVE_URL, zip_limits) is False, (
        "измеренный контейнер 125 727 643 B обязан дойти до очереди: его режет только "
        "отсутствие читателя, а не basename"
    )
    # Потолок артефакта — app-дефолт 256 MiB, заданный в ядре; в run.yaml ось не нужна.
    assert zip_limits.max_artifact_bytes == CrawlConfig().max_artifact_bytes
    assert zip_limits.max_artifact_bytes > MEASURED_ARCHIVE_BYTES
    assert zip_limits.attachment_formats == SUPPORTED_ARCHIVE_FORMATS
    assert zip_limits.attachment_formats_explicit is False
    assert zip_limits.max_artifact_bytes_explicit is False

    image_limits = _limits(IMAGE_URL)
    assert is_blacklisted(IMAGE_URL, image_limits) is True, (
        "образ .ova вне позитивной вселенной допуска (attachment_formats) — остаётся "
        "отсечённым блэклистом"
    )
