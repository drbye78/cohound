"""Регрессионный тест генератора пакета ``ru-pg-dbms`` (фаза P3, Deliverable B).

Генератор ``packages/ru-pg-dbms/sources/build_package.py`` обязан выдавать артефакты,
которые принимает строгий загрузчик ``core.config.domain_profiles`` (``extra="forbid"``).
Исторически он писал мёртвую схему ``render: {mode: …}`` / ``index: {partition_key: …}``,
из-за чего повторная генерация давала невалидный ``domain_profiles.yaml`` .

Тест вызывает **чистый** эмиттер ``build_domain_profiles()`` (без I/O; ``main()`` требует
внешних xlsx) и прогоняет его выход через ``load_domain_profiles``. Дополнительно
сверяет общие ключи с ручной (эталонной) версией на диске: генератор не должен
расходиться с ней по уже зафиксированным доменам.

P7 (полнота корпуса): добавлены пины на бюджетные оси ``max_depth``/``max_documents``,
на per-site ``sitemap`` (включая ловушку матчинга ``sbertech.ru``) и на блок ``crawl:``
в эмитируемом ``run.yaml`` (дрейф «файл правили руками, генератор — нет»).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from core.config import load_run_config
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile, load_domain_profiles
from core.domain import CrawlStrategy, DocumentLevel, FetchMode, RateLimitSpec
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.fetch_plan import select_domain_profile

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "packages" / "ru-pg-dbms" / "sources" / "build_package.py"
ON_DISK_PROFILES = ROOT / "packages" / "ru-pg-dbms" / "domain_profiles.yaml"
ON_DISK_RUN = ROOT / "packages" / "ru-pg-dbms" / "run.yaml"

#: Допустимые поля профиля (строгая схема ``DomainProfile``; ``extra="forbid"``).
PROFILE_FIELDS = {
    "fetch",
    "extract",
    "wait_for",
    "strategy",
    "rate_limit",
    "max_workers",
    "extract_workers",
    "max_depth",
    "max_documents",
    "exclude_paths",
    "exclude_documents",
    "document_level",
    "sitemap",
}

#: P7 gate §3: бюджеты ``run.yaml:crawl`` (run-слой, execution-дефолты).
RUN_CRAWL_MAX_DEPTH = 8
RUN_CRAWL_MAX_DOCUMENTS = 300
RUN_CRAWL_MAX_WORKERS = 2
RUN_CRAWL_EXTRACT_WORKERS = 2
RUN_CRAWL_EXCLUDE_PATHS = [
    "/blog/*",
    "/news/*",
    "/events/*",
    "/cases/*",
    "/mediacenter/*",
]
RUN_CRAWL_EXCLUDE_DOCUMENTS = [
    "*.zip", "*.rar", "*.7z", "*.gz", "*.tar", "*.exe", "*.msi", "*.dmg",
    "*.iso", "*.mp4", "*.avi", "*.css", "*.js", "*.png", "*.jpg", "*.jpeg",
    "*.gif", "*.svg", "*.webp", "*.ico", "*.woff", "*.woff2", "*.ttf",
]


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


def _without_comments(text: str) -> str:
    """Убирает полнострочные ``#``-комментарии и пустые строки (YAML-тело как есть)."""
    return "".join(
        line
        for line in text.splitlines(keepends=True)
        if line.strip() and not line.lstrip().startswith("#")
    )


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
    # Мёртвая схема (render/index) не должна присутствовать в выходе.
    for key, raw in profiles.items():
        assert set(raw) <= PROFILE_FIELDS, key


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


def test_on_disk_profiles_body_is_exactly_generator_dump() -> None:
    """On-disk файл = ``yaml_dump(build_domain_profiles())`` + полнострочные комментарии.

    Пин «байт-в-байт по содержимому»: любая правка значений/порядка ключей в файле без
    правки генератора (или наоборот) ломает тест. Разрешены только полнострочные
    ``#``-комментарии и пустые строки — они не являются данными.
    """
    module = _load_generator()
    dumped = module.yaml_dump(module.build_domain_profiles())
    on_disk = _without_comments(ON_DISK_PROFILES.read_text(encoding="utf-8"))
    assert on_disk == dumped


def test_generator_emits_run_crawl_block_matching_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Эмитируемый ``run.yaml:crawl`` равен on-disk блоку (дрейф не вернётся).

    ``main()`` запускается с подменёнными I/O-границами (пустые источники, ``PACKAGE_DIR``
    → ``tmp_path``), поэтому внешние xlsx не нужны и ничего в пакете не мутируется.
    """
    module = _load_generator()
    (tmp_path / "sources").mkdir()
    monkeypatch.setattr(module, "PACKAGE_DIR", tmp_path)
    monkeypatch.setattr(module, "read_psq", lambda: [])
    monkeypatch.setattr(module, "read_golden", lambda: {})
    monkeypatch.setattr(module, "_sha256", lambda path: "0" * 64)

    assert module.main() == 0

    emitted = yaml.safe_load((tmp_path / "run.yaml").read_text(encoding="utf-8"))
    on_disk = yaml.safe_load(ON_DISK_RUN.read_text(encoding="utf-8"))
    assert emitted["crawl"] == on_disk["crawl"]


def test_run_crawl_axes_accepted_by_strict_loader() -> None:
    """On-disk ``run.yaml`` проходит строгий ``load_run_config`` с новыми осями (P7)."""
    cfg = load_run_config(ON_DISK_RUN)
    crawl = cfg.crawl
    assert crawl is not None, "run.yaml обязан нести секцию crawl"

    assert crawl.strategy is CrawlStrategy.HONEST
    assert crawl.rate_limit == RateLimitSpec(min_s=1.0, max_s=3.0)
    assert crawl.max_depth == RUN_CRAWL_MAX_DEPTH
    assert crawl.max_documents == RUN_CRAWL_MAX_DOCUMENTS
    assert crawl.max_workers == RUN_CRAWL_MAX_WORKERS
    assert crawl.extract_workers == RUN_CRAWL_EXTRACT_WORKERS
    assert crawl.document_level is DocumentLevel.FULL
    assert list(crawl.exclude_paths or []) == RUN_CRAWL_EXCLUDE_PATHS
    assert list(crawl.exclude_documents or []) == RUN_CRAWL_EXCLUDE_DOCUMENTS


@pytest.mark.parametrize(
    ("url", "max_depth", "max_documents"),
    [
        # seed-корень портала /docs без sitemap — бюджет и есть ограничитель.
        ("https://postgrespro.ru/docs/16/", 8, 500),
        # 23 URL, плоско.
        ("https://pangolin.sbertech.ru/", 2, 60),
        # Новый ключ: www.gaz-is.ru раньше не матчился ничем.
        ("https://www.gaz-is.ru/", 8, 200),
        # path-ключ приоритетнее host-ключа (sitemap только здесь).
        ("https://sbertech.ru/documents/platform-v-pangolin-se", 2, 400),
        # host-ключ: без sitemap (иначе host-root seed тянет 89 продуктов).
        ("https://sbertech.ru/", 2, 400),
        # seed — глубокий путь, 8 уровней запаса.
        ("https://platformv.sbertech.ru/products/pangolin", 8, 300),
        # path-ключ внутри sitemap-хоста.
        ("https://tantorlabs.ru/products/dbms/", 8, 200),
        # host-ключ: 3 уровня on-host, off-host фиды не берём.
        ("https://tantorlabs.ru/", 3, 200),
        ("https://jatoba.ru/", 2, 50),
        # sitemap 404 → глубина как ориентир.
        ("https://docs.tantorlabs.ru/", 8, 300),
        ("https://wiki.astralinux.ru/", 8, 300),
    ],
)
def test_resolve_crawl_limits_per_site(url: str, max_depth: int, max_documents: int) -> None:
    """Резолв лимитов (app < run < site) даёт значения gate §3 для репрезентативных URL."""
    cfg = load_run_config(ON_DISK_RUN)
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    limits = resolve_crawl_limits(
        url, app=CrawlConfig(), run=cfg.crawl, profiles=profiles
    )

    assert limits.max_depth == max_depth
    assert limits.max_documents == max_documents
    # Прочие оси наследуются из run-слоя (site-слой их не переопределяет).
    assert limits.max_workers == RUN_CRAWL_MAX_WORKERS
    assert limits.extract_workers == RUN_CRAWL_EXTRACT_WORKERS
    assert limits.document_level is DocumentLevel.FULL
    assert list(limits.exclude_paths) == RUN_CRAWL_EXCLUDE_PATHS
    assert list(limits.exclude_documents) == RUN_CRAWL_EXCLUDE_DOCUMENTS


def test_www_gaz_is_matches_new_profile() -> None:
    """``www.gaz-is.ru`` матчится новым ключом ``gaz-is.ru`` (раньше — app-дефолты)."""
    profiles = load_domain_profiles(ON_DISK_PROFILES)
    profile = select_domain_profile("https://www.gaz-is.ru/", profiles)
    assert profile is not None, "www.gaz-is.ru обязан матчиться профилем gaz-is.ru"
    assert profile.fetch is FetchMode.HTTP
    assert profile.extract.value == "auto"
    assert profile.max_depth == 8
    assert profile.max_documents == 200
    assert profile.sitemap is None, "у gaz-is.ru sitemap нет (500 на /sitemap.xml)"


def test_sitemap_declared_only_on_most_specific_sbertech_key() -> None:
    """Sitemap sbertech.ru — только на path-ключе (ловушка ``_sitemap_url_for``).

    ``_sitemap_url_for(seed)`` берёт самый специфичный профиль: host-root seed матчит
    host-ключ, поэтому host-wide sitemap там затянул бы все продукты в партицию (I17).
    """
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    host_profile = select_domain_profile("https://sbertech.ru/", profiles)
    assert host_profile is not None
    assert host_profile.sitemap is None, "host-ключ sbertech.ru обязан быть без sitemap"

    path_profile = select_domain_profile("https://sbertech.ru/documents/anything", profiles)
    assert path_profile is not None
    assert path_profile.sitemap == "https://sbertech.ru/sitemap.xml"


if __name__ == "__main__":  # pragma: no cover - локальный запуск
    raise SystemExit(pytest.main([__file__, "-q"]))
