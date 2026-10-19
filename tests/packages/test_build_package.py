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

F1/F10 (I17): host-корни мульти-продуктовых сайтов (``sbertech.ru``, ``gaz-is.ru``,
``wiki.astralinux.ru``) не discover'ят (``max_depth == 0``) и не объявляют host-wide
sitemap; обход — на path-ключе продукта. Sitemap снят с path-ключей, где он почти no-op
(``platformv.sbertech.ru``, ``tantorlabs.ru/products/dbms``). Инвариант закреплён
отдельным тестом ``test_i17_multi_product_host_root_keys_never_discover`` (и на on-disk
конфиге, и на выходе генератора), чтобы регрессия не вернулась молча.
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
CORPUS_URLS = ROOT / "packages" / "ru-pg-dbms" / "sources" / "corpus_urls.yaml"

#: Инвариант I17: host-корни мульти-продуктовых сайтов не discover'ят (max_depth 0)
#: и не объявляют host-wide sitemap. Обход живёт на path-ключе продукта.
MULTI_PRODUCT_HOST_ROOTS = ("sbertech.ru", "gaz-is.ru", "wiki.astralinux.ru")

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
RUN_CRAWL_EXTRACT_WORKERS = 1
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
    assert crawl.document_level is DocumentLevel.LAYOUT_TABLES
    assert list(crawl.exclude_paths or []) == RUN_CRAWL_EXCLUDE_PATHS
    assert list(crawl.exclude_documents or []) == RUN_CRAWL_EXCLUDE_DOCUMENTS


@pytest.mark.parametrize(
    ("url", "max_depth", "max_documents"),
    [
        # seed-корень портала /docs без sitemap — бюджет-потолок для крупного docs-портала.
        ("https://postgrespro.ru/docs/16/", 8, 800),
        # 23 URL, плоско — 60 покрывает с запасом.
        ("https://pangolin.sbertech.ru/", 2, 60),
        # Host-root мульти-продуктового сайта: discovery запрещён (I17) — только корень.
        ("https://www.gaz-is.ru/", 0, 10),
        # Продуктовый путь jatoba (реальная раскладка сайта — /produkty/...) — обход уместен.
        ("https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/", 8, 200),
        # path-ключ приоритетнее host-ключа (sitemap только здесь); в scope seed'а ~22 URL
        # pangolin (502 чужих документа отсекает scope-фильтр), 60 — запас.
        ("https://sbertech.ru/documents/platform-v-pangolin-se", 2, 60),
        # host-ключ: без sitemap и без discovery (иначе BFS тянет 89 продуктов, I17).
        ("https://sbertech.ru/", 0, 10),
        # seed — глубокий путь, 8 уровней запаса.
        ("https://platformv.sbertech.ru/products/pangolin", 8, 300),
        # path-ключ внутри sitemap-хоста; весь хост — 283 URL.
        ("https://tantorlabs.ru/products/dbms/", 8, 200),
        # host-ключ: 3 уровня on-host, 283 URL ⇒ 300 с запасом.
        ("https://tantorlabs.ru/", 3, 300),
        # 2 URL (1 PDF) ⇒ 50 покрывает сайт.
        ("https://jatoba.ru/", 2, 50),
        # sitemap 404, крупный docs-хост ⇒ 800 — потолок против усечения.
        ("https://docs.tantorlabs.ru/", 8, 800),
        # Host-root общего справочного центра Astra — не продуктовый корень (I17).
        ("https://wiki.astralinux.ru/", 0, 10),
        # Тантор-контент — Confluence-пространство TANDOCS (стабильный префикс /tandocs).
        ("https://wiki.astralinux.ru/tandocs/baza-znanij-tantor-238752972.html", 8, 300),
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
    assert limits.document_level is DocumentLevel.LAYOUT_TABLES
    assert list(limits.exclude_paths) == RUN_CRAWL_EXCLUDE_PATHS
    assert list(limits.exclude_documents) == RUN_CRAWL_EXCLUDE_DOCUMENTS


def test_www_gaz_is_host_root_does_not_discover() -> None:
    """``www.gaz-is.ru`` матчится ключом ``gaz-is.ru``, но host-корень не обходится (I17).

    Корень — страница компании (у вендора десятки продуктов), поэтому ``max_depth == 0``:
    только сам URL. Обход живёт на path-ключе продукта jatoba (реальная раскладка сайта —
    ``/produkty/inform-sistemy/subd-jatoba``, robots.txt; seed ``/products/jatoba/`` — 404).
    """
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    host_profile = select_domain_profile("https://www.gaz-is.ru/", profiles)
    assert host_profile is not None, "www.gaz-is.ru обязан матчиться профилем gaz-is.ru"
    assert host_profile.fetch is FetchMode.HTTP
    assert host_profile.extract.value == "auto"
    assert host_profile.max_depth == 0, "host-корень не должен discover'ить (I17)"
    assert host_profile.max_documents == 10
    assert host_profile.sitemap is None, "у gaz-is.ru sitemap нет (500 на /sitemap.xml)"

    product_profile = select_domain_profile(
        "https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/", profiles
    )
    assert product_profile is not None, "продуктовый path-ключ jatoba обязан матчиться"
    assert product_profile.fetch is FetchMode.HTTP
    assert product_profile.max_depth == 8
    assert product_profile.max_documents == 200


def test_corpus_urls_jatoba_seed_is_live_product_page() -> None:
    """Регенерированный seed jatoba — живая продуктовая страница, а не 404.

    Живая проверка (2026-10): ``www.gaz-is.ru/products/jatoba`` — 404 (http↔https
    301-пинг-понг → 404); реальная страница — ``/produkty/inform-sistemy/subd-jatoba``
    (200; robots.txt ссылается на старый слаг ``subd-jotoba`` → 302 на ``subd-jatoba``).
    Пин: файл остаётся детерминированной деривацией паспортов (``passport_urls``), т.е.
    правка паспорта и регенерация не разъедутся.
    """
    from core.config.passports import load_passports, passport_urls

    seeds = yaml.safe_load(CORPUS_URLS.read_text(encoding="utf-8"))
    assert "https://www.gaz-is.ru/products/jatoba/" not in seeds["jatoba"]
    assert "https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/" in seeds["jatoba"]
    derived = {
        pid: passport_urls(passport)
        for pid, passport in load_passports(ROOT / "passports").items()
    }
    assert seeds == derived, "corpus_urls.yaml обязан совпадать с деривацией из паспортов"


def test_wiki_astralinux_host_root_does_not_discover_tandocs_is_scoped() -> None:
    """``wiki.astralinux.ru`` — общий справочный центр Astra: host-корень не обходит.

    Тантор-контент живёт в Confluence-пространстве TANDOCS («База Знаний Tantor»),
    публичные URL — ``/tandocs/<slug>-<id>.html`` (замер 2026-10: 68 уникальных
    ``/tandocs``-ссылок с домашней страницы пространства; ``/display/TANDOCS`` 302 →
    ``/tandocs/baza-znanij-tantor-238752972.html``). Префикс профиля — ``/tandocs``:
    path-матч литеральный, канонический ``/display/TANDOCS`` после 302 вне scope.
    """
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    host_profile = select_domain_profile("https://wiki.astralinux.ru/", profiles)
    assert host_profile is not None
    assert host_profile.max_depth == 0, "host-корень wiki не должен discover'ить (I17)"
    assert host_profile.max_documents == 10

    space_profile = select_domain_profile(
        "https://wiki.astralinux.ru/tandocs/baza-znanij-tantor-238752972.html", profiles
    )
    assert space_profile is not None
    assert space_profile is not host_profile
    assert space_profile.max_depth == 8
    assert space_profile.max_documents == 300

    # Канонический Confluence-путь пространства не попадает под /tandocs-префикс.
    assert select_domain_profile("https://wiki.astralinux.ru/display/TANDOCS", profiles) is (
        host_profile
    )


def test_sitemap_declared_only_on_most_specific_sbertech_key() -> None:
    """Sitemap sbertech.ru — только на path-ключе (ловушка ``_sitemap_url_for``).

    ``_sitemap_url_for(seed)`` берёт самый специфичный профиль: host-root seed матчит
    host-ключ, поэтому host-wide sitemap там затянул бы все продукты в партицию (I17).
    Host-ключ дополнительно без discovery: BFS с depth 2 доходит до
    ``/documents/{slug}/{version}`` — 89 продуктов (та же утечка I17, BFS-путём).
    """
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    host_profile = select_domain_profile("https://sbertech.ru/", profiles)
    assert host_profile is not None
    assert host_profile.sitemap is None, "host-ключ sbertech.ru обязан быть без sitemap"
    assert host_profile.max_depth == 0, "host-корень sbertech.ru не должен discover'ить"
    assert host_profile.max_documents == 10

    path_profile = select_domain_profile("https://sbertech.ru/documents/anything", profiles)
    assert path_profile is not None
    assert path_profile.sitemap == "https://sbertech.ru/sitemap.xml"
    assert path_profile.max_depth == 2
    assert path_profile.max_documents == 60


@pytest.mark.parametrize("host", MULTI_PRODUCT_HOST_ROOTS)
def test_i17_multi_product_host_root_keys_never_discover(host: str) -> None:
    """Инвариант I17: host-корень мульти-продуктового сайта не обходит и без sitemap.

    Регрессия возвращалась дважды: сначала через host-wide sitemap, затем через BFS
    (``sbertech.ru`` при ``max_depth: 2`` доходил до ``/documents/{slug}/{version}`` —
    501 URL на 89 продуктов). Тест держит инвариант одновременно на on-disk конфиге и
    на выходе генератора, поэтому ни ручная правка, ни дрейф генератора не пройдут молча.
    """
    module = _load_generator()
    generated = {
        key: DomainProfile.model_validate(raw)
        for key, raw in module.build_domain_profiles().items()
    }
    on_disk = load_domain_profiles(ON_DISK_PROFILES)

    for source, profiles in (("generator", generated), ("on-disk", on_disk)):
        assert host in profiles, f"{source}: host-ключ {host} пропал"
        profile = profiles[host]
        assert profile.max_depth == 0, (
            f"{source}: host-корень {host} не должен discover'ить (I17): "
            f"max_depth={profile.max_depth}"
        )
        assert profile.sitemap is None, (
            f"{source}: host-wide sitemap на {host} затянет соседние продукты (I17)"
        )


def test_noop_path_sitemaps_dropped_host_sitemaps_kept() -> None:
    """Path-scoped seed внутри host-wide sitemap — почти no-op: sitemap там не объявляем.

    Замеры: ``platformv.sbertech.ru`` — 1 <loc> из 801 в scope, ``tantorlabs.ru/products/dbms``
    — 3 из 58. Каждый такой sitemap даёт запись об отказе на каждом прогоне, не добавляя
    покрытия; host-ключи свои sitemap сохраняют.
    """
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    platformv = select_domain_profile(
        "https://platformv.sbertech.ru/products/rabota-s-dannymi/pangolin/", profiles
    )
    assert platformv is not None
    assert platformv.sitemap is None, "path-scoped seed внутри host-wide sitemap — no-op"

    tantor_path = select_domain_profile("https://tantorlabs.ru/products/dbms/", profiles)
    assert tantor_path is not None
    assert tantor_path.sitemap is None

    tantor_host = select_domain_profile("https://tantorlabs.ru/", profiles)
    assert tantor_host is not None
    assert tantor_host.sitemap == "https://tantorlabs.ru/sitemap.xml"

    # Остальные объявленные sitemap не тронуты.
    for url, expected in (
        ("https://jatoba.ru/", "https://jatoba.ru/sitemap.xml"),
        ("https://pangolin.sbertech.ru/", "https://pangolin.sbertech.ru/sitemap.xml"),
        ("https://sbertech.ru/documents/anything", "https://sbertech.ru/sitemap.xml"),
    ):
        profile = select_domain_profile(url, profiles)
        assert profile is not None
        assert profile.sitemap == expected, url


if __name__ == "__main__":  # pragma: no cover - локальный запуск
    raise SystemExit(pytest.main([__file__, "-q"]))
