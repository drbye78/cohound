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
    "exclude_queries",
    "document_level",
    "browser_workers",
    "sitemap",
}

#: P7 gate §3: бюджеты ``run.yaml:crawl`` (run-слой, execution-дефолты).
RUN_CRAWL_MAX_DEPTH = 8
RUN_CRAWL_MAX_DOCUMENTS = 600
#: ``max_workers`` (сетевой пул) объявлен на **узле** — ``config/deploy.yaml:crawl``
#: (решение владельца 07.11.2026: это свойство машины, как ``host_concurrency`` и
#: ``pdf_device``, а не измерения). В пакете ось поэтому **отсутствует**, и пин ниже
#: держит именно это: значение, приехавшее в run-слой, перекрыло бы host-значение.
NODE_CRAWL_MAX_WORKERS = 10
NODE_CRAWL_HOST_CONCURRENCY = 5
#: Пул экстракции пересмотрен после замера `queue_max_size=1` (−49 % RSS: 4174 → 2126 MB
#: на 665-стр. документе при идентичном markdown) ⇒ 2 воркера помещаются в бюджет хоста.
#: Пин держит значение синхронным с `run.yaml` и генератором (иначе дрейф, P7).
#: На **run-слое** ось ``crawl.extract_workers`` не объявлена: число выводит формула из
#: бюджета памяти (F0/Д-7, ``workers_from_budget``). Единственное исключение — профиль
#: `gaz-is.ru` (``extract_workers: 1``): там пик уровня `full` — пик **популяции**
#: (4 833 MB на 2 763-стр. документе), а документы хоста ≤ 20 страниц, поэтому гейт
#: отказывал в старте, хотя выведенное окно всё равно 1 (см. пин ниже).
RUN_CRAWL_EXTRACT_WORKERS = None
RUN_CRAWL_EXTRACT_MAX_TASKS_PER_CHILD = 25
RUN_CRAWL_EXCLUDE_PATHS = [
    "/blog/*",
    "/news/*",
    "/events/*",
    "/cases/*",
    "/mediacenter/*",
]
RUN_CRAWL_EXCLUDE_DOCUMENTS = [
    "*.gz", "*.tar", "*.tgz", "*.exe", "*.msi", "*.dmg",
    "*.iso", "*.ova", "*.vmdk", "*.qcow2", "*.mp4", "*.avi", "*.css", "*.js",
    "*.png", "*.jpg", "*.jpeg", "*.gif", "*.svg", "*.webp", "*.ico", "*.woff",
    "*.woff2", "*.ttf",
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


def _assert_seeds_are_declared(
    seeds: dict[str, list[str]], passports: dict[str, object]
) -> None:
    """Seed-набор объявлен: либо вывод из паспорта, либо перечисление с названным источником.

    Пин остаётся тем же — «файл не живёт отдельно от паспортов», — но у части продуктов
    носитель объявления другой, и это надо назвать, а не ослабить проверку.

    * паспорт без ``seed_enumeration_source``: ``seeds[pid] == passport_urls(passport)``
      **байт-в-байт** (прежнее утверждение, без исключений);
    * паспорт с ``seed_enumeration_source``: ``doc_domains`` **не может** выразить набор —
      у GaussDB страница каталога не несёт текста (замер 22.11.2026: 77 578 байт разметки,
      ~1 900 символов, ``<div id="content">`` пуст), а вендор публикует машинный индекс на
      15 235 документов. Тогда проверяется то, что делает набор объявленным: источник
      назван, адреса уникальны и ``http(s)``, и **каждый** лежит внутри объявленной области
      ``doc_domains`` (иначе seed затянул бы в партицию чужой продукт — I17).
    """
    from urllib.parse import urlsplit

    from core.config.passports import passport_urls

    assert set(seeds) == set(passports), (
        f"продукты расходятся: только в файле {sorted(set(seeds) - set(passports))}, "
        f"только в паспортах {sorted(set(passports) - set(seeds))}"
    )
    for pid, passport in passports.items():
        declared = seeds[pid]
        source = getattr(passport, "seed_enumeration_source", None)
        if source is None:
            assert declared == passport_urls(passport), (
                f"{pid}: corpus_urls.yaml обязан совпадать с деривацией из паспорта"
            )
            continue
        assert source.startswith(("http://", "https://")), f"{pid}: источник не URL: {source}"
        assert declared, f"{pid}: перечисление объявлено, а адресов нет"
        assert len(set(declared)) == len(declared), f"{pid}: в перечислении есть дубликаты"
        areas: list[tuple[str, str]] = []
        for domain in passport.doc_domains:
            raw = str(domain).strip().strip("/")
            if not raw:
                continue
            host, _, prefix = raw.partition("/")
            areas.append((host, prefix))
        assert areas, f"{pid}: перечисление без объявленной области doc_domains"
        for url in declared:
            parts = urlsplit(url)
            assert parts.scheme in ("http", "https"), f"{pid}: {url} — не http(s)"
            covered = any(
                parts.netloc == host and (not prefix or parts.path.lstrip("/").startswith(prefix))
                for host, prefix in areas
            )
            assert covered, f"{pid}: {url} вне объявленной области doc_domains {areas}"


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
    for profile in loaded.values():
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
    assert crawl.rate_limit == RateLimitSpec(min_s=1.0, max_s=5.0)
    assert crawl.max_depth == RUN_CRAWL_MAX_DEPTH
    assert crawl.max_documents == RUN_CRAWL_MAX_DOCUMENTS
    # Ось ушла на узел (``config/deploy.yaml:crawl.max_workers``): пакет её не несёт,
    # иначе host-значение было бы перекрыто (app < run < site).
    assert crawl.max_workers is None
    # Ось не объявлена намеренно: значение выводится из бюджета, а не назначается
    # вручную — объявленная ось была бы сильнее формулы.
    assert crawl.extract_workers is RUN_CRAWL_EXTRACT_WORKERS
    assert (
        crawl.extract_max_tasks_per_child == RUN_CRAWL_EXTRACT_MAX_TASKS_PER_CHILD
    )
    assert crawl.document_level is DocumentLevel.LAYOUT_TABLES
    assert list(crawl.exclude_paths or []) == RUN_CRAWL_EXCLUDE_PATHS
    assert list(crawl.exclude_documents or []) == RUN_CRAWL_EXCLUDE_DOCUMENTS


@pytest.mark.parametrize(
    ("url", "max_depth", "max_documents", "document_level", "extract_workers"),
    [
        # Дерево объявленной редакции. ГРАНИЦА СУЖЕНА 24.11.2026 (решение владельца):
        # было `postgrespro.ru/docs` — каталог ВСЕХ продуктов вендора (9 наборов ×
        # 53 группы набор/версия × 3.91 языковых варианта = 166 145 адресов; обход
        # `ru-pg-dbms-e11e04cbb3d9` остановлен на 40 854 документах / 8.05 GB, из них
        # 91 % — версии 9.4–17 и Standard, которых паспорт не объявляет).
        # `max_documents` теперь **не объявлен**: размер дерева редакции не измерен ни
        # разу, а прежние 20 000 относились к корню `/docs/`. Потолок здесь был бы
        # выбором приоритета, а не заявкой на размер — исчерпание измеряется обходом.
        # Язык — ось `exclude_queries` (см. `test_postgrespro_docs_root_keeps_russian_only`).
        ("https://postgrespro.ru/docs/enterprise/18/", 8, 3000, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Host-ключ портала (и страницы продукта) сохраняет прежний бюджет: path-ключ
        # `/docs` не должен протекать на соседние корни того же хоста. Проба 10.11.2026
        # взяла 3 996 документов и была остановлена хостом (980 × `429` на `/list/*`):
        # 800 — выбор приоритета, не измеренный размер.
        ("https://postgrespro.ru/", 8, 800, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Ключ обслуживает четыре поддерева портала (с 26.11.2026 host-корня нет, см.
        # `test_pangolin_scope_is_announced_edition_only`). 3 500 — исторический бюджет
        # host-корня: статическая нижняя граница 3 025 (`/center/docs/7` 2 307 + kintsugi
        # 227 + copywala 491), выше любого из четырёх поддеревьев, поэтому ничего не
        # усекает; точные пределы — `hound crawl drain --apply` после исчерпания.
        ("https://pangolin.sbertech.ru/", 8, 3500, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Host-root мульти-продуктового сайта: discovery запрещён (I17) — только корень.
        # Окно извлечения объявлено (1) — единственный ключ, где ось стоит на профиле:
        # `full` и его пик 4 833 MB (замер на 2 763-стр. документе) не влезают в бюджет
        # хоста, хотя выведенное окно всё равно 1, а документы этого хоста ≤ 20 страниц.
        ("https://www.gaz-is.ru/", 0, 10, DocumentLevel.FULL, 1),
        # Продуктовый путь jatoba: измерено 1 HTML в scope + 17 отказов `robots_disallow`
        # (`Disallow: *.pdf$` закрывает документы продукта) ⇒ 50 выше измерения.
        # Уровень — прогонный: site-слой резолвится **одним** наиболее специфичным
        # ключом, а не слиянием с host-ключом, поэтому `full` и `extract_workers` хоста
        # сюда не протекают.
        ("https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/", 8, 50, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # path-ключ приоритетнее host-ключа (sitemap только здесь); в scope seed'а 27 URL
        # pangolin (замер 09.11.2026 по `sbertech.ru/sitemap.xml`), 100 — запас.
        ("https://sbertech.ru/documents/platform-v-pangolin-se", 4, 100, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # host-ключ: без sitemap и без discovery (иначе BFS тянет 89 продуктов, I17).
        ("https://sbertech.ru/", 0, 10, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # seed — глубокий путь, 8 уровней запаса.
        ("https://platformv.sbertech.ru/products/pangolin", 8, 300, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # path-ключ внутри sitemap-хоста; измерено 3 документа, `truncated: false`.
        ("https://tantorlabs.ru/products/dbms/", 8, 50, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # host-ключ: проба 10.11.2026 взяла 9 162 документа (прервана таймаутом); 300 —
        # граница I17 (host-корень затягивает чужие продукты), а не размер сайта.
        ("https://tantorlabs.ru/", 3, 300, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Измерено 7 документов, `truncated: false` ⇒ 50 покрывает сайт.
        ("https://jatoba.ru/", 2, 50, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # sitemap 404. Размер ИЗМЕРЕН 12.11.2026: 1 207 документов, `truncated: false`
        # (одна редакция `ru/18_6/se`; плоские `/tdb/*.html` — 301-алиасы).
        ("https://docs.tantorlabs.ru/", 8, 1500, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Host-root общего справочного центра Astra — не продуктовый корень (I17).
        ("https://wiki.astralinux.ru/", 0, 10, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Тантор-контент — Confluence-пространство TANDOCS. Измерено статически 73 страницы
        # (13.11.2026); 403 прогона 07.11 — свойство Scrapy User-Agent, не закрытый корень.
        ("https://wiki.astralinux.ru/tandocs/baza-znanij-tantor-238752972.html", 8, 150, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
        # Предок линии: sitemap отдаёт 1 153 `<loc>` под `/docs/18/` (замер 21.11.2026),
        # reference-сборка — 1 136 документов; 2 000 — выше измерения. Глубина 2: документы
        # приходят depth-0 seeds, страницы `/docs/18/` плоские. Другой мажор (`/docs/11/`)
        # ключом не покрыт намеренно (I17).
        ("https://www.postgresql.org/docs/18/", 2, 2000, DocumentLevel.LAYOUT_TABLES, RUN_CRAWL_EXTRACT_WORKERS),
    ],
)
def test_resolve_crawl_limits_per_site(
    url: str,
    max_depth: int,
    max_documents: int,
    document_level: DocumentLevel,
    extract_workers: int | None,
) -> None:
    """Резолв лимитов (app < run < site) даёт значения gate §3 для репрезентативных URL.

    ``document_level`` — четвёртый пин: ось объявлена и **на профиле** (`gaz-is.ru: full`,
    потому что документы продукта — сканы, закрытые robots, и попадают в корпус ручной
    подачей). Остальные ключи остаются на оси прогона: пин держит оба конца, иначе
    «профиль сильнее» молча стало бы «профиль равен прогону».

    ``extract_workers`` — пятый пин, и он держит **оба** состояния оси: ``None`` (окно
    выводится из бюджета памяти) везде, кроме `gaz-is.ru`, где окно объявлено явно и
    поэтому перестаёт быть выведенным. Проверяется и значение, и флаг
    ``extract_workers_derived`` — иначе «объявили» и «вывели» снова стали бы неразличимы.
    """
    cfg = load_run_config(ON_DISK_RUN)
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    limits = resolve_crawl_limits(
        url, app=CrawlConfig(), run=cfg.crawl, profiles=profiles
    )

    assert limits.max_depth == max_depth
    assert limits.max_documents == max_documents
    # ``max_workers``/``host_concurrency`` пакет больше не объявляет: они на узле
    # (``config/deploy.yaml:crawl``). Пустой app-слой ⇒ оси не заданы ни на одном слое.
    assert limits.max_workers is None
    assert limits.host_concurrency is None
    assert limits.extract_workers == extract_workers
    assert limits.document_level is document_level
    assert list(limits.exclude_paths) == RUN_CRAWL_EXCLUDE_PATHS
    assert list(limits.exclude_documents) == RUN_CRAWL_EXCLUDE_DOCUMENTS

def test_postgrespro_docs_root_keeps_russian_only() -> None:
    """Корень документации объявляет русский язык осью `exclude_queries` (site-слой).

    Класс дефекта: сайт отдаёт каждую страницу тремя **отдельными** адресами
    (`?lang=ru`, `?lang=en`, `?lang=ru-en`), `content_hash` у них разный у 99.9 % пар —
    это не редирект и не дубль байтов, а множитель ×3.91 к корпусу. `exclude_paths`
    query-строку не видит (матчит только путь), поэтому нужна отдельная ось.
    Измерено 24.11.2026 на обходе `ru-pg-dbms-e11e04cbb3d9`: 166 145 адресов при
    42 517 канонических страницах.
    """
    profiles = load_domain_profiles(ON_DISK_PROFILES)
    cfg = load_run_config(ON_DISK_RUN)

    docs = resolve_crawl_limits(
        "https://postgrespro.ru/docs/enterprise/18/sql-vacuum",
        app=CrawlConfig(),
        run=cfg.crawl,
        profiles=profiles,
    )
    assert list(docs.exclude_queries) == ["lang=en", "lang=ru-en"]

    # Та же ось у деревьев сателлитов: их корпус тоже русский.
    for url in (
        "https://postgrespro.ru/docs/ppem/2/agent-config-ref",
        "https://postgrespro.ru/docs/pg_probackup3/3.5/pbk-setting-up-continuous-wal-archiving",
        "https://postgrespro.ru/docs/ora2pgpro/26/introduction",
    ):
        satellite = resolve_crawl_limits(
            url, app=CrawlConfig(), run=cfg.crawl, profiles=profiles
        )
        assert list(satellite.exclude_queries) == ["lang=en", "lang=ru-en"], url

    # Ось не протекает на соседние корни того же хоста: там объявлено пусто.
    portal = resolve_crawl_limits(
        "https://postgrespro.ru/products/postgrespro/enterprise/",
        app=CrawlConfig(),
        run=cfg.crawl,
        profiles=profiles,
    )
    assert list(portal.exclude_queries) == []


def test_pangolin_scope_is_announced_edition_only() -> None:
    """Корпус Pangolin = дерево объявленной редакции + объявленные сателлиты.

    Класс дефекта (тот же, что у postgrespro): корнем был весь портал
    ``pangolin.sbertech.ru``, и в корпус попадали ТРИ редакции сразу — ``center/docs/7``
    (текст 7.3.2, 321 документ), ``center/docs/8`` (8.3.0, 239) и ``PSQ/8.1.0``
    (1 154), плюс блог, ``cve-policy`` и сертификаты. Измерено 26.11.2026 по
    ``state.db`` стенда: **5 053** адреса под ``/center/docs/``. Свидетельство о версии,
    которой нет в паспорте, не подтверждает утверждение об объявленной редакции
    (precision-first) и размывает бюджет корня.

    Пин держит четыре вещи, каждая из которых — лазейка, если её убрать:
    1. состав корней (дерево редакции, база знаний, три сателлита — и ничего больше);
    2. редакцию в корне дерева (``PSQ/8.3.0``, а не префикс ``PSQ`` — он накрыл бы все
       18 опубликованных версий);
    3. оси: дерево PSQ отдаёт 200 обычному HTTP, портал требует browser+honest;
    4. отсутствие версий 5/6/7 и маркетинга в seed-наборе.
    """
    from core.config.passports import load_passports, passport_urls
    from core.pipeline.fetch_plan import resolve_fetch_plan

    seeds = yaml.safe_load(CORPUS_URLS.read_text(encoding="utf-8"))
    passport = load_passports(ROOT / "passports")["pangolin"]
    profiles = load_domain_profiles(ON_DISK_PROFILES)
    cfg = load_run_config(ON_DISK_RUN)

    # Порядок — деривация `passport_urls`: `doc_domains` продукта, затем корни
    # сателлитов в порядке списка `satellites` (Kintsugi — management_console,
    # CopyWala — backup). Корни сателлитов объявлены **у сателлитов**, а не у продукта:
    # это их материал, и объявлять его документацией продукта было бы неверно.
    expected = [
        "https://platformv.sbertech.ru/docs/public/PSQ/8.3.0/",
        "https://pangolin.sbertech.ru/center/knowledge-base/",
        "https://pangolin.sbertech.ru/center/docs/gradely/",
        "https://pangolin.sbertech.ru/center/docs/kintsugi/",
        "https://pangolin.sbertech.ru/center/docs/copywala/",
    ]
    # 1. Файл — деривация паспорта, и набор именно такой.
    assert seeds["pangolin"] == expected
    assert passport_urls(passport) == expected, (
        "corpus_urls — деривация doc_domains паспорта; правка одной стороны без другой "
        "разъедется при регенерации"
    )
    assert "Pangolin 8.3" in passport.version

    # 2–4. Ни одной версии вне объявленной, ни блога, ни маркетинга.
    for url in expected:
        assert "/center/docs/5" not in url and "/center/docs/6" not in url, url
        assert "/center/docs/7" not in url, url
        assert "/center/docs/8" not in url, url
        assert "cve-policy" not in url and "/blog" not in url, url
        assert "rabota-s-dannymi" not in url, url
    assert "https://pangolin.sbertech.ru/" not in seeds["pangolin"]

    # 3. Оси: дерево PSQ — http (замер 26.11.2026: 200 обычному HTTP), портал — browser.
    psq = resolve_fetch_plan(expected[0], profiles)
    assert psq.fetch is FetchMode.HTTP
    for url in expected[1:]:
        portal = resolve_fetch_plan(url, profiles)
        assert portal.fetch is FetchMode.BROWSER, url
        assert portal.strategy is CrawlStrategy.HONEST, url

    # Бюджет: ни одно из поддеревьев не усекается известным размером из `state.db`
    # (knowledge-base 135, copywala 344, kintsugi 122, gradely 186; PSQ/8.3.0 не измерен,
    # унаследован 1 500 от 8.1.0 = 1 155 + 30 %).
    known = {"knowledge-base": 135, "copywala": 344, "kintsugi": 122, "gradely": 186}
    for url in expected[1:]:
        limits = resolve_crawl_limits(url, app=CrawlConfig(), run=cfg.crawl, profiles=profiles)
        subtree = url.rstrip("/").rsplit("/", 1)[-1]
        assert limits.max_documents > known[subtree], (url, limits.max_documents)


def test_declared_extract_workers_wins_over_derivation() -> None:
    """Объявленная на профиле ось `extract_workers` сильнее формулы из бюджета памяти.

    Пин держит **различие двух состояний**, а не число: там, где ось объявлена
    (`gaz-is.ru`, уровень `full` с пиком популяции 4 833 MB), окно не выводится
    (`extract_workers_derived is False`) и равно объявленному; на соседнем path-ключе
    того же хоста ось не объявлена и окно выводится. Без этого пина «профиль объявил»
    и «формула вывела» снова стали бы неразличимы, а preflight перестал бы быть гейтом.
    """
    cfg = load_run_config(ON_DISK_RUN)
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    declared = resolve_crawl_limits(
        "https://www.gaz-is.ru/",
        app=CrawlConfig(),
        run=cfg.crawl,
        profiles=profiles,
        derive_extract_workers=True,
        mem_budget_mb=16000,
    )
    derived = resolve_crawl_limits(
        "https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/",
        app=CrawlConfig(),
        run=cfg.crawl,
        profiles=profiles,
        derive_extract_workers=True,
        mem_budget_mb=16000,
    )

    assert declared.extract_workers == 1
    assert declared.extract_workers_derived is False
    assert derived.extract_workers_derived is True
    assert derived.extract_workers is not None and derived.extract_workers >= 1


def test_node_layer_declares_crawl_width() -> None:
    """Ширина краула разрешается из **узла**, и ни run-, ни site-слой её не перекрывают.

    Решение владельца 07.11.2026: ``max_workers`` — свойство машины, ``host_concurrency`` —
    свойство узла. Пин держит оба конца цепочки: объявленное на узле значение доезжает до
    ``CrawlLimits``, а пакет его не перебивает (иначе правка ``run.yaml`` вернула бы
    «число из пакета» и замер темпа показывал бы чужую ширину пула).
    """
    cfg = load_run_config(ON_DISK_RUN)
    profiles = load_domain_profiles(ON_DISK_PROFILES)
    limits = resolve_crawl_limits(
        "https://docs.tantorlabs.ru/",
        app=CrawlConfig(
            max_workers=NODE_CRAWL_MAX_WORKERS,
            host_concurrency=NODE_CRAWL_HOST_CONCURRENCY,
        ),
        run=cfg.crawl,
        profiles=profiles,
    )
    assert limits.max_workers == NODE_CRAWL_MAX_WORKERS
    assert limits.host_concurrency == NODE_CRAWL_HOST_CONCURRENCY
    # Обе оси входят в отпечаток конфигурации краула: смена ширины — новый ``run_id``.
    payload = limits.as_dict()
    assert payload["max_workers"] == NODE_CRAWL_MAX_WORKERS
    assert payload["host_concurrency"] == NODE_CRAWL_HOST_CONCURRENCY


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
    assert product_profile.max_documents == 50


def test_corpus_urls_jatoba_seed_is_live_product_page() -> None:
    """Регенерированный seed jatoba — живая продуктовая страница, а не 404.

    Живая проверка (2026-10): ``www.gaz-is.ru/products/jatoba`` — 404 (http↔https
    301-пинг-понг → 404); реальная страница — ``/produkty/inform-sistemy/subd-jatoba``
    (200; robots.txt ссылается на старый слаг ``subd-jotoba`` → 302 на ``subd-jatoba``).
    Пин: файл остаётся детерминированной деривацией паспортов (``passport_urls``), т.е.
    правка паспорта и регенерация не разъедутся.
    """
    from core.config.passports import load_passports

    seeds = yaml.safe_load(CORPUS_URLS.read_text(encoding="utf-8"))
    assert "https://www.gaz-is.ru/products/jatoba/" not in seeds["jatoba"]
    assert "https://www.gaz-is.ru/produkty/inform-sistemy/subd-jatoba/" in seeds["jatoba"]
    _assert_seeds_are_declared(seeds, load_passports(ROOT / "passports"))


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
    assert space_profile.max_documents == 150

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
    assert path_profile.max_depth == 4
    assert path_profile.max_documents == 100


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

def test_postgrespro_seed_order_is_authored_docs_first_portal_last() -> None:
    """Порядок seed-корней postgrespro — **авторский** (execution_change §13).

    Корни дренятся в этом порядке, и каждый — свой бюджет. ``docs`` первым (высшая
    ценность для карты возможностей), портал последним (вспомогательное и объёмное).
    Пин ловит и ре-сортировку файла, и расхождение с деривацией паспорта: ``sorted()``
    снова поставил бы ``postgrespro.ru/`` (портал) перед ``postgrespro.ru/docs/`` —
    ровно дефект живого прогона 22.10.

    Список — из шести корней: учебный ``edu`` выведен 14.11.2026 (решение владельца,
    см. ``test_edu_subdomain_is_excluded_from_corpus_and_scope``); деревья сателлитов
    (``ppem``, ``pg_probackup3``, ``ora2pgpro``) добавлены 24.11.2026 вместе с сужением
    корня документации до объявленной редакции.
    """
    from core.config.passports import load_passports

    seeds = yaml.safe_load(CORPUS_URLS.read_text(encoding="utf-8"))
    assert seeds["postgrespro"] == [
        "https://postgrespro.ru/docs/enterprise/18/",
        "https://postgrespro.ru/docs/ppem/2/",
        "https://postgrespro.ru/docs/pg_probackup3/3.5/",
        "https://postgrespro.ru/docs/ora2pgpro/26/",
        "https://postgrespro.ru/products/postgrespro/enterprise/",
        "https://postgrespro.ru/",
    ]
    assert seeds["postgrespro"] != sorted(seeds["postgrespro"]), (
        "пин: порядок авторский — sorted() поставил бы портал перед docs"
    )
    _assert_seeds_are_declared(seeds, load_passports(ROOT / "passports"))


def test_edu_subdomain_is_excluded_from_corpus_and_scope() -> None:
    """``edu.postgrespro.ru`` выведен из корпуса и из scope — пин исключения, не удаление.

    История: до 22.10 хост не был объявлен нигде и попадал в партицию как «любой поддомен»
    ``postgrespro.ru``, унаследовав его профиль (``max_documents: 800``): 664 элемента
    бюджета корня ушли на учебные раздатки, а docs-корень не начался. Тогда edu стал
    отдельным корнем со своим потолком. **14.11.2026** владелец вывел его из корпуса:
    2 939 документов / 87 803 чанка (34,7 % чанков прогона ``ru-pg-dbms-145ad66ca3f9``)
    при **0** ячеек, опирающихся на его свидетельство, — во всех прогонах стенда, включая
    два полных (905 ячеек: ``2a5b51af5778``, ``c2e7b860ac17``). То есть исключение не
    отзывает ни одной ячейки и не требует пере-разметки эталона.

    Пин держит **обе** половины исключения, потому что половина = лазейка: без seed-корня
    edu вернулся бы discovery/sitemap, а ключ ``domain_profiles.yaml`` объявляет хост в
    scope (``Frontier.host_in_scope``) сам по себе. Последняя проверка — поведенческая:
    на объявленных корнях продукта edu вне scope, то есть исключение не косметическое.
    """
    from urllib.parse import urlsplit

    from core.config.passports import load_passports, passport_urls
    from core.pipeline.crawl_frontier import CrawlLimits, Frontier, normalize_scope_host

    seeds = yaml.safe_load(CORPUS_URLS.read_text(encoding="utf-8"))
    passport = load_passports(ROOT / "passports")["postgrespro"]
    profiles = load_domain_profiles(ON_DISK_PROFILES)

    # 1. Нет seed-корня.
    assert "https://edu.postgrespro.ru/" not in seeds["postgrespro"]
    # 2. Нет в паспорте — иначе регенерация `corpus_urls.yaml` вернёт корень.
    assert "https://edu.postgrespro.ru/" not in passport_urls(passport), (
        "edu обязан отсутствовать в doc_domains паспорта: corpus_urls — его деривация"
    )
    # 3. Нет ключа профиля — иначе хост остаётся объявленным в scope без всякого seed.
    assert "edu.postgrespro.ru" not in profiles

    # 4. Поведенческий пин: scope корней продукта считается **как в фазе corpus**
    # (`core/pipeline/corpus.py`: объявленные хосты = seed-хосты + ключи профилей,
    # минус собственные хосты продукта). Обход учебной страницы из корня портала
    # отвергается — и через `host_in_scope`, и через `in_scope` на самом URL.
    declared_hosts = {
        normalize_scope_host(urlsplit(str(url)).hostname or "")
        for product_urls in seeds.values()
        for url in product_urls
    } | {
        normalize_scope_host(str(key).split("/", 1)[0]) for key in profiles
    }
    own_hosts = {
        normalize_scope_host(urlsplit(str(url)).hostname or "")
        for url in seeds["postgrespro"]
    }
    frontier = Frontier(
        "https://postgrespro.ru/",
        CrawlLimits(max_depth=3),
        scope_hosts=declared_hosts - own_hosts,
    )
    assert frontier.host_in_scope("edu.postgrespro.ru") is False, (
        "edu объявлен в scope: seed-корень или ключ профиля вернулся"
    )
    assert (
        frontier.in_scope("https://edu.postgrespro.ru/dba2_06_mvcc_autovacuum.html") is False
    )

