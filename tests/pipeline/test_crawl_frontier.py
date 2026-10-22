"""Тесты резолвера осей контроля краула и BFS-frontier (D1/D2, ``crawl_frontier``).

Чистые, без сети. Проверяются: трёхуровневая цепочка app < run < site, ``extra="forbid"``
профиля домена, блэклисты (путь/basename, case-sensitive), scope по seed-root (host + граница
пути) и усечение по глубине.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain.models import CrawlRunConfig
from core.pipeline.crawl_frontier import (
    CrawlLimits,
    Frontier,
    blacklist_match,
    is_blacklisted,
    resolve_crawl_limits,
    root_path_prefix,
)

APP = CrawlConfig()
URL = "https://example.com/docs/a"


# --------------------------------------------------------------------------- #
# (g) precedence app < run < site
# --------------------------------------------------------------------------- #


def test_defaults_are_zero_depth_no_limits() -> None:
    limits = resolve_crawl_limits(URL, app=APP)
    assert limits.max_depth == 0
    assert limits.max_documents is None
    assert limits.exclude_paths == () and limits.exclude_documents == ()

# --------------------------------------------------------------------------- #
# (g2) robots: условие измерения, а не рычаг пропускной способности (D-5)
# --------------------------------------------------------------------------- #

def test_robots_is_unconditional_in_as_dict() -> None:
    """``robots`` входит в дайджест **всегда** (как ``max_depth``), даже при дефолте.

    Гейт ``robots.txt`` меняет состав корпуса, поэтому прогон с ним и без него обязан
    иметь разный ``crawl_config_digest``: иначе один ``run_id`` описывал бы два корпуса.
    """
    payload = resolve_crawl_limits(URL, app=APP).as_dict()
    assert "robots" in payload
    assert payload["robots"] is False
    assert CrawlLimits().as_dict()["robots"] is False
    assert CrawlLimits(robots=True).as_dict()["robots"] is True

def test_robots_explicit_app_layer_wins_over_autodetect() -> None:
    """Явный ``crawl.robots`` переопределяет автодетект (в т.ч. выключает его)."""
    off_with_depth = resolve_crawl_limits(
        URL, app=CrawlConfig(robots=False), run=CrawlRunConfig(max_depth=3)
    )
    assert off_with_depth.max_depth == 3
    assert off_with_depth.robots is False
    on_without_depth = resolve_crawl_limits(URL, app=CrawlConfig(robots=True))
    assert on_without_depth.max_depth == 0
    assert on_without_depth.robots is True

def test_robots_autodetect_mirrors_discovery() -> None:
    """``None`` = автодетект: обход ссылок либо per-site ``sitemap`` включают robots."""
    assert resolve_crawl_limits(URL, app=APP).robots is False
    assert (
        resolve_crawl_limits(URL, app=APP, run=CrawlRunConfig(max_depth=1)).robots is True
    )
    sitemap_profiles = {"example.com": DomainProfile(sitemap="https://example.com/s.xml")}
    assert (
        resolve_crawl_limits(URL, app=APP, profiles=sitemap_profiles).robots is True
    )

def test_robots_changes_crawl_digest() -> None:
    """Смена ``robots`` меняет ``crawl_config_digest`` (execution_change, §13)."""
    from core.pipeline.report import crawl_control_digest

    off = crawl_control_digest(
        run_crawl=None, app=CrawlConfig(robots=False), profiles=None
    )
    on = crawl_control_digest(
        run_crawl=None, app=CrawlConfig(robots=True), profiles=None
    )
    assert off != on


def test_run_layer_overrides_app() -> None:
    run = CrawlRunConfig(
        max_depth=2,
        max_documents=5,
        exclude_paths=["/tmp/*"],
        exclude_documents=["*.pdf"],
    )
    limits = resolve_crawl_limits(URL, app=APP, run=run)
    assert limits.max_depth == 2
    assert limits.max_documents == 5
    assert limits.exclude_paths == ("/tmp/*",)
    assert limits.exclude_documents == ("*.pdf",)


def test_site_layer_overrides_run() -> None:
    run = CrawlRunConfig(max_depth=2, max_documents=5)
    profiles = {"example.com/docs": DomainProfile(max_depth=4, max_documents=9)}
    limits = resolve_crawl_limits(URL, app=APP, run=run, profiles=profiles)
    assert limits.max_depth == 4
    assert limits.max_documents == 9


def test_unset_run_layer_falls_back_to_app() -> None:
    app = CrawlConfig(max_documents=7)
    limits = resolve_crawl_limits(URL, app=app, run=CrawlRunConfig())
    assert limits.max_documents == 7


def test_reject_zero_max_documents_at_config_load() -> None:
    with pytest.raises(ValidationError):
        CrawlConfig(max_documents=0)
    with pytest.raises(ValidationError):
        CrawlRunConfig(max_documents=0)
    with pytest.raises(ValidationError):
        DomainProfile(max_documents=0)


def test_reject_empty_glob_entries() -> None:
    with pytest.raises(ValidationError):
        CrawlConfig(exclude_paths=[""])
    with pytest.raises(ValidationError):
        DomainProfile(exclude_documents=["  "])


# --------------------------------------------------------------------------- #
# (e) блэклисты: путь vs basename, case-sensitive
# --------------------------------------------------------------------------- #


def test_blacklist_path_and_basename_matching() -> None:
    limits = CrawlLimits(exclude_paths=["/private/*"], exclude_documents=["*.pdf"])
    assert blacklist_match("https://h/private/a", limits) == "/private/*"
    assert blacklist_match("https://h/docs/a.pdf", limits) == "*.pdf"
    # case-sensitive: /private/A матчит glob (звёздочка поглощает регистр), но *.PDF — нет.
    assert is_blacklisted("https://h/private/A", limits) is True
    assert is_blacklisted("https://h/docs/A.PDF", limits) is False
    assert is_blacklisted("https://h/public/a", limits) is False
    # подстрока/суффикс не матчатся: только fnmatch по сегменту/пути.
    assert is_blacklisted("https://h/a/b.pdf.text", limits) is False


# --------------------------------------------------------------------------- #
# (c) scope по seed-root
# --------------------------------------------------------------------------- #


def test_root_path_prefix() -> None:
    assert root_path_prefix("https://h/") == ""
    assert root_path_prefix("https://h") == ""
    assert root_path_prefix("https://h/documents/") == "/documents"
    assert root_path_prefix("https://h/documents") == "/documents"


def test_frontier_scope_host_and_path_boundary() -> None:
    limits = CrawlLimits(max_depth=3)
    frontier = Frontier("https://h/documents", limits)
    assert frontier.in_scope("https://h/documents/x") is True
    assert frontier.in_scope("https://h/documents") is True
    assert frontier.in_scope("https://h/documents-archive") is False
    # C (execution_change): поддомен больше **не** в scope по одному факту поддоменности —
    # только если объявлен оператором (`scope_hosts`). Прежний пин (поддомен всегда in
    # scope) снят намеренно: именно он позволял `edu.postgrespro.ru` съесть бюджет
    # `postgrespro.ru`.
    assert frontier.in_scope("https://sub.h/documents/x") is False
    declared = Frontier("https://h/documents", limits, scope_hosts=["sub.h"])
    assert declared.in_scope("https://sub.h/documents/x") is True
    assert declared.in_scope("https://other.h/documents/x") is False
    assert frontier.in_scope("https://other/documents/x") is False
    assert frontier.in_scope("mailto:x@y") is False
    assert frontier.in_scope("https://h/documents#frag") is True


def test_frontier_bfs_offer_depth_and_truncation() -> None:
    limits = CrawlLimits(max_depth=1)
    frontier = Frontier("https://h/", limits)
    frontier.add_seed(ROOT_URL := "https://h/")
    node = frontier.next()
    assert node == (ROOT_URL, 0)
    # Ссылки глубины 1 ставятся; на границе (глубина == max_depth) — усечение по глубине.
    frontier.offer(["https://h/a"], 0)
    assert frontier.next() == ("https://h/a", 1)
    frontier.offer(["https://h/b"], 1)
    assert frontier.next() is None
    assert frontier.truncated is True
    assert frontier.truncated_reason == "max_depth"


def test_frontier_max_documents_capacity() -> None:
    limits = CrawlLimits(max_depth=5, max_documents=1)
    frontier = Frontier("https://h/", limits)
    assert frontier.add_seed("https://h/") == "queued"
    frontier.note_fetched()
    assert frontier.has_capacity is False
    # Постановка нового URL после достижения потолка → усечение, не исключение.
    assert frontier.discover("https://h/a", 1) is False
    assert frontier.truncated is True
    assert frontier.truncated_reason == "max_documents"


def test_frontier_blacklisted_seed_surfaced() -> None:
    limits = CrawlLimits(exclude_paths=["/blocked*"])
    frontier = Frontier("https://h/", limits)
    assert frontier.add_seed("https://h/blocked") == "blacklisted"
    assert frontier.skipped_blacklisted == ["https://h/blocked"]


# --------------------------------------------------------------------------- #
# (N2) percent-encoded path traversal не обходит path-scope
# --------------------------------------------------------------------------- #


def test_frontier_rejects_encoded_dot_dot_traversal() -> None:
    """N2: ``%2e%2e`` и буквальный ``..`` отвергаются, обычный путь — нет."""
    from core.pipeline.crawl_frontier import decoded_scope_path

    limits = CrawlLimits(max_depth=3)
    frontier = Frontier("https://docs.test/documents", limits)
    # Буквальная и percent-encoded формы dot-segment — вне scope (сервер декодирует → ../).
    assert frontier.in_scope("https://docs.test/documents/../private") is False
    assert frontier.in_scope("https://docs.test/documents/%2e%2e/private") is False
    assert frontier.in_scope("https://docs.test/documents/%2E%2E/private") is False
    # Нормальный вложенный путь остаётся в scope.
    assert frontier.in_scope("https://docs.test/documents/x") is True
    # Декодер отдаёт декодированный путь и ``None`` на traversal.
    assert decoded_scope_path("/documents/a%20b") == "/documents/a b"
    assert decoded_scope_path("/documents/%2e%2e/x") is None


# --------------------------------------------------------------------------- #
# (C) scope = объявленные хосты (execution_change §13): поддомен ≠ автоматически in scope
# --------------------------------------------------------------------------- #

def test_frontier_undeclared_subdomain_is_out_of_scope_and_counted() -> None:
    """C: необъявленный поддомен вне scope корня, отказ **видим** (счётчик по хостам).

    Живой прогон 22.10: ``postgrespro.ru`` израсходовал 800-страничный бюджет на
    ``edu.postgrespro.ru``, которого оператор не объявлял. Теперь такой URL отвергается,
    а число отвергнутых поддоменов по хостам попадает в провенанс корня.
    """
    frontier = Frontier("https://example.org/", CrawlLimits(max_depth=3))
    assert frontier.in_scope("https://edu.example.org/a.html") is False
    assert frontier.discover("https://edu.example.org/a.html", 1) is False
    # Дедуп по URL: повторное предложение того же URL счётчик не удваивает.
    assert frontier.discover("https://edu.example.org/a.html", 1) is False
    assert frontier.discover("https://edu.example.org/b.html", 1) is False
    assert frontier.discover("https://career.example.org/c.html", 1) is False
    assert frontier.out_of_scope_subdomains == {
        "edu.example.org": 2,
        "career.example.org": 1,
    }
    assert frontier.out_of_scope_subdomain_examples["edu.example.org"] == [
        "https://edu.example.org/a.html",
        "https://edu.example.org/b.html",
    ]
    # Корневой хост и его ``www.``-вариант — не поддомены, в счётчик не попадают.
    assert frontier.discover("https://example.org/ok", 1) is True
    assert frontier.discover("https://www.example.org/ok", 1) is True
    assert "example.org" not in frontier.out_of_scope_subdomains
    assert "www.example.org" not in frontier.out_of_scope_subdomains
    # Принятые элементы с «чужого» хоста видны отдельно (здесь — только www-вариант,
    # который нормализуется к хосту корня и потому не считается чужим).
    assert frontier.accepted_foreign_hosts == {}

def test_frontier_declared_subdomain_in_scope_and_counted_as_foreign_host() -> None:
    """C: объявленный поддомен (профиль/корень) в scope; его элементы считаются отдельно."""
    frontier = Frontier(
        "https://example.org/",
        CrawlLimits(max_depth=3),
        scope_hosts=["docs.example.org"],
    )
    assert frontier.in_scope("https://docs.example.org/guide") is True
    assert frontier.discover("https://docs.example.org/guide", 1) is True
    assert frontier.accepted_foreign_hosts == {"docs.example.org": 1}
    # Объявлен только ``docs`` — прочие поддомены по-прежнему вне scope.
    assert frontier.in_scope("https://edu.example.org/a.html") is False

def test_frontier_www_equivalence_is_preserved_both_ways() -> None:
    """``www.`` эквивалентен базовому хосту с обеих сторон (jatoba.ru / www.gaz-is.ru)."""
    bare_root = Frontier("https://jatoba.ru/", CrawlLimits(max_depth=2))
    assert bare_root.in_scope("https://jatoba.ru/") is True
    assert bare_root.in_scope("https://www.jatoba.ru/about") is True

    www_root = Frontier("https://www.gaz-is.ru/", CrawlLimits(max_depth=2))
    assert www_root.in_scope("https://gaz-is.ru/about") is True
    assert www_root.in_scope("https://www.gaz-is.ru/about") is True
    # Другой поддомен того же вендора — всё равно вне scope (объявлен только базовый хост).
    assert www_root.in_scope("https://shop.gaz-is.ru/about") is False

    # ``www.`` в самом объявлении хоста нормализуется (объявлен ``www.docs.example.org``).
    declared = Frontier(
        "https://example.org/",
        CrawlLimits(max_depth=2),
        scope_hosts=["www.docs.example.org"],
    )
    assert declared.in_scope("https://docs.example.org/x") is True

def test_frontier_terminal_artifact_still_honours_declared_host_scope() -> None:
    """``terminal_artifact=True`` обходит path-prefix, но **не** host scope (C)."""
    limits = CrawlLimits(max_depth=3)
    frontier = Frontier("https://example.org/docs", limits)
    # Артефакт вне path-prefix на своём хосте — допущен (P6b).
    assert frontier.in_scope("https://example.org/files/report.pdf") is False
    assert frontier.in_scope(
        "https://example.org/files/report.pdf", terminal_artifact=True
    ) is True
    # Артефакт на необъявленном поддомене — вне scope даже как терминальный артефакт.
    assert frontier.in_scope(
        "https://edu.example.org/files/report.pdf", terminal_artifact=True
    ) is False
    declared = Frontier(
        "https://example.org/docs", limits, scope_hosts=["files.example.org"]
    )
    assert declared.in_scope(
        "https://files.example.org/report.pdf", terminal_artifact=True
    ) is True

def test_frontier_with_limits_keeps_scope_and_counters() -> None:
    """Клон frontier не теряет scope-хосты и счётчики поддоменов (D4-seam)."""
    frontier = Frontier(
        "https://example.org/", CrawlLimits(max_depth=3), scope_hosts=["docs.example.org"]
    )
    frontier.discover("https://edu.example.org/a.html", 1)
    frontier.discover("https://docs.example.org/a.html", 1)
    clone = frontier.with_limits(CrawlLimits(max_depth=1))
    assert clone.in_scope("https://docs.example.org/b.html") is True
    assert clone.in_scope("https://edu.example.org/b.html") is False
    assert clone.out_of_scope_subdomains == {"edu.example.org": 1}
    assert clone.accepted_foreign_hosts == {"docs.example.org": 1}

# --------------------------------------------------------------------------- #
# (N5) блэклист: tolerance ведущего слэша; document-basename caveat
# --------------------------------------------------------------------------- #


def test_blacklist_path_pattern_without_leading_slash() -> None:
    """N5: ``docs/*`` без ведущего ``/`` матчит ``/docs/x`` как ``/docs/*``."""
    limits = CrawlLimits(exclude_paths=["docs/*", "/private/*"])
    assert blacklist_match("https://h/docs/x", limits) == "docs/*"
    assert blacklist_match("https://h/private/a", limits) == "/private/*"
    assert is_blacklisted("https://h/public/x", limits) is False


def test_exclude_documents_matches_path_basename_with_query() -> None:
    """N5: basename берётся из пути (query отбрасывается); имя только в query — caveat."""
    limits = CrawlLimits(exclude_documents=["*.pdf"])
    # Query-строка не мешает: basename — из path-компонента.
    assert blacklist_match("https://h/docs/report.pdf?ver=1", limits) == "*.pdf"
    # Caveat: имя файла, переданное только в query, basename не является (задокументировано).
    assert is_blacklisted("https://h/download?file=report.pdf", limits) is False


# --------------------------------------------------------------------------- #
# (h) P0/OOM: max_response_bytes — дефолт 20 MiB и цепочка app < run < site
# --------------------------------------------------------------------------- #


def test_max_response_bytes_default_is_20_mib_and_digest_neutral() -> None:
    limits = resolve_crawl_limits(URL, app=CrawlConfig())
    assert limits.max_response_bytes == 20_971_520
    # Дефолт app не «задан явно» — ключ не попадает в дайджест (байт-идентичность).
    assert "max_response_bytes" not in limits.as_dict()


def test_max_response_bytes_precedence_app_run_site() -> None:
    app = CrawlConfig(max_response_bytes=1000)
    run = CrawlRunConfig(max_response_bytes=2000)
    site = DomainProfile(max_response_bytes=3000)
    assert (
        resolve_crawl_limits(
            URL, app=app, run=run, profiles={"example.com": site}
        ).max_response_bytes
        == 3000
    )
    assert (
        resolve_crawl_limits(URL, app=app, run=run, profiles={}).max_response_bytes
        == 2000
    )
    assert (
        resolve_crawl_limits(URL, app=app, run=None, profiles={}).max_response_bytes
        == 1000
    )


def test_max_response_bytes_explicit_enters_as_dict() -> None:
    limits = resolve_crawl_limits(
        URL, app=CrawlConfig(), run=CrawlRunConfig(max_response_bytes=777)
    )
    assert limits.as_dict()["max_response_bytes"] == 777
    assert (
        resolve_crawl_limits(
            URL,
            app=CrawlConfig(),
            profiles={"example.com": DomainProfile(max_response_bytes=888)},
        ).as_dict()["max_response_bytes"]
        == 888
    )


def test_max_response_bytes_rejects_non_positive() -> None:
    with pytest.raises(ValidationError):
        CrawlConfig(max_response_bytes=0)
    with pytest.raises(ValidationError):
        CrawlRunConfig(max_response_bytes=0)
    with pytest.raises(ValidationError):
        DomainProfile(max_response_bytes=0)


# --------------------------------------------------------------------------- #
# P6: оси допуска контейнеров и границ спуска
# --------------------------------------------------------------------------- #


def test_artifact_axes_defaults_are_digest_neutral() -> None:
    """Дефолты app не делают дайджест другим: ключей нет без явного значения (§13)."""
    limits = resolve_crawl_limits(URL, app=APP)
    assert limits.max_artifact_bytes == 262_144_000
    assert limits.attachment_formats == ("zip", "7z", "rar")
    assert limits.archive_max_member_bytes == 67_108_864
    assert limits.archive_max_uncompressed_bytes == 268_435_456
    assert limits.archive_max_members is None
    payload = limits.as_dict()
    for key in (
        "max_artifact_bytes",
        "attachment_formats",
        "archive_max_member_bytes",
        "archive_max_uncompressed_bytes",
        "archive_max_members",
        "archive_descent",
    ):
        assert key not in payload


def test_artifact_axes_explicit_enter_as_dict() -> None:
    limits = resolve_crawl_limits(
        URL,
        app=CrawlConfig(),
        run=CrawlRunConfig(
            max_artifact_bytes=1_000_000,
            attachment_formats=["zip"],
            archive_max_member_bytes=2_000_000,
            archive_max_uncompressed_bytes=3_000_000,
            archive_max_members=7,
            archive_descent="plan",
        ),
    )
    payload = limits.as_dict()
    assert payload["max_artifact_bytes"] == 1_000_000
    assert payload["attachment_formats"] == ["zip"]
    assert payload["archive_max_member_bytes"] == 2_000_000
    assert payload["archive_max_uncompressed_bytes"] == 3_000_000
    assert payload["archive_max_members"] == 7
    assert payload["archive_descent"] == "plan"


def test_artifact_axes_precedence_app_run_site() -> None:
    app = CrawlConfig(max_artifact_bytes=1000, attachment_formats=["zip"])
    run = CrawlRunConfig(max_artifact_bytes=2000, attachment_formats=["7z"])
    site = DomainProfile(max_artifact_bytes=3000, attachment_formats=["rar"])
    assert (
        resolve_crawl_limits(URL, app=app, run=run, profiles={"example.com": site})
        .max_artifact_bytes
        == 3000
    )
    assert (
        resolve_crawl_limits(URL, app=app, run=run, profiles={}).max_artifact_bytes
        == 2000
    )
    assert (
        resolve_crawl_limits(URL, app=app, run=None, profiles={}).max_artifact_bytes
        == 1000
    )
    assert (
        resolve_crawl_limits(URL, app=app, run=run, profiles={"example.com": site})
        .attachment_formats
        == ("rar",)
    )
    assert (
        resolve_crawl_limits(URL, app=app, run=run, profiles={}).attachment_formats
        == ("7z",)
    )


def test_attachment_formats_normalized_and_validated() -> None:
    assert CrawlConfig(attachment_formats=[".ZIP", "Zip"]).attachment_formats == ("zip",)
    assert CrawlRunConfig(attachment_formats=["7z"]).attachment_formats == ["7z"]
    assert DomainProfile(attachment_formats=["RAR"]).attachment_formats == ["rar"]
    with pytest.raises(ValidationError):
        CrawlConfig(attachment_formats=["gz"])
    with pytest.raises(ValidationError):
        CrawlRunConfig(attachment_formats=["ova"])
    with pytest.raises(ValidationError):
        DomainProfile(attachment_formats=["iso"])


def test_archive_descent_enum_validation() -> None:
    assert CrawlConfig().archive_descent.value == "full"
    assert CrawlRunConfig(archive_descent="plan").archive_descent.value == "plan"
    with pytest.raises(ValidationError):
        CrawlRunConfig(archive_descent="deep")


# --------------------------------------------------------------------------- #
# P6b: терминальный артефакт — обход path-prefix (host/SSRF/блэклист сохраняются)
# --------------------------------------------------------------------------- #

DOC_ROOT = "https://docs.test/documents"
OUTSIDE_PDF = "https://docs.test/files/report.pdf"
OUTSIDE_ZIP = "https://docs.test/files/bundle.zip"


def test_artifact_outside_path_prefix_is_admitted_and_recorded() -> None:
    """Документ вне префикса — enqueued (он не продолжает навигацию), обход видим."""
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.in_scope(OUTSIDE_PDF) is False

    assert frontier.discover(OUTSIDE_PDF, 1) is True
    assert frontier.next() == (OUTSIDE_PDF, 1)
    assert frontier.artifacts_outside_prefix_count == 1
    assert frontier.artifacts_outside_prefix == [OUTSIDE_PDF]
    # Заявленный контейнер — тот же класс.
    assert frontier.discover(OUTSIDE_ZIP, 1) is True
    assert frontier.artifacts_outside_prefix_count == 2


def test_artifact_on_foreign_host_is_rejected() -> None:
    """Host scope не отменяется обходом: чужой host — вне scope без записи обхода."""
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.discover("https://other.test/files/report.pdf", 1) is False
    assert frontier.artifacts_outside_prefix_count == 0


def test_artifact_traversal_is_still_rejected() -> None:
    """``..``-traversal не становится обходом scope даже для артефакта (N2)."""
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert (
        frontier.discover("https://docs.test/documents/../private/report.pdf", 1)
        is False
    )
    assert (
        frontier.discover("https://docs.test/documents/%2e%2e/private/report.pdf", 1)
        is False
    )
    assert frontier.artifacts_outside_prefix_count == 0


def test_blacklisted_artifact_still_rejected_and_not_counted_as_bypass() -> None:
    """Блэклист оператора побеждает обход: URL виден как blacklisted, обхода не было."""
    limits = CrawlLimits(max_depth=3, exclude_documents=["*.pdf"])
    frontier = Frontier(DOC_ROOT, limits)

    assert frontier.discover(OUTSIDE_PDF, 1) is False
    assert frontier.skipped_blacklisted == [OUTSIDE_PDF]
    assert frontier.artifacts_outside_prefix_count == 0


def test_blacklisted_path_wins_over_artifact_bypass() -> None:
    limits = CrawlLimits(max_depth=3, exclude_paths=["/files/*"])
    frontier = Frontier(DOC_ROOT, limits)
    assert frontier.discover(OUTSIDE_PDF, 1) is False
    assert frontier.skipped_blacklisted == [OUTSIDE_PDF]
    assert frontier.artifacts_outside_prefix_count == 0


def test_html_outside_path_prefix_is_still_rejected() -> None:
    """Обход — свойство класса, а не «любой URL вне префикса»."""
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.discover("https://docs.test/files/page.html", 1) is False
    assert frontier.discover("https://docs.test/files/disk.ova", 1) is False
    assert frontier.artifacts_outside_prefix_count == 0
    assert frontier.next() is None


def test_in_scope_terminal_artifact_skips_path_prefix_only() -> None:
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.in_scope(OUTSIDE_PDF, terminal_artifact=True) is True
    # Host scope и traversal-guard действуют и с флагом.
    assert frontier.in_scope("https://other.test/x.pdf", terminal_artifact=True) is False
    assert (
        frontier.in_scope(
            "https://docs.test/documents/%2e%2e/x.pdf", terminal_artifact=True
        )
        is False
    )
    # Не-артефакт внутри префикса — прежний in-scope.
    assert frontier.in_scope("https://docs.test/documents/x.html") is True
    # Флаг не универсальный пропуск: страница вне префикса не проходит и с ним.
    assert frontier.in_scope("https://docs.test/files/x.html", terminal_artifact=True) is False


def test_scope_admission_three_outcomes() -> None:
    from core.pipeline.crawl_frontier import (
        SCOPE_ARTIFACT_OUTSIDE_PREFIX,
        SCOPE_IN,
        SCOPE_OUT,
    )

    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.scope_admission("https://docs.test/documents/a") == SCOPE_IN
    assert frontier.scope_admission(OUTSIDE_PDF) == SCOPE_ARTIFACT_OUTSIDE_PREFIX
    assert frontier.scope_admission("https://docs.test/files/a.html") == SCOPE_OUT
    assert frontier.scope_admission("https://other.test/files/a.pdf") == SCOPE_OUT
    # admission ничего не пишет: запись обхода — обязанность принявшего в очередь.
    assert frontier.artifacts_outside_prefix_count == 0


def test_revalidate_seed_admits_known_artifact_outside_prefix() -> None:
    """Известный URL-артефакт (url_state) вне префикса ревалидируется, а не отбрасывается."""
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.add_revalidate_seed(OUTSIDE_PDF) == "queued"
    assert frontier.next() == (OUTSIDE_PDF, 0)
    assert frontier.artifacts_outside_prefix_count == 1
    # Не-артефакт вне префикса — прежний orphan-путь вызывающего.
    assert frontier.add_revalidate_seed("https://docs.test/files/a.html") == "out_of_scope"


def test_bypass_examples_are_bounded_but_count_is_exact() -> None:
    from core.pipeline.crawl_frontier import ARTIFACT_BYPASS_EXAMPLES_MAX

    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    urls = [f"https://docs.test/files/doc-{i}.pdf" for i in range(25)]
    for url in urls:
        assert frontier.discover(url, 1) is True

    assert frontier.artifacts_outside_prefix_count == 25
    assert len(frontier.artifacts_outside_prefix) == ARTIFACT_BYPASS_EXAMPLES_MAX
    assert frontier.artifacts_outside_prefix == urls[:ARTIFACT_BYPASS_EXAMPLES_MAX]


def test_bypass_survives_with_limits_clone() -> None:
    frontier = Frontier(DOC_ROOT, CrawlLimits(max_depth=3))
    assert frontier.discover(OUTSIDE_PDF, 1) is True

    clone = frontier.with_limits(CrawlLimits(max_depth=1))
    assert clone.artifacts_outside_prefix_count == 1
    assert clone.artifacts_outside_prefix == [OUTSIDE_PDF]
