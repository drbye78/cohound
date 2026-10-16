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


def test_domain_profile_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        DomainProfile(bogus=1)  # type: ignore[call-arg]


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
    assert frontier.in_scope("https://sub.h/documents/x") is True  # поддомен
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
