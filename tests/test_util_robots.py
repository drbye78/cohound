"""Тесты единой робот-политики ``core.util.robots`` (D1/§10) и её промоушена.

Без сети: текст ленты robots инъектируется. Проверяются дефолт-разрешение при недоступной
ленте, явный запрет, ``Crawl-delay``, RFC 9309-матчинг (``*``/``$``/query, самое длинное
правило, при равенстве ``Allow``) и то, что ``core.pipeline.crawl_probe`` ре-экспортирует
ту же реализацию (single implementation).
"""

from __future__ import annotations

from core.pipeline.crawl_probe import RobotsGate as ProbeRobotsGate
from core.util.robots import RobotsGate

DISALLOW = "User-agent: *\nDisallow: /blocked\n"
DELAY = "User-agent: *\nCrawl-delay: 5\n"

# Реальные формы правил из robots.txt сайтов когорты (см. P7 gate §1):
# wildcard-инфикс, query-правило, path-правило, end-anchor и allow-исключения.
REAL_WORLD = """User-agent: *
Disallow: *.pdf*
Disallow: /*?*
Disallow: /tables
Allow: /*page=*
Allow: /*.gif
Allow: *.css*
"""


def _gate(text: str) -> RobotsGate:
    return RobotsGate(lambda _host: text)


def test_unavailable_robots_is_allowed() -> None:
    gate = RobotsGate(lambda _host: None)
    assert gate.allowed("https://example.com/anything") is True


def test_disallow_refused() -> None:
    gate = RobotsGate(lambda _host: DISALLOW)
    assert gate.allowed("https://example.com/blocked") is False
    assert gate.allowed("https://example.com/open") is True


def test_crawl_delay_reported() -> None:
    gate = RobotsGate(lambda _host: DELAY)
    assert gate.crawl_delay("https://example.com/x") == 5.0
    assert RobotsGate(lambda _host: None).crawl_delay("https://example.com/x") is None


def test_fetch_error_is_allowed_default() -> None:
    def boom(_host: str) -> str:
        raise RuntimeError("no robots")

    assert RobotsGate(boom).allowed("https://example.com/x") is True


def test_crawl_probe_reexports_single_implementation() -> None:
    assert ProbeRobotsGate is RobotsGate


def test_wildcard_infix_blocks_documents() -> None:
    """``Disallow: *.pdf*`` — реальная форма с хостов когорты (сейчас не матчилась)."""
    gate = _gate("User-agent: *\nDisallow: *.pdf*\n")
    assert gate.allowed("https://example.com/a/b.pdf") is False
    assert gate.allowed("https://example.com/doc/x.pdf?y=1") is False
    assert gate.allowed("https://example.com/doc/x.html") is True


def test_end_anchor_matches_exact_suffix() -> None:
    """``Disallow: *.pdf$`` — якорь конца: query уже другое правило."""
    gate = _gate("User-agent: *\nDisallow: *.pdf$\n")
    assert gate.allowed("https://example.com/a/b.pdf") is False
    assert gate.allowed("https://example.com/a/b.pdf?x=1") is True


def test_query_rule_matches_path_plus_query() -> None:
    """``Disallow: /*?*`` обязан ловить query (сравнение по пути **и** query)."""
    gate = _gate("User-agent: *\nDisallow: /*?*\n")
    assert gate.allowed("https://example.com/x?y=1") is False
    assert gate.allowed("https://example.com/x") is True


def test_longest_match_wins_over_shorter_disallow() -> None:
    gate = _gate("User-agent: *\nDisallow: /docs\nAllow: /docs/public\n")
    assert gate.allowed("https://example.com/docs/public/a") is True
    assert gate.allowed("https://example.com/docs/private/a") is False


def test_tie_allow_wins() -> None:
    gate = _gate("User-agent: *\nDisallow: /same\nAllow: /same\n")
    assert gate.allowed("https://example.com/same") is True
    # Порядок правил в ленте на исход не влияет.
    reversed_gate = _gate("User-agent: *\nAllow: /same\nDisallow: /same\n")
    assert reversed_gate.allowed("https://example.com/same") is True


def test_star_agent_gets_only_star_group() -> None:
    text = (
        "User-agent: *\nDisallow: /a\n\n"
        "User-agent: other\nDisallow: /b\n"
    )
    gate = _gate(text)
    assert gate.allowed("https://example.com/a") is False
    assert gate.allowed("https://example.com/b") is True
    # Именованный агент, наоборот, берёт свою группу (длиннее, чем ``*``).
    assert RobotsGate(lambda _host: text, user_agent="other").allowed(
        "https://example.com/b"
    ) is False


def test_empty_disallow_allows_everything() -> None:
    gate = _gate("User-agent: *\nDisallow:\n")
    assert gate.allowed("https://example.com/anything") is True
    assert gate.allowed("https://example.com/x?y=1") is True


def test_non_ascii_rule_matches_percent_encoded_url() -> None:
    gate = _gate("User-agent: *\nDisallow: /документы\n")
    assert (
        gate.allowed(
            "https://example.com/%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D1%8B/page"
        )
        is False
    )
    # Обратное направление: правило percent-encoded, URL — обычный.
    encoded_gate = _gate(
        "User-agent: *\nDisallow: /%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D1%8B\n"
    )
    assert encoded_gate.allowed("https://example.com/документы/page") is False


def test_crawl_delay_inside_group_reported() -> None:
    gate = _gate("User-agent: *\nDisallow: /x\nCrawl-delay: 7\n")
    assert gate.crawl_delay("https://example.com/x") == 7.0


def test_real_world_rule_shapes() -> None:
    """Регрессия на точных формах из лент когорты (P7 gate §1)."""
    gate = _gate(REAL_WORLD)
    assert gate.allowed("https://example.com/a/b.pdf") is False
    assert gate.allowed("https://example.com/x?y=1") is False
    assert gate.allowed("https://example.com/tables") is False
    assert gate.allowed("https://example.com/doc/x.html") is True
    # Allow-исключения длиннее совпавших Disallow.
    assert gate.allowed("https://example.com/tables?page=2") is True
    assert gate.allowed("https://example.com/logo.gif") is True
    assert gate.allowed("https://example.com/static/site.css") is True
