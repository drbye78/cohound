"""Тесты единой робот-политики ``core.util.robots`` (D1/§10) и её промоушена.

Без сети: текст ленты robots инъектируется. Проверяются дефолт-разрешение при недоступной
ленте, явный запрет, ``Crawl-delay``, RFC 9309-матчинг (``*``/``$``/query, самое длинное
правило, при равенстве ``Allow``), ABNF-пустые строки внутри группы, UTF-8 BOM, линейность
матчера на вложенных звёздочках и то, что ``core.pipeline.crawl_probe`` ре-экспортирует
ту же реализацию (single implementation).
"""

from __future__ import annotations

import time

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

# Точная форма живой ленты https://jatoba.ru/robots.txt (F2): пустая строка сразу после
# ``User-agent: *``, затем правила. Раньше пустая строка закрывала группу и все 7 правил
# терялись (хост, реально входящий в пакет кейса).
JATOBA = """Host: jatoba.ru
User-agent: *

Disallow: /cms/
Disallow: /install.php
Disallow: *&etext=
Disallow: *&pco=
Disallow: *&matchtype=
Disallow: *erid
Disallow: *etext


User-agent: Yandex
Clean-Param: erid
Clean-Param: utm
Clean-Param: etext
Disallow: /cms/
Disallow: /install.php
Disallow: *&etext=
Disallow: *&pco=
Disallow: *&matchtype=
Disallow: *erid
Disallow: *etext
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


def test_blank_line_inside_group_keeps_rules() -> None:
    """F2: ``emptyline`` допустима внутри группы (ABNF RFC 9309 §2.1).

    Регрессия на точной форме живой ленты ``jatoba.ru``: пустая строка сразу после
    ``User-agent: *`` не должна выбрасывать всю группу.
    """
    gate = _gate(JATOBA)
    assert gate.allowed("https://jatoba.ru/cms/admin") is False
    assert gate.allowed("https://jatoba.ru/install.php") is False
    assert gate.allowed("https://jatoba.ru/x?&etext=1") is False
    assert gate.allowed("https://jatoba.ru/x?&pco=2") is False
    assert gate.allowed("https://jatoba.ru/x?&matchtype=a") is False
    assert gate.allowed("https://jatoba.ru/erid") is False
    assert gate.allowed("https://jatoba.ru/x?etext=1") is False
    assert gate.allowed("https://jatoba.ru/") is True
    assert gate.allowed("https://jatoba.ru/x?y=1") is True


def test_blank_lines_between_rules_stay_in_group() -> None:
    """F2: пустые строки между правилами — часть той же группы."""
    gate = _gate("User-agent: *\nDisallow: /a\n\nDisallow: /b\n\n\nDisallow: /c\n")
    assert gate.allowed("https://example.com/a") is False
    assert gate.allowed("https://example.com/b") is False
    assert gate.allowed("https://example.com/c") is False
    assert gate.allowed("https://example.com/d") is True


def test_blank_line_between_groups_still_separates() -> None:
    """F2: ``User-agent`` после правил открывает новую группу (пустые строки не мешают)."""
    text = "User-agent: *\nDisallow: /a\n\nUser-agent: other\nDisallow: /b\n"
    gate = _gate(text)
    assert gate.allowed("https://example.com/a") is False
    assert gate.allowed("https://example.com/b") is True
    named = RobotsGate(lambda _host: text, user_agent="other")
    assert named.allowed("https://example.com/a") is True
    assert named.allowed("https://example.com/b") is False


def test_utf8_bom_does_not_drop_first_group() -> None:
    """F5: ведущий UTF-8 BOM не должен «съедать» первый ключ и всю группу ``*``."""
    gate = _gate("\ufeffUser-agent: *\nDisallow: /secret\n")
    assert gate.allowed("https://example.com/secret") is False
    assert gate.allowed("https://example.com/open") is True


def test_multiple_stars_match_sequentially() -> None:
    """Линейный матчер: средние отрезки ищутся по порядку, ``$`` якорит конец."""
    gate = _gate("User-agent: *\nDisallow: /a*b*c$\n")
    assert gate.allowed("https://example.com/axxbyyc") is False
    assert gate.allowed("https://example.com/abc") is False
    assert gate.allowed("https://example.com/axxbyycd") is True  # не конец
    assert gate.allowed("https://example.com/axxcyyb") is True  # порядок нарушен


def test_nested_wildcards_are_linear_not_catastrophic() -> None:
    """F3/ReDoS: ~12 звёздочек против 400-символьного пути — линейный матчер.

    Прежняя regex-реализация (``.*`` на каждую ``*`` + ``re.match``) на этом входе
    бэктрекала экспоненциально и не завершалась за минуты. Граница ``< 0.5 s`` — про
    **сложность** (линейный проход), а не про скорость конкретной машины.
    """
    pattern = "/a" + "*a" * 12 + "b$"
    gate = _gate(f"User-agent: *\nDisallow: {pattern}\n")

    started = time.perf_counter()
    # Нет завершающего ``b`` — правило не совпало, хост разрешён.
    assert gate.allowed("https://example.com/" + "a" * 400) is True
    elapsed = time.perf_counter() - started
    assert elapsed < 0.5, f"нелинейный матчинг: {elapsed:.3f}s"

    started = time.perf_counter()
    # Завершающий ``b`` — правило совпало.
    assert gate.allowed("https://example.com/" + "a" * 400 + "b") is False
    elapsed = time.perf_counter() - started
    assert elapsed < 0.5, f"нелинейный матчинг: {elapsed:.3f}s"
