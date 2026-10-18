"""Дефект B (I17): ``<loc>`` sitemap — **подчинённые** seeds корня, а не задающие scope.

Seed оператора задаёт scope корня (host + path-префикс по границе сегмента). Раньше
``<loc>`` per-site ``DomainProfile.sitemap`` ставились через ``Frontier.add_seed`` без
scope-чека, поэтому host-wide sitemap при path-scoped seed заливал в продукт-партицию
все страницы хоста — нарушение I17 (ретривал продукт-скоупед: evidence только из
паспорта продукта и его сателлитов). Гейт: ``p7-crawl-completeness-gate.md`` §2.

Проверяется: loc внутри path-scope корня фетчится; loc вне scope — не фетчится и виден
в ``errors`` (не тихий дроп); host-root seed по-прежнему означает «весь host в scope»;
loc, отвергнутый корнем A, остаётся доступным корню B того же продукта; один и тот же
loc двух корней фетчится не более одного раза; без sitemap поллер не вызывается.

Харнесс переиспользуется из ``test_corpus`` (те же детерминированные фейки портов):
``tests/pipeline`` без ``__init__.py``, поэтому pytest кладёт каталог в ``sys.path``.
"""

from __future__ import annotations

from pathlib import Path

from core.config.domain_profiles import DomainProfile
from core.util.feed import FeedEntry

from test_corpus import _Crawler, _FakePoller, _build, _build_of

HOST = "docs.test"
SITEMAP_URL = f"https://{HOST}/sitemap.xml"

#: Path-scoped корень ``/p1/a``: соседний путь ``/p1/b/...`` — вне scope.
ROOT_A = f"https://{HOST}/p1/a"
LOC_A_IN = f"https://{HOST}/p1/a/page"
LOC_A_OUT = f"https://{HOST}/p1/b/page"

#: Второй корень того же продукта (свой path-scope).
ROOT_B = f"https://{HOST}/p2/b"
LOC_B_IN = f"https://{HOST}/p2/b/page"

#: Host-корень: ``https://h/`` ⇒ scope = весь host.
ROOT_HOST = f"https://{HOST}/"


def _sitemap_profile() -> dict[str, DomainProfile]:
    """Per-site профиль host-ключа с sitemap (нейтральные фикстуры §1.3)."""
    return {HOST: DomainProfile(sitemap=SITEMAP_URL)}


def _locs(*urls: str) -> _FakePoller:
    """``PollPort``-двойник sitemap с заданными ``<loc>``."""
    return _FakePoller([FeedEntry(url=url) for url in urls])


def test_loc_inside_path_scope_is_fetched(tmp_path: Path) -> None:
    """``<loc>`` под path-префиксом корня — ставится и фетчится (аугментация seed'а)."""
    poller = _locs(LOC_A_IN)
    crawler = _Crawler({ROOT_A: b"seed", LOC_A_IN: b"loc"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_A]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    assert result.errors == []
    assert [c[0] for c in poller.calls] == [SITEMAP_URL]
    assert [u for u, _ in crawler.calls] == [ROOT_A, LOC_A_IN]
    assert {d.url for d in _build_of(result).docs} == {ROOT_A, LOC_A_IN}


def test_loc_outside_path_scope_not_fetched_and_visible(tmp_path: Path) -> None:
    """``<loc>`` вне path-scope корня не ставится, но отказ виден в ``errors``."""
    poller = _locs(LOC_A_IN, LOC_A_OUT)
    crawler = _Crawler({ROOT_A: b"seed", LOC_A_IN: b"loc", LOC_A_OUT: b"other"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_A]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    assert LOC_A_OUT not in [u for u, _ in crawler.calls]
    # Не тихий дроп: запись называет loc и корень, относительно scope которого он отвергнут.
    assert any(
        LOC_A_OUT in err and "вне scope корня" in err and ROOT_A in err
        for err in result.errors
    )
    assert {d.url for d in _build_of(result).docs} == {ROOT_A, LOC_A_IN}


def test_host_root_seed_keeps_whole_host_in_scope(tmp_path: Path) -> None:
    """Host-root seed (``https://h/``) ⇒ scope = весь host: host-wide sitemap не режется."""
    locs = (f"https://{HOST}/x", f"https://{HOST}/y/z")
    poller = _locs(*locs)
    crawler = _Crawler({ROOT_HOST: b"seed", locs[0]: b"x", locs[1]: b"y"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_HOST]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    assert result.errors == []
    assert [u for u, _ in crawler.calls] == [ROOT_HOST, *locs]
    assert {d.url for d in _build_of(result).docs} == {ROOT_HOST, *locs}


def test_loc_rejected_by_root_a_still_offered_to_root_b(tmp_path: Path) -> None:
    """Loc вне scope корня A не «съедается» дедупом и достаётся корню B того же продукта."""
    poller = _locs(LOC_B_IN)
    crawler = _Crawler({ROOT_A: b"a", ROOT_B: b"b", LOC_B_IN: b"loc"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_A, ROOT_B]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    # Sitemap опрошен один раз на продукт (кэш ``(pid, sitemap_url)``), loc — у корня B.
    assert [c[0] for c in poller.calls] == [SITEMAP_URL]
    assert [u for u, _ in crawler.calls] == [ROOT_A, ROOT_B, LOC_B_IN]
    # Отказ корня A наблюдаем (именно он, не корень B).
    assert any(
        LOC_B_IN in err and "вне scope корня" in err and ROOT_A in err
        for err in result.errors
    )
    assert not any(f"корня {ROOT_B}" in err for err in result.errors)


def test_same_loc_offered_by_two_roots_fetched_once(tmp_path: Path) -> None:
    """Loc в scope обоих корней: дедуп ``(pid, loc)`` сохраняет один фетч."""
    poller = _locs(LOC_A_IN)
    crawler = _Crawler({ROOT_HOST: b"host", ROOT_A: b"a", LOC_A_IN: b"loc"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_HOST, ROOT_A]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    assert result.errors == []
    assert [u for u, _ in crawler.calls].count(LOC_A_IN) == 1
    assert {d.url for d in _build_of(result).docs} == {ROOT_HOST, ROOT_A, LOC_A_IN}


def test_no_sitemap_never_calls_poller(tmp_path: Path) -> None:
    """Без sitemap путь не исполняется: ни обращений к поллеру, ни новых seeds/errors."""
    poller = _FakePoller(raising=RuntimeError("поллер не должен вызываться"))
    crawler = _Crawler({ROOT_A: b"seed"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_A]},
        crawler=crawler,
        domain_profiles={HOST: DomainProfile()},
        pollers={"sitemap": poller},
    )
    assert poller.calls == []
    assert result.errors == []
    assert [u for u, _ in crawler.calls] == [ROOT_A]
    assert {d.url for d in _build_of(result).docs} == {ROOT_A}
