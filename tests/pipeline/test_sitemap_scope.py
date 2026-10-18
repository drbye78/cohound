"""Дефект B (I17): ``<loc>`` sitemap — **подчинённые** seeds корня, а не задающие scope.

Seed оператора задаёт scope корня (host + path-префикс по границе сегмента). Раньше
``<loc>`` per-site ``DomainProfile.sitemap`` ставились через ``Frontier.add_seed`` без
scope-чека, поэтому host-wide sitemap при path-scoped seed заливал в продукт-партицию
все страницы хоста — нарушение I17 (ретривал продукт-скоупед: evidence только из
паспорта продукта и его сателлитов). Гейт: ``p7-crawl-completeness-gate.md`` §2.

Проверяется: loc внутри path-scope корня фетчится; loc вне scope — не фетчится и виден
в **провенансе усечения** корня (``skipped_out_of_scope``), но **не** в ``errors``
(host-wide sitemap при path-scoped seed — штатная конфигурация, F10); классы отказа
``host`` (sitemap рекламирует другой host) и ``path`` (тот же host, путь вне префикса)
считаются раздельно (F6); отказы агрегируются одной записью на класс с точным счётчиком
(F8); host-root seed по-прежнему означает «весь host в scope»; loc, отвергнутый корнем A,
остаётся доступным корню B того же продукта; один и тот же loc двух корней фетчится не
более одного раза; без sitemap поллер не вызывается и провенанс не меняется.

Харнесс переиспользуется из ``test_corpus`` (те же детерминированные фейки портов):
``tests/pipeline`` без ``__init__.py``, поэтому pytest кладёт каталог в ``sys.path``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.config.domain_profiles import DomainProfile
from core.util.feed import FeedEntry

from test_corpus import (
    RUN_ID,
    SqliteStore,
    _Crawler,
    _FakePoller,
    _build,
    _build_of,
    _items,
    _make_run,
)

HOST = "docs.test"
SITEMAP_URL = f"https://{HOST}/sitemap.xml"

#: Path-scoped корень ``/p1/a``: соседний путь ``/p1/b/...`` — вне scope.
ROOT_A = f"https://{HOST}/p1/a"
LOC_A_IN = f"https://{HOST}/p1/a/page"
LOC_A_OUT = f"https://{HOST}/p1/b/page"

#: ``<loc>`` чужого host: sitemap рекламирует другой сайт (класс ``host``, не ``path``).
LOC_FOREIGN_HOST = "https://evil.test/p1/a/page"

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


def _root_record(result: Any, root: str) -> dict[str, Any]:
    """Провенанс-запись ровно одного seed-root продукта (``p1``)."""
    records = [rec for rec in _build_of(result).truncation if rec["root"] == root]
    assert len(records) == 1
    return records[0]


def _skips(result: Any, root: str) -> list[dict[str, Any]]:
    """Агрегированные записи отказов scope у ``<loc>`` sitemap этого корня."""
    return list(_root_record(result, root).get("skipped_out_of_scope", []))


def _by_reason(skips: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Индекс записей по классу отказа (``host``/``path``); дубликат класса — провал."""
    assert len({rec["reason"] for rec in skips}) == len(skips)
    return {rec["reason"]: rec for rec in skips}


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
    # Всё в scope ⇒ отказов нет ⇒ ключа провенанса нет (запись не «шумит»).
    assert "skipped_out_of_scope" not in _root_record(result, ROOT_A)


def test_loc_outside_path_scope_recorded_in_provenance_not_errors(tmp_path: Path) -> None:
    """``<loc>`` вне path-scope не ставится; отказ — провенанс корня, а не ``errors``."""
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
    assert {d.url for d in _build_of(result).docs} == {ROOT_A, LOC_A_IN}
    # F10: штатный отказ scope не деградирует канал сбоев и не делает корпус усечённым.
    assert result.errors == []
    assert _build_of(result).truncated is False
    # F8: отказ виден, посчитан и назван (класс + корень + сам loc).
    record = _root_record(result, ROOT_A)
    assert record["truncated"] is False
    (skip,) = _skips(result, ROOT_A)
    assert skip["reason"] == "path"
    assert skip["count"] == 1
    assert skip["examples"] == [LOC_A_OUT]
    assert LOC_A_OUT in skip["message"]
    assert "вне scope корня" in skip["message"]
    assert ROOT_A in skip["message"]
    assert "path вне префикса" in skip["message"]


def test_foreign_host_loc_counted_apart_from_path_rejections(tmp_path: Path) -> None:
    """Чужой host и path-сосед — разные классы: два счётчика, две записи, не смешаны."""
    poller = _locs(LOC_A_OUT, LOC_FOREIGN_HOST)
    crawler = _Crawler({ROOT_A: b"seed"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_A]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    assert result.errors == []
    assert [u for u, _ in crawler.calls] == [ROOT_A]
    skips = _by_reason(_skips(result, ROOT_A))
    assert set(skips) == {"host", "path"}
    # Host-класс: sitemap рекламирует другой host — отдельный сигнал, не «path-сосед».
    assert skips["host"]["count"] == 1
    assert skips["host"]["examples"] == [LOC_FOREIGN_HOST]
    assert "чужой host" in skips["host"]["message"]
    # Path-класс: тот же host, путь вне префикса корня.
    assert skips["path"]["count"] == 1
    assert skips["path"]["examples"] == [LOC_A_OUT]
    assert "path вне префикса" in skips["path"]["message"]
    assert LOC_FOREIGN_HOST not in skips["path"]["examples"]
    assert LOC_A_OUT not in skips["host"]["examples"]


def test_large_rejection_set_is_one_aggregated_record_with_exact_count(
    tmp_path: Path,
) -> None:
    """500 отвергнутых ``<loc>`` → ровно одна запись с точным счётчиком и хвостом «и ещё»."""
    out_of_scope = tuple(f"https://{HOST}/p1/b/page-{i}" for i in range(500))
    poller = _locs(*out_of_scope)
    crawler = _Crawler({ROOT_A: b"seed"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_A]},
        crawler=crawler,
        domain_profiles=_sitemap_profile(),
        pollers={"sitemap": poller},
    )
    assert result.errors == []
    assert [u for u, _ in crawler.calls] == [ROOT_A]
    (skip,) = _skips(result, ROOT_A)  # одна запись на класс, не 500
    assert skip["reason"] == "path"
    assert skip["count"] == 500
    assert skip["examples"] == list(out_of_scope[:3])
    assert "500" in skip["message"]
    assert "и ещё 497" in skip["message"]


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
    assert _skips(result, ROOT_HOST) == []


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
    # Отказ корня A наблюдаем (именно он, не корень B) — и это не ``errors``.
    assert result.errors == []
    (skip,) = _skips(result, ROOT_A)
    assert skip["reason"] == "path"
    assert skip["examples"] == [LOC_B_IN]
    assert ROOT_A in skip["message"]
    assert _skips(result, ROOT_B) == []


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
    # Дедуп — не отказ: ни у одного корня провенанса отказов нет.
    assert _skips(result, ROOT_HOST) == []
    assert _skips(result, ROOT_A) == []


# --------------------------------------------------------------------------- #
# F12: loc отвергнут корнем A (блэклист/бюджет) — не «съеден», достаётся корню B
# --------------------------------------------------------------------------- #

#: Вложенные корни: ``ROOT_INNER`` ⊂ ``ROOT_OUTER`` ⇒ ``LOC_SHARED`` в scope обоих.
ROOT_OUTER = f"https://{HOST}/p1"
ROOT_INNER = f"https://{HOST}/p1/a"
LOC_SHARED = f"https://{HOST}/p1/a/page"


def test_loc_blacklisted_at_root_a_still_offered_to_root_b(tmp_path: Path) -> None:
    """Loc заблэклистен профилем корня A — не помечен использованным, берётся корнем B.

    Блэклист резолвится по **корню** (``resolve_crawl_limits(root_url)``), поэтому вложенные
    корни могут иметь разные ``exclude_paths``: loc не выходит из scope, но у A он зачёркнут,
    у B — нет. Раньше ``(pid, loc)`` помечался сразу после scope-чека, и B его уже не получал
    (F12). Корни обходятся в лексикографическом порядке URL, поэтому A — внешний ``/p1``.
    """
    poller = _locs(LOC_SHARED)
    crawler = _Crawler({ROOT_INNER: b"a", ROOT_OUTER: b"b", LOC_SHARED: b"loc"})
    profiles = {
        f"{HOST}/p1": DomainProfile(sitemap=SITEMAP_URL, exclude_paths=["/p1/a/page"]),
        f"{HOST}/p1/a": DomainProfile(sitemap=SITEMAP_URL),
    }
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [ROOT_INNER, ROOT_OUTER]},
        crawler=crawler,
        domain_profiles=profiles,
        pollers={"sitemap": poller},
    )
    # Sitemap опрошен один раз (кэш по ``(pid, sitemap_url)``).
    assert [c[0] for c in poller.calls] == [SITEMAP_URL]
    # Корень A отказал блэклистом (видимо), корень B поставил loc и добыл его.
    assert any(
        f"{LOC_SHARED}: sitemap-seed зачёркнут блэклистом" in e
        for e in result.errors
    )
    assert [u for u, _ in crawler.calls] == [ROOT_OUTER, ROOT_INNER, LOC_SHARED]
    assert {d.url for d in _build_of(result).docs} == {
        ROOT_INNER,
        ROOT_OUTER,
        LOC_SHARED,
    }


def test_loc_budget_rejected_root_a_still_offered_to_root_b(tmp_path: Path) -> None:
    """Бюджет корня A исчерпан resume'ом — его locs не «съедены» и достаются корню B.

    ``max_documents`` host-корня A = 1, а durable-страницы A уже занимают бюджет ⇒ seed A не
    ставится (``truncated``, ранний выход). Раньше ``_fresh_sitemap_seeds`` успевал пометить
    все in-scope ``<loc>`` использованными до этой проверки, и вложенный корень B не получал
    ничего (F12). Теперь пометка — только по факту ``add_seed`` ∈ {queued, dup}.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    extra_loc = f"https://{HOST}/p1/a/extra"
    try:
        # Первый прогон: durable-страницы под /p1/a (seed + одна ссылка).
        _build(
            tmp_path,
            pages={},
            urls={"p1": [ROOT_INNER]},
            crawler=_Crawler(
                {ROOT_INNER: f'<a href="{LOC_SHARED}">x</a>'.encode(), LOC_SHARED: b"x"}
            ),
            domain_profiles={f"{HOST}/p1/a": DomainProfile(max_depth=1)},
            store=store,
            run_id=RUN_ID,
        )
        assert {i.url for i in _items(store)} >= {ROOT_INNER, LOC_SHARED}

        # Второй прогон: host-корень A (идёт первым) уже без бюджета; B — вложенный /p1/a.
        crawler = _Crawler({ROOT_INNER: b"inner", extra_loc: b"extra"})
        poller = _locs(LOC_SHARED, extra_loc)
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [ROOT_INNER, ROOT_HOST]},
            crawler=crawler,
            domain_profiles={
                HOST: DomainProfile(sitemap=SITEMAP_URL, max_documents=1),
                f"{HOST}/p1/a": DomainProfile(sitemap=SITEMAP_URL, max_documents=3),
            },
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        # A отвергнут бюджетом (seed), но loc не «съеден»: B поставил его и добыл.
        assert any(
            f"{ROOT_HOST}: seed не поставлен — достигнут max_documents" in e
            for e in result.errors
        )
        assert [u for u, _ in crawler.calls].count(extra_loc) == 1
        assert extra_loc in {d.url for d in _build_of(result).docs}
        host_record = _root_record(result, ROOT_HOST)
        assert host_record["truncated"] is True
        assert host_record["truncated_reason"] == "max_documents"
        inner_record = _root_record(result, ROOT_INNER)
        assert inner_record["truncated"] is False
    finally:
        store.close()


def test_no_sitemap_never_calls_poller(tmp_path: Path) -> None:
    """Без sitemap путь не исполняется: ни поллера, ни новых seeds/errors/провенанса."""
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
    # Без sitemap запись корня байт-в-байт прежняя: ключа провенанса отказов нет.
    assert "skipped_out_of_scope" not in _root_record(result, ROOT_A)
