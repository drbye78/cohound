"""Порядок frontier — ключ ``(depth, url)`` и бюджет на изъятии (F3 плана 17, A1/A2).

Чистые тесты, без сети и стора. Проверяется **семантика краула** (``crawl-2``):

* :meth:`Frontier.next` отдаёт минимальный ``(depth, url)``, а не порядок поступления —
  значит «что добыто» перестаёт зависеть от момента готовности страниц;
* ``offer`` одних и тех же ссылок в любом порядке даёт один и тот же порядок изъятия;
* ``max_documents`` решается **на изъятии**: постановка его не проверяет (иначе набор
  поставленного был бы функцией планировщика), а отказ на изъятии **помечает** усечение;
* ревалидация (depth-0 листья известных URL) бюджет не расходует и продолжает
  обслуживаться при исчерпанном бюджете discovery (``09`` §6.3).
"""

from __future__ import annotations

from core.pipeline.crawl_frontier import CrawlLimits, Frontier

ROOT = "https://h/"


def _drain(frontier: Frontier) -> list[tuple[str, int]]:
    """Изъять всё, что отдаёт :meth:`Frontier.next`, учитывая каждую добытую страницу.

    ``note_fetched`` вызывается как в дренаже: страница добыта ⇒ бюджет израсходован.
    Иначе тест проверял бы изъятие без бюджета, то есть не то, что заявлено.
    """
    order: list[tuple[str, int]] = []
    while True:
        node = frontier.next()
        if node is None:
            return order
        order.append(node)
        url, _depth = node
        if not frontier.is_revalidation(url):
            frontier.note_fetched()


# --------------------------------------------------------------------------- #
# (A1) Порядок обхода — объявленный ключ, а не порядок поступления
# --------------------------------------------------------------------------- #


def test_next_is_priority_by_depth_then_url() -> None:
    """Очередь поставлена в перемешанном порядке — изъятие идёт по ``(depth, url)``."""
    frontier = Frontier(ROOT, CrawlLimits(max_depth=2))
    assert frontier.add_seed(ROOT) == "queued"
    # Обнаруженные ссылки предлагаются в порядке, который **не** совпадает ни с
    # лексикографическим, ни с порядком изъятия: при FIFO порядок был бы именно таким.
    for url in ("https://h/c", "https://h/a", "https://h/e", "https://h/b"):
        assert frontier.discover(url, 1) is True
    assert frontier.add_revalidate_seed("https://h/known") == "queued"

    order = _drain(frontier)

    # Глубина 0 (seed и ревалидационный лист) — первыми, между собой по URL; затем
    # глубина 1 по URL. Ни один элемент не потерян.
    assert order == [
        (ROOT, 0),
        ("https://h/known", 0),
        ("https://h/a", 1),
        ("https://h/b", 1),
        ("https://h/c", 1),
        ("https://h/e", 1),
    ]


def test_same_links_in_any_order_give_same_dequeue_order() -> None:
    """``offer`` одного набора ссылок в разном порядке ⇒ один порядок изъятия."""
    links = [f"https://h/page-{i}" for i in range(8)]

    def _order(offered: list[str]) -> list[tuple[str, int]]:
        frontier = Frontier(ROOT, CrawlLimits(max_depth=1))
        frontier.add_seed(ROOT)
        frontier.offer(offered, 0)
        return _drain(frontier)

    forward = _order(links)
    backward = _order(list(reversed(links)))
    shuffled = _order([links[3], links[7], links[0], links[5], links[1], links[6], links[2], links[4]])

    assert forward == backward == shuffled
    # Порядок постановки больше не наблюдается: первым идёт лексикографический минимум.
    assert [url for url, _depth in forward] == [ROOT, *links]


# --------------------------------------------------------------------------- #
# (A2) Бюджет ``max_documents`` — на изъятии, а не на постановке
# --------------------------------------------------------------------------- #


def test_budget_is_enforced_at_dequeue() -> None:
    """При ``max_documents=N`` поставлено больше N, изъято ровно N, усечение помечено.

    Постановка бюджет **не** проверяет: иначе набор поставленных URL зависел бы от
    момента готовности страниц, а с ним и набор добытых. Отказ виден: ``truncated`` с
    причиной ``max_documents`` и непустая очередь — число, а не тихая остановка.
    """
    frontier = Frontier(ROOT, CrawlLimits(max_depth=1, max_documents=2))
    assert frontier.add_seed(ROOT) == "queued"
    for i in range(6):
        assert frontier.discover(f"https://h/page-{i}", 1) is True
    assert len(frontier) == 7
    assert frontier.truncated is False  # изъятия ещё не было — усечения нет

    order = _drain(frontier)

    assert len(order) == 2  # ровно потолок, ни страницей больше
    assert [url for url, _depth in order] == [ROOT, "https://h/page-0"]
    assert frontier.truncated is True
    assert frontier.truncated_reason == "max_documents"
    assert len(frontier) == 5  # «в очереди осталось N» не потеряно


def test_offer_does_not_truncate_at_admission() -> None:
    """Набор поставленного не зависит от бюджета — в т.ч. у ссылок одной страницы."""
    frontier = Frontier(ROOT, CrawlLimits(max_depth=1, max_documents=1))
    frontier.add_seed(ROOT)
    frontier.offer([f"https://h/p{i}" for i in range(5)], 0)
    assert len(frontier) == 6
    assert frontier.truncated is False


# --------------------------------------------------------------------------- #
# (A2/09 §6.3) Ревалидация: голова и бюджет
# --------------------------------------------------------------------------- #


def test_revalidation_head_still_works() -> None:
    """Голова обслуживаемого порядка и счётчик ревалидационного блока.

    Ключ ``(depth, url)`` изменил то, что раньше было верно по построению: ревалидация
    (depth-0 лист известных URL) больше **не** обязана стоять в очереди первой — её
    место в порядке задаёт URL. Зафиксировано фактическое поведение:

    * пока бюджет discovery есть, головой может оказаться и ревалидация, и обычный
      узел — счётчик считает только **непрерывный** блок ревалидации в голове;
    * при исчерпанном бюджете discovery ревалидация всё равно обслуживается, даже если
      в очереди остались discovery-узлы с меньшим ключом: она бюджет не расходует
      (``09`` §6.3), а те всё равно недостижимы по бюджету.
    """
    frontier = Frontier(ROOT, CrawlLimits(max_depth=1, max_documents=2))
    # Ревалидационные листья — глубина 0, обнаруженная ссылка — глубина 1: голова
    # очереди ревалидация, и блок из двух листьев непрерывен.
    frontier.add_revalidate_seed("https://h/known-a")
    frontier.add_revalidate_seed("https://h/known-b")
    frontier.offer(["https://h/page"], 0)
    assert frontier.next_is_revalidation() is True
    assert frontier.revalidation_head_count(5) == 2
    assert frontier.revalidation_head_count(1) == 1

    # Обычный seed — тоже глубина 0, и по ключу он **раньше** ревалидационного листа
    # (короткая строка меньше): блок ревалидации уже не в голове, счётчик 0.
    earlier = Frontier(ROOT, CrawlLimits(max_depth=1, max_documents=2))
    earlier.add_revalidate_seed("https://h/known-a")
    earlier.add_seed(ROOT)
    assert earlier.next_is_revalidation() is False
    assert earlier.revalidation_head_count(5) == 0

    # Порядок изъятия при доступном бюджете — сквозной по ключу.
    assert _drain(frontier) == [
        ("https://h/known-a", 0),
        ("https://h/known-b", 0),
        ("https://h/page", 1),
    ]

    closed = Frontier(ROOT, CrawlLimits(max_depth=1, max_documents=1))
    closed.add_seed(ROOT)
    closed.note_fetched()  # бюджет израсходован
    closed.add_revalidate_seed("https://h/known")  # ключ (0, "https://h/known") > ROOT
    assert closed.next_is_revalidation() is True
    assert closed.revalidation_head_count(5) == 1
    assert closed.next() == ("https://h/known", 0)  # ревалидация прошла при закрытом бюджете
    assert closed.next() is None  # discovery недостижим
    assert closed.truncated is True
    assert closed.truncated_reason == "max_documents"