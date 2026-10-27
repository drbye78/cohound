"""Таксономия сбоев краула живёт в порту, а не в адаптере — и остаётся одна (R5).

Проверяется не «код работает», а **место** контракта и его сохранность:

* гейт ``selfcheck._crawl_taxonomy_from_port`` проходит на дереве и **падает** на
  возврате старого импорта (проверяется подачей заведомо нарушающего файла — иначе
  зелёный гейт означал бы только «на чистом дереве молчит»);
* многострочный ``from ... import (...)`` тоже ловится: грепп по строке его не видит,
  поэтому гейт разбирает ``ast``;
* адаптеры выставляют **те же объекты**, что порт (одна иерархия, а не копии);
* ``CrawlUnavailable`` — один тип: до R5 ``crawl_http`` и ``crawl_browser`` объявляли
  по своему классу с этим именем, и ``except`` одного не ловил другой;
* ``crawl_probe_driver`` различает «движок недоступен» **типом**, а не сравнением
  ``type(exc).__name__`` (так было компромиссом, пока класс жил в адаптере).
"""

from __future__ import annotations

import selfcheck

from core.adapters import crawl_browser, crawl_http, crawl_scrapy
from core.pipeline.crawl_probe_driver import _is_unavailable
from core.ports.crawl_errors import (
    NOT_FOUND_STATUSES,
    CrawlError,
    CrawlNotFound,
    CrawlPayloadTooLarge,
    CrawlRefused,
    CrawlUnavailable,
    CrawlUrlError,
)


def test_gate_passes_on_the_tree() -> None:
    """Реальное дерево: таксономию не тянут из адаптеров."""
    selfcheck._crawl_taxonomy_from_port()


def test_gate_trips_on_single_line_legacy_import(tmp_path) -> None:
    """Возврат старого импорта обязан падать, а не проходить молча."""
    legacy = tmp_path / "legacy_single.py"
    legacy.write_text(
        "from core.adapters.crawl_scrapy import CrawlError\n", encoding="utf-8"
    )
    violations = selfcheck._crawl_taxonomy_violations(
        [legacy], root=selfcheck.ROOT
    )
    assert violations and "CrawlError" in violations[0]


def test_gate_trips_on_multiline_legacy_import(tmp_path) -> None:
    """Многострочная форма — та, которую грепп по строке не поймал бы."""
    legacy = tmp_path / "legacy_multi.py"
    legacy.write_text(
        "from core.adapters.crawl_browser import (\n    BROWSER,\n    CrawlUnavailable,\n)\n",
        encoding="utf-8",
    )
    violations = selfcheck._crawl_taxonomy_violations([legacy], root=selfcheck.ROOT)
    assert violations and "CrawlUnavailable" in violations[0]


def test_gate_trips_on_redeclaration(tmp_path) -> None:
    """Повторное объявление класса вне порта — тот самый дрейф (два ``CrawlUnavailable``)."""
    dup = tmp_path / "dup.py"
    dup.write_text(
        "from core.ports.crawl_errors import CrawlError\n\n\n"
        "class CrawlUnavailable(CrawlError):\n    pass\n",
        encoding="utf-8",
    )
    violations = selfcheck._crawl_taxonomy_violations([dup], root=selfcheck.ROOT)
    assert violations and "объявлен CrawlUnavailable" in violations[0]


def test_adapters_expose_the_port_objects() -> None:
    """Одна иерархия на всех транспортах: адаптеры импортируют, а не копируют."""
    assert crawl_scrapy.CrawlError is CrawlError
    assert crawl_scrapy.CrawlNotFound is CrawlNotFound
    assert crawl_http.CrawlUnavailable is CrawlUnavailable
    assert crawl_browser.CrawlUnavailable is CrawlUnavailable


def test_adapters_do_not_advertise_taxonomy_in_all() -> None:
    """``__all__`` адаптера не предлагает путь «возьмите исключение у реализации»."""
    for module in (crawl_scrapy, crawl_http, crawl_browser):
        leaked = sorted(name for name in module.__all__ if name in selfcheck._CRAWL_TAXONOMY)
        assert not leaked, f"{module.__name__}.__all__ всё ещё рекламирует {leaked}"


def test_hierarchy_and_machine_codes_are_preserved() -> None:
    """Перенос классов не изменил контракт: наследственность и машинные коды."""
    assert issubclass(CrawlUrlError, CrawlError)
    assert issubclass(CrawlUrlError, ValueError)  # validate_url поднимает ValueError
    assert issubclass(CrawlRefused, CrawlError)
    assert issubclass(CrawlNotFound, CrawlError)
    assert issubclass(CrawlPayloadTooLarge, CrawlError)
    assert issubclass(CrawlUnavailable, CrawlError)

    assert CrawlNotFound("gone").failure_kind == "not_found"
    assert CrawlPayloadTooLarge("big").failure_kind == "payload_too_large"
    assert CrawlRefused("robots").refusal_reason == "robots_disallow"
    assert CrawlError("boom", http_status=429).http_status == 429
    assert CrawlUnavailable("no engine").http_status is None
    assert NOT_FOUND_STATUSES == frozenset({404, 410})


def test_probe_driver_recognises_engine_failure_by_type() -> None:
    """Признак «движок недоступен» — тип из порта, а не совпадение имени класса."""
    assert _is_unavailable(CrawlUnavailable("нет playwright")) is True
    assert _is_unavailable(CrawlError("источник упал")) is False

    # Посторонний класс с тем же именем: сравнение по имени дало бы True.
    foreign = type("CrawlUnavailable", (Exception,), {})
    assert _is_unavailable(foreign("чужой")) is False
