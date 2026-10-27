"""Порт внешнего поиска — ``SearchAdapterPort`` (Ш5, I19/I23).

Гарантия I19: выход — только URL+байты/кандидаты, **не evidence**. Превращение в evidence
происходит в Ш5 (chunk+hash+index). Вызовы логирует вызывающий pipeline в ``search_log`` (I23).

В профиле ``split`` реальный адаптер исполняется в DMZ; ядро использует remote-proxy
(``ports/gateway.py``). Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from core.ports.types import RawPage


@dataclass(frozen=True)
class SearchQuery:
    """Поисковый запрос (внутренний контракт; SERP-специфика — в адаптере)."""

    text: str
    lang: str = "ru"
    limit: int = 10
    phase: str = "corpus"


@dataclass(frozen=True)
class SearchResult:
    """Кандидат из внешнего поиска. ``raw_bytes`` — для retention в ``corpus_raw`` (I19)."""

    url: str
    title: str = ""
    snippet: str = ""
    rank: int = 0
    raw_bytes: bytes | None = None


@dataclass(frozen=True)
class SearchTiming:
    """Наблюдённое время **одного** поискового вызова, с (S4).

    Ожидание внешней системы и сами запросы — **разные** факты, поэтому они разделены:

    * ``wait_seconds`` — время, проведённое в паузах между опросами внешней
      async-операции (это не «работа», а простой прогона);
    * ``request_seconds`` — время HTTP-вызовов провайдера (и запуск операции, и опросы),
      включая паузы backoff внутри повторов: они происходят **внутри** вызова и от
      ожидания async-операции отделяются как часть стоимости самого запроса;
    * ``polls`` — сколько раз операция опрошена (объём вызовов, а не их длительность).

    Зачем: «контур медленный» без этих чисел неотличимо от «контур ждал», а прогон,
    потративший часы на ожидание чужой системы, по отчёту не отличается от прогона в
    минуты. Числа ничего не ограничивают — они делают стоимость ожидания видимой.

    :param measured: адаптер тайминги **наблюдает**. ``False`` — вызов прошёл через шов,
        который их не передаёт (профиль ``split``): тогда нули означают «не наблюдали»,
        а не «ждали ноль секунд». Ноль у наблюдающего адаптера — факт, и он публикуется.
    :param from_cache: ответ получен из R7-кэша: живого вызова не было, ожидания не было
        по построению (сеть не задействована).
    """

    wait_seconds: float = 0.0
    request_seconds: float = 0.0
    polls: int = 0
    measured: bool = False
    from_cache: bool = False

    def merged_with(self, other: SearchTiming) -> SearchTiming:
        """Сумма двух наблюдений одного продукта.

        ``measured``/``from_cache`` — конъюнкция: сумма, где хоть одно слагаемое не
        наблюдалось, не выдаётся за измеренную (иначе неизмеренное стало бы нулём).
        """
        return SearchTiming(
            wait_seconds=self.wait_seconds + other.wait_seconds,
            request_seconds=self.request_seconds + other.request_seconds,
            polls=self.polls + other.polls,
            measured=self.measured and other.measured,
            from_cache=self.from_cache and other.from_cache,
        )


@runtime_checkable
class SearchAdapterPort(Protocol):
    """Внешний поиск как источник входного контента (Ш5). Commodity-слой."""

    adapter_kind: str
    provider: str

    def search(self, q: SearchQuery) -> list[SearchResult]: ...

    def fetch(self, url: str) -> RawPage: ...
