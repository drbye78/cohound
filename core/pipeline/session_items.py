"""Доступ к элементам сеанса краула без загрузки всей сессии в память (R1).

``prior_items`` (``core.pipeline.crawl_session``) собирает **весь** сеанс в ``dict``
``(product_id, url) -> CrawlSessionItem``. Измерено на run-DB прогона
``ru-pg-dbms-3a737f13962f`` (30 207 строк): полная карта — **107 MB RSS**
(3.63 KB/строка), тогда как ``Frontier`` на те же 30 207 URL занимает 11.6 MB.
Целевой масштаб — 100k+ URL на продукт, то есть ~360 MB на одну карту: цена растёт
линейно по сеансу, хотя горячий набор обращений к элементам — небольшая его часть.

Точечное чтение по PK стоит 22.8 мкс (замер на той же БД 23.11.2026), поэтому полный
проход по 30 207 URL — 0.7 с, а не «недопустимо дорого». Отсюда устройство: LRU поверх
точечных чтений, память O(LRU) вместо O(сессии).

Класс нейтрален (§1.3): ни продукта, ни домена — только порт хранилища и ключ.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Iterator, MutableMapping
from typing import Any

from core.domain import CrawlSessionItem

#: Сентинел «строки нет» в LRU. Отдельный объект, а не ``None`` в значении: ``None``
#: как значение словаря неотличим от «ключ отсутствует» только по ``get``, но при
#: вытеснении/обходе путаница реальна — поэтому отсутствие хранится явно.
_MISSING: Any = object()


class SessionItemIndex(MutableMapping[tuple[str, str], CrawlSessionItem]):
    """Элементы сеанса по требованию: LRU поверх точечных чтений стора.

    Зачем: ``prior_items`` грузит всю сессию в ``dict`` (измерено 107 MB на 30 207 URL).
    Здесь память — O(LRU), а не O(сессии). Точечное чтение по PK стоит 22.8 мкс
    (замер на run-DB 23.11.2026), то есть полный проход по 30 207 URL — 0.7 с.

    Поведение интерфейса ``MutableMapping``:

    * ``get``/``__getitem__`` — промах LRU идёт в стор; найденное кладётся в LRU;
      **отсутствие тоже кэшируется** (отдельным сентинелом), иначе один и тот же
      отсутствующий URL читался бы повторно на каждом обращении;
    * ``__setitem__`` — только LRU. В стор **не** пишет: персист — дело
      ``mark_session``/``upsert_crawl_session_item*``, и «положил в память» не должно
      означать «записал на диск»;
    * ``__delitem__``/``__iter__``/``__len__`` — из стора, лениво. Удаления строки
      индекс не делает (в стор не пишет), поэтому ``KeyError`` — если строки нет
      в сторе; удаление из LRU при этом выполняется;
    * ``values()``/``items()`` перекрыты итераторами по
      ``store.iter_crawl_session_items``: базовые реализации ``MutableMapping`` шли бы
      через ``__iter__`` + ``__getitem__``, то есть N точечных чтений вместо одного
      запроса. Потребитель — ``core.pipeline.corpus_commit`` (множества ``engine``/
      ``kind`` по всем персистенным элементам).

    ``session_id is None`` — durable-краул выключен: индекс пуст, ``get`` → ``None``,
    запись идёт только в LRU, в стор обращений нет вовсе.

    Потокобезопасность: краул многопоточный (``browser_workers``/``max_workers`` > 1),
    поэтому LRU (``OrderedDict`` + ``move_to_end``) живёт под ``threading.Lock``. Замок
    держится только вокруг операций над словарём; чтение стора выполняется вне его,
    чтобы медленный ``SELECT`` не сериализовал потоки. Гонка «два потока читают один
    промах» при этом безвредна: оба прочитают одно и то же, второй перезапишет первое
    тем же значением.
    """

    def __init__(
        self,
        store: Any,
        session_id: str | None,
        *,
        capacity: int = 1024,
    ) -> None:
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        self._store = store
        self._session_id = session_id
        self._capacity = capacity
        self._lru: OrderedDict[tuple[str, str], Any] = OrderedDict()
        self._lock = threading.Lock()
        #: Счётчик обращений к стору — наблюдаемость без обёрток в тестах.
        self.reads = 0

    # -- внутреннее ---------------------------------------------------------- #
    def _cache_get(self, key: tuple[str, str]) -> Any:
        """Читает LRU: значение, ``_MISSING`` или ``None`` (ключ не кэширован)."""
        with self._lock:
            if key not in self._lru:
                return None
            value = self._lru[key]
            self._lru.move_to_end(key)
            return value

    def _cache_put(self, key: tuple[str, str], value: Any) -> None:
        with self._lock:
            self._lru[key] = value
            self._lru.move_to_end(key)
            while len(self._lru) > self._capacity:
                self._lru.popitem(last=False)

    def _cache_drop(self, key: tuple[str, str]) -> None:
        with self._lock:
            self._lru.pop(key, None)

    def _load(self, key: tuple[str, str]) -> CrawlSessionItem | None:
        """Точечное чтение из стора с кэшированием (в том числе отрицательного)."""
        if self._session_id is None:
            return None
        product_id, url = key
        self.reads += 1
        item = self._store.get_crawl_session_item(self._session_id, product_id, url)
        self._cache_put(key, _MISSING if item is None else item)
        return item

    # -- MutableMapping ------------------------------------------------------ #
    def __getitem__(self, key: tuple[str, str]) -> CrawlSessionItem:
        cached = self._cache_get(key)
        if isinstance(cached, CrawlSessionItem):
            return cached
        if cached is _MISSING:
            raise KeyError(key)
        item = self._load(key)
        if item is None:
            raise KeyError(key)
        return item

    def get(
        self, key: tuple[str, str], default: Any = None
    ) -> CrawlSessionItem | Any:
        cached = self._cache_get(key)
        if isinstance(cached, CrawlSessionItem):
            return cached
        if cached is _MISSING:
            return default
        item = self._load(key)
        return default if item is None else item

    def __setitem__(self, key: tuple[str, str], value: CrawlSessionItem) -> None:
        self._cache_put(key, value)

    def __delitem__(self, key: tuple[str, str]) -> None:
        # Удаление — операция над стором; индекс лишь перестаёт держать устаревшую копию.
        if self._session_id is None or self._store.get_crawl_session_item(
            self._session_id, key[0], key[1]
        ) is None:
            raise KeyError(key)
        self._cache_drop(key)

    def __iter__(self) -> Iterator[tuple[str, str]]:
        if self._session_id is None:
            return iter(())
        # Материализуем ключи: итератор стора держит курсор, а потребитель может
        # обратиться к индексу внутри цикла — один запрос всё равно дешевле N чтений.
        return iter(
            [
                (item.product_id, item.url)
                for item in self._store.iter_crawl_session_items(self._session_id)
            ]
        )

    def __len__(self) -> int:
        if self._session_id is None:
            return 0
        return sum(1 for _ in self._store.iter_crawl_session_items(self._session_id))

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, tuple) or len(key) != 2:
            return False
        return self.get(key) is not None

    def values(self) -> Iterator[CrawlSessionItem]:  # type: ignore[override]
        """Все элементы сеанса одним запросом к стору (не N точечных чтений)."""
        if self._session_id is None:
            return iter(())
        return self._store.iter_crawl_session_items(self._session_id)

    def items(self) -> Iterator[tuple[tuple[str, str], CrawlSessionItem]]:  # type: ignore[override]
        """Пары ``((product_id, url), item)`` одним запросом к стору."""
        if self._session_id is None:
            return iter(())
        return (
            ((item.product_id, item.url), item)
            for item in self._store.iter_crawl_session_items(self._session_id)
        )
