"""Очередь передачи «документ добыт → документ сконвертирован» (17 §4.1, F5).

**Зачем отдельный модуль.** Очередь — единственный hand-off между краулом и конверсией
(план ``17``, F5): краул пишет строку, потребитель переводит её в терминальное состояние.
Оба конца обязаны говорить об одном носителе, но ни один не вправе знать о внутренностях
другого: краул не конвертирует, потребитель не краулит. Модуль — этот общий контракт.

**Два носителя, одно поведение.** При активном сеансе краула очередь durable — таблица
``extract_queue`` (:meth:`StorePort.enqueue_extract_item`/``iter_extract_queue``/
``set_extract_state``). Прогон **без сеанса** (``store``/``run_id`` не заданы — durable-краул
выключен, ``test_store_none_with_run_id_degrades_cleanly``) не имеет и этого субстрата:
тогда тот же контракт живёт в памяти процесса. Это не второй путь конверсии — потребитель,
порядок и состояния одни и те же; различается только носитель факта «документ добыт»,
который без сеанса и не мог бы стать durable (некуда адресовать).

**Порядок — содержимого, а не расписания.** Чтение идёт в порядке ``(product_id, url)``
(``iter_extract_queue`` документирует это же). Память сортирует тем же ключом, поэтому
потребитель на обоих носителях видит одну последовательность: иначе два прогона с одним
набором документов дали бы разный порядок записи корпуса — то есть сравнение артефактов
показывало бы расписание, а не состав.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from core.domain import ExtractQueueItem, ExtractQueueState, ReuseKind
from core.pipeline.corpus_persist import _now_iso

#: Терминальные состояния строки: работа по ней закрыта (``done|failed|skipped``).
TERMINAL_QUEUE_STATES = frozenset(
    {ExtractQueueState.DONE, ExtractQueueState.FAILED, ExtractQueueState.SKIPPED}
)


def queue_key(item: ExtractQueueItem) -> tuple[str, str, str]:
    """Ключ строки очереди — PK таблицы ``(session_id, product_id, url)``."""
    return (item.session_id, item.product_id, item.url)


class ExtractQueue:
    """Очередь документа в конверсию: durable при сеансе, память без него.

    Один экземпляр на фазу. ``put`` идемпотентен по ключу (повтор фетча того же URL не
    создаёт вторую строку и **не** перезаписывает состояние, выставленное потребителем:
    ``done`` не возвращается в ``pending``). ``set_state`` — условный переход: при
    ``expect`` строка переходит только из ожидаемого состояния.
    """

    def __init__(self, *, store: Any = None, session_id: str | None = None) -> None:
        #: Durable-склад (``None`` — сеанса нет, очередь живёт в памяти).
        self._store = store
        self._session_id = session_id
        #: Память: ключ ``(product_id, url)`` → строка. Пусто при durable-носителе.
        self._rows: dict[tuple[str, str], ExtractQueueItem] = {}
        #: Строки, сконвертированные **этим** объектом очереди (то есть в этой фазе).
        #: Нужен, чтобы терминальное состояние прошлого запуска не выглядело как «уже
        #: сконвертировано в этом»: тот же ``run_id`` переисполняется (resume/``--reset``),
        #: и новый Pass B обязан получить документы заново, а строка ``done`` о прошлом
        #: запуске — не повод построить пустую партицию.
        self._converted: set[tuple[str, str]] = set()
        #: Происхождение наблюдения **этого** исполнения по ключу строки (16 §3.3).
        #: Durable-строка его не несёт и не может: ``live``/``revalidated``/``reused`` —
        #: факт цикла, а не документа (у resume тот же адрес и тот же ``status_code``,
        #: что у первого фетча). Краул знает его в момент ``put`` — здесь он и остаётся,
        #: чтобы потребитель не выводил происхождение из истории строки.
        self._cycle_reuse: dict[tuple[str, str], ReuseKind] = {}
        #: ``(product_id, work_url) → url строки``: адрес строки — финальный URL ответа
        #: (редирект), а повторный ``put`` того же факта приходит по запрошенному адресу.
        #: Без этой пары один факт дал бы две строки. Остаточное, названное: если строка
        #: записана **прошлым** исполнением под финальным URL, а это исполнение возобновило
        #: страницу по запрошенному (``resume_page`` финального адреса не знает), пара
        #: отсутствует и появится вторая строка; документ при этом не удваивается —
        #: ``corpus_docs`` адресуется ``product_id:doc_hash``.
        self._by_work: dict[tuple[str, str], str] = {}
        #: Обратная карта: адрес строки → адрес работы краула. Потребителю нужен именно
        #: адрес работы для ``mark``/``url_state`` (аудит и инкрементальный план адресуются
        #: запрошенным URL), тогда как ``corpus_docs.url`` — финальный адрес документа.
        self._work_by_row: dict[tuple[str, str], str] = {}

    @property
    def durable(self) -> bool:
        """``True`` — строки лежат в ``extract_queue`` (есть адресуемый сеанс)."""
        return self._store is not None and self._session_id is not None

    # -- запись (краул) ---------------------------------------------------- #
    def put(
        self,
        item: ExtractQueueItem,
        *,
        work_url: str | None = None,
        reuse_kind: ReuseKind | None = None,
    ) -> None:
        """Пишет факт «документ добыт» **идемпотентно** по ключу строки.

        ``reuse_kind`` — происхождение наблюдения **этого** исполнения (краул знает его
        из пути страницы: resume ⇒ ``reused``, ``304`` ⇒ ``revalidated``, живой фетч ⇒
        ``live``). На носителе строки он не пишется: строка durable и одна на сеанс, а
        происхождение принадлежит циклу.
        """
        key = (item.product_id, item.url)
        if reuse_kind is not None:
            self._cycle_reuse[key] = reuse_kind
        if work_url is not None:
            known = self._by_work.get((item.product_id, work_url))
            if known is not None and known != item.url:
                item = item.model_copy(update={"url": known})
            self._by_work[(item.product_id, work_url)] = item.url
            self._work_by_row.setdefault((item.product_id, item.url), work_url)
        key = (item.product_id, item.url)
        if self.durable:
            self._store.enqueue_extract_item(item)
            return
        if key not in self._rows:
            self._rows[key] = item

    def work_url(self, item: ExtractQueueItem) -> str:
        """Адрес работы краула для строки (``mark``/``url_state``); по умолчанию — адрес строки.

        Строка адресует **документ** (финальный URL ответа), а аудит сеанса и
        ``url_state`` — **запрошенный** URL: по нему строится следующий инкрементальный
        план и условный GET. Без этого различия ``mark`` создал бы второй элемент сеанса
        под финальным адресом (``crawl_fetched`` удвоился бы), а валидаторы легли бы под
        адрес, который план никогда не спросит. Остаточное, названное: карта строится в
        этом исполнении (``put``); для строки, которую это исполнение не наблюдало,
        возвращается адрес строки.
        """
        return self._work_by_row.get((item.product_id, item.url), item.url)

    def cycle_reuse_kind(self, item: ExtractQueueItem) -> ReuseKind | None:
        """Происхождение наблюдения строки в этом исполнении (``None`` — не наблюдалась)."""
        return self._cycle_reuse.get((item.product_id, item.url))

    def close(
        self,
        product_id: str,
        url: str,
        state: ExtractQueueState,
        *,
        error_kind: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Закрыть строку **без конверсии** (``SKIPPED``): краул сказал «конвертировать нечего».

        Нужно краулу: терминальный отказ источника (``payload_too_large``/
        ``unsupported_binary``/``not_found`` — ``terminal_failure_kind``) означает, что
        retained-байтов по этому адресу нет, а строка, оставшаяся ``pending``/``done``,
        обещала бы потребителю существующий файл. Это **не** состояние потребителя
        (``done``/``failed`` конверсии), а закрытие собственного факта краула, поэтому
        ``error_kind`` строки называет отказ источника, а не класс сбоя извлечения.
        """
        if self.durable:
            return bool(
                self._store.set_extract_state(
                    self._session_id,  # type: ignore[arg-type]
                    product_id,
                    url,
                    state,
                    error_kind=error_kind,
                    error=error,
                )
            )
        key = (product_id, url)
        current = self._rows.get(key)
        if current is None:
            return False
        self._rows[key] = current.model_copy(
            update={
                "state": state,
                "state_changed_at": _now_iso(),
                "error_kind": error_kind,
                "error": error,
            }
        )
        return True

    # -- чтение (потребитель) ---------------------------------------------- #
    def items(self, product_id: str | None = None) -> Iterator[ExtractQueueItem]:
        """Строки в каноническом порядке ``(product_id, url)``, опционально по продукту."""
        if self.durable:
            for item in self._store.iter_extract_queue(self._session_id):
                if product_id is None or item.product_id == product_id:
                    yield item
            return
        for key in sorted(self._rows):
            item = self._rows[key]
            if product_id is None or item.product_id == product_id:
                yield item

    def counts(self) -> dict[str, int]:
        """Число строк по состояниям (только непустые), как ``count_extract_queue``."""
        if self.durable:
            return dict(self._store.count_extract_queue(self._session_id))
        counts: dict[str, int] = {}
        for item in self._rows.values():
            state = str(item.state.value)
            counts[state] = counts.get(state, 0) + 1
        return dict(sorted(counts.items()))

    def pending(self, product_id: str | None = None) -> int:
        """Сколько строк ещё не закрыто (``pending``/``claimed``) — барьер фазы.

        Именно это число, а не «осталось документов в памяти», отвечает на вопрос «конверсия
        продукта закончена?»: незакрытая строка означает, что документ добыт и лежит в
        ``corpus_raw``, но в корпус не попал.
        """
        return sum(
            1
            for item in self.items(product_id)
            if item.state not in TERMINAL_QUEUE_STATES
        )

    def mark_converted(self, item: ExtractQueueItem) -> None:
        """Отмечает строку как сконвертированную **в этой фазе** (идемпотентность вызова)."""
        self._converted.add((item.product_id, item.url))

    def was_converted(self, item: ExtractQueueItem) -> bool:
        """``True`` — строка уже конвертирована этим объектом очереди (повтор вызова)."""
        return (item.product_id, item.url) in self._converted

    # -- состояние (потребитель) ------------------------------------------- #
    def set_state(
        self,
        item: ExtractQueueItem,
        state: ExtractQueueState,
        *,
        expect: ExtractQueueState | None = None,
        claimed_by: str | None = None,
        error_kind: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Условный переход строки; ``False`` — строка не перешла (закрыта/забрана).

        Поля краула не переписываются ни на одном носителе: меняется только то, чем
        владеет потребитель (состояние и его след).
        """
        if self.durable:
            return bool(
                self._store.set_extract_state(
                    item.session_id,
                    item.product_id,
                    item.url,
                    state,
                    expect=expect,
                    claimed_by=claimed_by,
                    error_kind=error_kind,
                    error=error,
                )
            )
        key = (item.product_id, item.url)
        current = self._rows.get(key)
        if current is None:
            return False
        if expect is not None and current.state != expect:
            return False
        self._rows[key] = current.model_copy(
            update={
                "state": state,
                "state_changed_at": _now_iso(),
                "claimed_by": claimed_by or current.claimed_by,
                "error_kind": error_kind,
                "error": error,
            }
        )
        return True


__all__ = ["TERMINAL_QUEUE_STATES", "ExtractQueue", "queue_key"]