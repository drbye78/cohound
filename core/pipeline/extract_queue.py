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

import dataclasses
import datetime as _dt
import shutil
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from core.domain import ExtractQueueItem, ExtractQueueState, FailureKind, ReuseKind
from core.pipeline.corpus_persist import _now_iso

#: Терминальные состояния строки: работа по ней закрыта (``done|failed|skipped``).
TERMINAL_QUEUE_STATES = frozenset(
    {ExtractQueueState.DONE, ExtractQueueState.FAILED, ExtractQueueState.SKIPPED}
)

#: Порог аренды строки очереди: сколько строка может оставаться ``claimed`` **без**
#: heartbeat, прежде чем потребитель считается мёртвым (17 §4.2 п.3, F6).
#:
#: Значение опубликовано и выведено из замера, а не назначено «на глаз». Самый дорогой
#: измеренный режим извлечения — ``layout_tables`` на CPU: **1.49 с/стр**
#: (``docs/reports/measurements/2026-10-28-gpu-vs-cpu-extraction.md``), поэтому документ
#: на 1200 страниц занимает ≈1788 с. Живой потребитель **обязан** продлевать аренду
#: (``heartbeat_at``), поэтому порог отвечает не за «долгий документ», а за «воркер
#: молчит», и 3600 с дают вдвое больше самого длинного измеренного документа: за это время
#: пропущенный heartbeat означает мёртвый процесс, а не медленный парсер.
#:
#: Порог — **операционная** константа, а не ось: он не входит в ``CrawlLimits.as_dict()`` и
#: не меняет состав корпуса (кто именно доработает строку — расписание, а не измерение).
EXTRACT_CLAIM_LEASE_S = 3600.0

#: Период heartbeat живого потребителя: втрое меньше порога аренды, чтобы один пропущенный
#: tick не превращал живого воркера в мёртвого.
EXTRACT_HEARTBEAT_S = 1200.0

#: Сколько аренд строка переживает, прежде чем станет терминально ``failed`` (§4.2 п.5).
#: Три — «два воркера умерли на этом документе»: третий повтор уже не про удачу, а про
#: дефект, и строка обязана назвать класс сбоя вместо бесконечного воскрешения.
EXTRACT_MAX_ATTEMPTS = 3

#: Класс сбоя исчерпания попыток — канонический ``FailureKind``, а не свободная строка.
EXTRACT_ATTEMPTS_ERROR_KIND = FailureKind.ATTEMPTS_EXHAUSTED.value

#: Предел ожидания backpressure (17 §4.3, F6), секунды. Пока backlog не спадает, приём
#: приостановлен; ожидание ограничено, иначе «краул без потребителя» висел бы вечно. По
#: истечении приём останавливается **видимо** (запись ``crawl_backpressure`` и сеанс
#: ``partial`` с причиной ``extract_backlog_full``), а не молча.
EXTRACT_BACKLOG_WAIT_S = 300.0

#: Шаг опроса backlog во время ожидания, секунды (операционная константа).
EXTRACT_BACKLOG_POLL_S = 5.0


@dataclasses.dataclass(frozen=True)
class BacklogReport:
    """Наблюдённый backlog очереди (17 §4.3): байты, файлы, свободное место, предел.

    ``bytes`` — Σ размеров **retained-файлов** строк ``pending``+``claimed``: это
    доминирующая статья раскладки (измерено: 4.5 GB на 2 109 файлах ≈2.1 MB/файл,
    ``docs/reports/measurements/2026-10-29-corpus-retention-footprint.md``). ``missing`` —
    строки, чей файл не найден: это **нарушение** I19 (строка обещала байты), и число
    публикуется, а не замалчивается. ``free_bytes`` — свободно на носителе retention;
    ``None`` — носитель неизвестен, и тогда предел не применяется: сравнивать нечего.
    """

    bytes: int = 0
    files: int = 0
    missing: int = 0
    rows: dict[str, int] = dataclasses.field(default_factory=dict)
    free_bytes: int | None = None
    limit_bytes: int | None = None

    def as_dict(self) -> dict[str, Any]:
        """Машинная форма для payload'ов CLI/событий (стабильные ключи)."""
        return {
            "backlog_bytes": self.bytes,
            "backlog_files": self.files,
            "backlog_missing": self.missing,
            "queue_rows": dict(sorted(self.rows.items())),
            "free_bytes": self.free_bytes,
            "backlog_limit_bytes": self.limit_bytes,
        }

    @property
    def over_limit(self) -> bool:
        """Предел достигнут (``limit_bytes`` не объявлен — не достигнут никогда)."""
        return self.limit_bytes is not None and self.bytes >= self.limit_bytes


def _dir_free_bytes(root: str | Path | None) -> int | None:
    """Свободное место на носителе retention; ``None`` — носитель неизвестен."""
    if root is None:
        return None
    try:
        return int(shutil.disk_usage(str(root)).free)
    except OSError:
        # Каталог ещё не создан/недоступен — «неизвестно», а не «ноль свободного»: ноль
        # остановил бы приём по выдуманной причине.
        return None


def queue_key(item: ExtractQueueItem) -> tuple[str, str, str]:
    """Ключ строки очереди — PK таблицы ``(session_id, product_id, url)``."""
    return (item.session_id, item.product_id, item.url)


class ExtractQueue:
    """Очередь документа в конверсию: durable при сеансе, память без него.

    Один экземпляр на фазу. ``put`` идемпотентен по ключу (повтор фетча того же URL не
    создаёт вторую строку и **не** перезаписывает состояние, выставленное потребителем:
    ``done`` не возвращается в ``pending``). ``claim``/``heartbeat``/``close`` — аренда
    (F6): строку берёт один потребитель, живой продлевает ``heartbeat``, просроченная
    возвращается в ``pending`` с ``attempts += 1``; ``set_state`` — условный переход.
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
        expect_claimed_by: str | None = None,
        claimed_by: str | None = None,
        error_kind: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Условный переход строки; ``False`` — строка не перешла (закрыта/забрана).

        Поля краула не переписываются ни на одном носителе: меняется только то, чем
        владеет потребитель (состояние и его след). ``expect_claimed_by`` — закрывать
        строку вправе только владелец **живой** аренды (F6): просроченную аренду могли
        отдать другому, и «закрытие задним числом» объявило бы ``done`` о чужой работе.
        """
        if self.durable:
            return bool(
                self._store.set_extract_state(
                    item.session_id,
                    item.product_id,
                    item.url,
                    state,
                    expect=expect,
                    expect_claimed_by=expect_claimed_by,
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
        if (
            expect_claimed_by is not None
            and current.claimed_by != expect_claimed_by
        ):
            return False
        now = _now_iso()
        claiming = state is ExtractQueueState.CLAIMED
        self._rows[key] = current.model_copy(
            update={
                "state": state,
                "state_changed_at": now,
                "claimed_by": claimed_by or current.claimed_by,
                "claimed_at": now if claiming else current.claimed_at,
                "heartbeat_at": now if claiming else current.heartbeat_at,
                "error_kind": error_kind,
                "error": error,
            }
        )
        return True

    def claim(self, item: ExtractQueueItem, worker: str) -> bool:
        """Взять строку в работу (F6, §4.2 п.4): ``pending → claimed`` условно.

        ``False`` — строку уже взял/закрыл другой потребитель; это **не ошибка**: работа
        просто не наша, и потребитель идёт дальше (документ не конвертируется дважды).
        """
        return self.set_state(
            item,
            ExtractQueueState.CLAIMED,
            expect=ExtractQueueState.PENDING,
            claimed_by=worker,
        )

    def heartbeat(self, item: ExtractQueueItem, worker: str) -> bool:
        """Продлить живую аренду (F6, §4.2 п.3): ``heartbeat_at = now``.

        Долгий документ обязан сердцебить: без этого живой потребитель был бы признан
        мёртвым и документ пошёл бы в конвертацию повторно. ``False`` — аренда потеряна.
        """
        if self.durable:
            return bool(
                self._store.touch_extract_claim(
                    item.session_id, item.product_id, item.url, claimed_by=worker
                )
            )
        key = (item.product_id, item.url)
        current = self._rows.get(key)
        if (
            current is None
            or current.state is not ExtractQueueState.CLAIMED
            or current.claimed_by != worker
        ):
            return False
        self._rows[key] = current.model_copy(update={"heartbeat_at": _now_iso()})
        return True

    def reclaim_stale(
        self,
        *,
        now: str | None = None,
        lease_s: float = EXTRACT_CLAIM_LEASE_S,
        max_attempts: int = EXTRACT_MAX_ATTEMPTS,
    ) -> list[dict[str, Any]]:
        """Вернуть просроченные аренды в ``pending``, назвав каждый факт (§4.2 п.3).

        ``now`` передаётся явно (тест просрочки не платит реальным ожиданием); порог —
        ``lease_s`` от ``now``. Возвращает список фактов для отчёта потребителя: число
        восстановленных, кто держал аренду, сколько раз и не исчерпаны ли попытки.
        """
        now_iso = now or _now_iso()
        older_than = _iso_minus_seconds(now_iso, lease_s)
        if self.durable:
            return list(
                self._store.reclaim_stale_extract_claims(
                    self._session_id,  # type: ignore[arg-type]
                    older_than=older_than,
                    now=now_iso,
                    max_attempts=max_attempts,
                    error_kind=EXTRACT_ATTEMPTS_ERROR_KIND,
                )
            )
        reclaimed: list[dict[str, Any]] = []
        for key in sorted(self._rows):
            item = self._rows[key]
            if item.state is not ExtractQueueState.CLAIMED:
                continue
            seen = item.heartbeat_at or item.claimed_at or ""
            if not seen or seen >= older_than:
                continue
            attempts = item.attempts + 1
            if attempts >= max_attempts:
                self._rows[key] = item.model_copy(
                    update={
                        "state": ExtractQueueState.FAILED,
                        "attempts": attempts,
                        "state_changed_at": now_iso,
                        "claimed_by": None,
                        "claimed_at": None,
                        "heartbeat_at": None,
                        "error_kind": EXTRACT_ATTEMPTS_ERROR_KIND,
                        "error": (
                            f"claim expired {attempts} time(s) "
                            f"(max_attempts={max_attempts})"
                        ),
                    }
                )
            else:
                self._rows[key] = item.model_copy(
                    update={
                        "state": ExtractQueueState.PENDING,
                        "attempts": attempts,
                        "state_changed_at": now_iso,
                        "claimed_by": None,
                        "claimed_at": None,
                        "heartbeat_at": None,
                        "error_kind": None,
                        "error": (
                            f"claim expired after {attempts} attempt(s): last owner "
                            f"{item.claimed_by or '—'} stopped heartbeating"
                        ),
                    }
                )
            reclaimed.append(
                {
                    "product_id": item.product_id,
                    "url": item.url,
                    "attempts": attempts,
                    "claimed_by": item.claimed_by,
                    "exhausted": attempts >= max_attempts,
                }
            )
        return reclaimed

    def backlog(
        self, *, limit_mb: int | None = None, raw_root: str | Path | None = None
    ) -> BacklogReport:
        """Backlog по диску: Σ retained-файлов строк ``pending``+``claimed`` (§4.3).

        Размер берётся у **файла**, а не из строки: колонки размера в очереди нет, а второй
        носитель того же числа разошёлся бы с диском при первом же удалении/подмене.
        Пропавший файл считается отдельно (``missing``): строка ``pending`` обещала байты,
        и молчать об этом нельзя (I19).
        """
        rows: dict[str, int] = {}
        total = 0
        files = 0
        missing = 0
        for item in self.items():
            state = str(item.state.value)
            rows[state] = rows.get(state, 0) + 1
            if item.state in TERMINAL_QUEUE_STATES:
                continue
            path = Path(item.raw_path) if item.raw_path else None
            if path is None or not path.exists():
                missing += 1
                continue
            try:
                total += int(path.stat().st_size)
            except OSError:
                missing += 1
                continue
            files += 1
        return BacklogReport(
            bytes=total,
            files=files,
            missing=missing,
            rows=rows,
            free_bytes=_dir_free_bytes(raw_root),
            limit_bytes=None if limit_mb is None else int(limit_mb) * 1024 * 1024,
        )


def _iso_minus_seconds(now_iso: str, seconds: float) -> str:
    """``now - seconds`` в том же ISO-формате (UTC, ``+00:00``) — порог просрочки.

    Сравнение в SQL идёт по строкам, поэтому обе величины обязаны быть в одной форме;
    ``_now_iso`` её и даёт. Неразборчивая метка — не повод пропустить аренду молча:
    возвращается сама метка, и строка остаётся ``claimed`` до следующего круга.
    """
    try:
        moment = _dt.datetime.fromisoformat(now_iso)
    except ValueError:
        return now_iso
    return (moment - _dt.timedelta(seconds=seconds)).isoformat()


@dataclasses.dataclass(frozen=True)
class BackpressureEvent:
    """Запись ``crawl_backpressure`` (17 §4.3): числа, а не «стало тесно».

    ``waited_s`` — сколько ждали; ``gave_up`` — ожидание исчерпано и приём остановлен
    (тогда сеанс закрывается ``partial`` с причиной ``extract_backlog_full``). Событие
    публикуется и при успешном ожидании: «ждали N с, backlog упал» — тоже факт прогона.
    """

    report: BacklogReport
    waited_s: float
    gave_up: bool

    def as_dict(self) -> dict[str, Any]:
        """Машинная форма записи; ``event`` — стабильный маркер для читателей."""
        payload: dict[str, Any] = {"event": "crawl_backpressure"}
        payload.update(self.report.as_dict())
        payload["waited_s"] = round(self.waited_s, 3)
        payload["gave_up"] = self.gave_up
        return payload


def wait_for_backlog(
    queue: ExtractQueue,
    *,
    limit_mb: int | None,
    raw_root: str | Path | None,
    wait_s: float = EXTRACT_BACKLOG_WAIT_S,
    poll_s: float = EXTRACT_BACKLOG_POLL_S,
    sleep: Callable[[float], None] | None = None,
    clock: Callable[[], float] | None = None,
) -> BackpressureEvent | None:
    """Ждать, пока backlog опустится ниже объявленного предела (17 §4.3, F6).

    ``None`` — предел не объявлен либо backlog уже ниже предела: ждать нечего, и запись не
    публикуется (иначе каждый прогон обрастал бы событием «0 байт < предел»).

    Ожидание ограничено ``wait_s`` и опрашивает с шагом ``poll_s``; ``sleep``/``clock``
    инъецируемы, поэтому тест T8 не платит реальным временем. По истечении — ``gave_up``:
    вызывающий обязан остановить приём **видимо** (сессия ``partial``, причина
    ``extract_backlog_full``), а не притвориться, что корпус полон.
    """
    if limit_mb is None:
        return None
    report = queue.backlog(limit_mb=limit_mb, raw_root=raw_root)
    if not report.over_limit:
        return None
    sleeper = sleep if sleep is not None else _default_sleep
    ticker = clock if clock is not None else _monotonic
    started = ticker()
    waited = 0.0
    while True:
        sleeper(poll_s)
        waited = ticker() - started
        report = queue.backlog(limit_mb=limit_mb, raw_root=raw_root)
        if not report.over_limit:
            return BackpressureEvent(report=report, waited_s=waited, gave_up=False)
        if waited >= wait_s:
            return BackpressureEvent(report=report, waited_s=waited, gave_up=True)


def _default_sleep(seconds: float) -> None:
    time.sleep(seconds)


def _monotonic() -> float:
    return time.monotonic()


__all__ = [
    "EXTRACT_ATTEMPTS_ERROR_KIND",
    "EXTRACT_BACKLOG_POLL_S",
    "EXTRACT_BACKLOG_WAIT_S",
    "EXTRACT_CLAIM_LEASE_S",
    "EXTRACT_HEARTBEAT_S",
    "EXTRACT_MAX_ATTEMPTS",
    "TERMINAL_QUEUE_STATES",
    "BacklogReport",
    "BackpressureEvent",
    "ExtractQueue",
    "queue_key",
    "wait_for_backlog",
]