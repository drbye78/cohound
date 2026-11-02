"""Владелец браузерного движка — один поток фазы корпуса (Ш5).

Дефект (измерен на живой пробе 01–02.11.2026): три корня дали ноль документов с
``CrawlError: cannot switch to a different thread``. Причина — движок Playwright создавался
в потоке одной полосы, а следующий корень обращался к нему из потока другой: sync-API
привязан к потоку-создателю.

Проверки офлайновые: настоящего Playwright и сети нет. ``_ThreadBoundBackend`` повторяет
**поведение** движка (контекст живёт в потоке-создателе), а не его API: этого достаточно,
чтобы воспроизвести отказ и увидеть, что он больше не наступает.

Тесты ниже доказывают четыре вещи: (1) браузерные фетчи когорты идут из одного потока,
который не является потоком драйвера; (2) полоса на браузерном фетче не блокирует выдачу
волн остальным; (3) владелец закрывает движок на своём потоке и повторное закрытие
безопасно; (4) три последовательных корня-полосы больше не падают.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from core.adapters.crawl_browser import BrowserCrawler, _PlaywrightBackend
from core.domain import CrawlRunConfig
from core.pipeline import corpus_drain, corpus_driver
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import LocalFetch
from core.ports.crawl_errors import CrawlError
from core.ports.types import FetchMode, FetchPlan, RawPage

#: Текст отказа реального Playwright: обращение к контексту из чужого потока.
FOREIGN_THREAD_ERROR = "cannot switch to a different thread (which happens to have exited)"

#: Имя потока-владельца: по нему движок опознаётся в ``py-spy`` и журналах.
BROWSER_THREAD_PREFIX = "cohound-browser"

#: Сколько ждать события другого потока. Гонок нет: ожидание событийное, а потолок —
#: только чтобы падение было падением, а не висящим тестом.
_WAIT_S = 5.0


class _ThreadBoundSession:
    """Сессия поверх «движка»: навигация тоже проверяет поток-владелец."""

    def __init__(self, backend: _ThreadBoundBackend) -> None:
        self._backend = backend

    def navigate(self, url: str, wait_for: str | None = None) -> tuple[str, bytes]:
        """Рендер: сначала — проверка потока (как у Playwright), потом байты."""
        self._backend._claim_thread(self._backend.navigate_threads)
        return url, b"<html>rendered</html>"

    def cookies(self, url: str) -> list[dict[str, Any]]:
        """Cookie не нужны: тесты идут honest-путём."""
        return []

    def close(self) -> None:
        """Страница закрывается вызывающим; ресурс принадлежит бэкенду."""
        return None


class _ThreadBoundBackend:
    """Фейковый Playwright: движок принадлежит потоку, который первым его открыл.

    Ровно тот отказ, что в живом прогоне: ``open``/``navigate`` из второго потока поднимают
    ``RuntimeError`` с текстом Playwright. Владелец держится **объектом** потока, а не его
    идентификатором: идентификатор вышедшего потока может достаться новому, и тогда ошибка
    «чужой поток» не воспроизвелась бы. Поэтому тест на трёх последовательных корнях
    падает на прежней схеме «пул на полосу» и проходит на схеме «один поток на фазу».
    """

    def __init__(self, *, gate: threading.Event | None = None) -> None:
        self._lock = threading.Lock()
        self._owner: threading.Thread | None = None
        #: Гейт живого рендера: держит «навигацию», пока его не отпустят снаружи.
        self._gate = gate
        #: Поток гейта истёк по таймауту (никто не отпустил) — признак блокировки когорты.
        self.gate_timed_out = False
        #: Потоки открытия сессии и навигации: по ним видно, кто владелец движка.
        self.open_threads: list[int] = []
        self.navigate_threads: list[int] = []
        self.thread_names: list[str] = []
        self.close_calls = 0
        self.close_threads: list[int] = []

    def _claim_thread(self, calls: list[int]) -> None:
        """Закрепить поток за движком; чужой (или вышедший) поток — отказ движка."""
        current = threading.current_thread()
        with self._lock:
            if self._owner is None:
                self._owner = current
            elif self._owner is not current:
                raise RuntimeError(FOREIGN_THREAD_ERROR)
            calls.append(current.ident)
            self.thread_names.append(current.name)

    def open(self) -> _ThreadBoundSession:
        """Открыть сессию: проверка потока + (опционально) «живой рендер» под гейтом."""
        self._claim_thread(self.open_threads)
        if self._gate is not None and not self._gate.wait(_WAIT_S):
            self.gate_timed_out = True
        return _ThreadBoundSession(self)

    def close(self) -> None:
        """Закрытие движка: запоминается поток, в котором оно исполнено."""
        with self._lock:
            self.close_calls += 1
            self.close_threads.append(threading.get_ident())


class _HttpCrawler:
    """HTTP-транспорт другой полосы: пока браузер висит, он обязан отработать.

    Первый фетч «отпускает» браузерный гейт — так перекрытие доказывается порядком
    событий, а не сном.
    """

    version = "http-crawler-1"
    engine = "http"
    supports_browser = False

    def __init__(
        self,
        *,
        browser_started: threading.Event,
        browser_gate: threading.Event,
    ) -> None:
        self._browser_started = browser_started
        self._browser_gate = browser_gate
        self.calls: list[str] = []
        #: Для каждого фетча: шёл ли он, пока браузерный фетч ещё висел.
        self.overlapped: list[bool] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        """Отработать HTTP-фетч и отпустить браузерный гейт."""
        self.calls.append(url)
        started = self._browser_started.wait(_WAIT_S)
        self.overlapped.append(started and not self._browser_gate.is_set())
        self._browser_gate.set()
        return RawPage(
            url=url,
            content=b"<html>http</html>",
            fetched_at="2026-01-01T00:00:00+00:00",
            fetch_mode=plan.fetch,
            lang=None,
        )


def _build(tmp_path: Path, *, urls: dict[str, list[str]], **kwargs: Any) -> Any:
    """Сборка корпуса на фикстурах ``test_corpus`` (план, экстрактор, чанкер)."""
    from test_corpus import _build as _build_corpus

    return _build_corpus(tmp_path, pages={}, urls=urls, **kwargs)


def _live_browser_threads() -> list[str]:
    """Живые потоки с именем владельца браузера (пусто — поток остановлен)."""
    return [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith(BROWSER_THREAD_PREFIX)
    ]


def _browser_notes(result: Any) -> list[str]:
    """Строка(и) наблюдения о браузерном маршруте из канала notes фазы."""
    return [
        entry
        for entry in result.log_entries
        if isinstance(entry, str) and "браузер:" in entry
    ]


# --------------------------------------------------------------------------- #
# (1) Все браузерные фетчи когорты идут из одного не-драйверского потока
# --------------------------------------------------------------------------- #


def test_browser_fetches_of_the_cohort_run_in_one_foreign_thread(
    tmp_path: Path,
) -> None:
    """Два корня на разных host'ах: все вызовы движка — из одного потока, не главного."""
    backend = _ThreadBoundBackend()
    crawler = BrowserCrawler(backend=backend)
    roots = ["https://br-a.test/", "https://br-b.test/"]
    driver_thread = threading.get_ident()

    result = _build(
        tmp_path,
        urls={"p1": roots},
        crawler=crawler,
        explicit_fetch={url: FetchMode.BROWSER for url in roots},
        crawl_defaults=CrawlRunConfig(max_workers=2, host_concurrency=2),
    )

    assert result.errors == []
    # Оба корня действительно отрендерены — иначе проверка потоков была бы пустой.
    assert len(backend.open_threads) == 2
    assert len(backend.navigate_threads) == 2
    # Один поток на когорту: второй корень пошёл в тот же поток, а не в новый.
    assert len(set(backend.open_threads + backend.navigate_threads)) == 1
    # ...и это **не** поток драйвера: работа ушла в выделенный владелец, а не инлайн.
    assert backend.open_threads[0] != driver_thread
    # Поток назван: иначе владельца движка не опознать в py-spy/журналах.
    assert all(name.startswith(BROWSER_THREAD_PREFIX) for name in backend.thread_names)
    # Сериализация опубликована числом, а не осталась догадкой.
    notes = _browser_notes(result)
    assert len(notes) == 1
    assert "2 фетчей через один поток" in notes[0]
    assert "полоса не блокируется" in notes[0]


# --------------------------------------------------------------------------- #
# (2) Браузерный фетч не блокирует выдачу волн другим полосам
# --------------------------------------------------------------------------- #


def test_browser_fetch_does_not_stop_other_lanes(tmp_path: Path) -> None:
    """Пока браузер висит на гейте, HTTP-корень другой полосы успевает отработать."""
    gate = threading.Event()
    started = threading.Event()

    class _GatedBackend(_ThreadBoundBackend):
        def open(self) -> _ThreadBoundSession:
            started.set()
            return super().open()

    backend = _GatedBackend(gate=gate)
    http = _HttpCrawler(browser_started=started, browser_gate=gate)
    browser_root = "https://br-lane.test/"
    http_root = "https://http-lane.test/"

    result = _build(
        tmp_path,
        urls={"p1": [browser_root], "p2": [http_root]},
        crawler=http,
        crawlers={"browser": BrowserCrawler(backend=backend)},
        explicit_fetch={browser_root: FetchMode.BROWSER},
        crawl_defaults=CrawlRunConfig(max_workers=2, host_concurrency=2),
    )

    assert result.errors == []
    # HTTP-полоса отработала, пока браузерный фетч ещё висел: выдачу волн никто не держал.
    assert http.calls == [http_root]
    assert http.overlapped == [True]
    # Браузерный гейт отпустили снаружи, а не по таймауту: ожидание было конечным.
    assert backend.gate_timed_out is False


# --------------------------------------------------------------------------- #
# (3) Владелец закрывает движок на своём потоке; пул остановлен
# --------------------------------------------------------------------------- #


def test_browser_engine_is_closed_on_its_own_thread(tmp_path: Path) -> None:
    """После фазы движок закрыт с потока-владельца, поток остановлен, повторный close цел."""
    backend = _ThreadBoundBackend()
    crawler = BrowserCrawler(backend=backend)
    root = "https://br-close.test/"

    result = _build(
        tmp_path,
        urls={"p1": [root]},
        crawler=crawler,
        explicit_fetch={root: FetchMode.BROWSER},
        crawl_defaults=CrawlRunConfig(max_workers=2, host_concurrency=1),
    )

    assert result.errors == []
    assert backend.close_calls == 1
    # Закрытие исполнено на том же потоке, что и фетчи: закрытие с чужого потока сырое
    # Playwright-закрытие проглотило бы (best-effort), и движок остался бы жить.
    assert backend.close_threads == backend.open_threads[:1]
    # Поток-владелец остановлен: брошенного браузерного процесса на остаток прогона нет.
    assert _live_browser_threads() == []
    # ``close`` идемпотентен: повторный вызов не падает и не закрывает движок дважды.
    crawler.close()
    assert backend.close_calls == 1


# --------------------------------------------------------------------------- #
# (4) Регресс исходного отказа: три последовательных корня-полосы
# --------------------------------------------------------------------------- #


def test_three_sequential_roots_do_not_switch_browser_threads(
    tmp_path: Path,
) -> None:
    """Три корня по очереди (одна полоса) — прежний код падал на втором.

    До правки каждая полоса заводила **свой** пул из одного потока и закрывала его на
    завершении корня: поток-создатель движка выходил, и следующий корень получал
    ``cannot switch to a different thread``. Теперь пул один на фазу, поэтому все три
    корня видят один и тот же поток-владелец.
    """
    backend = _ThreadBoundBackend()
    crawler = BrowserCrawler(backend=backend)
    roots = [
        "https://seq-a.test/",
        "https://seq-b.test/",
        "https://seq-c.test/",
    ]

    result = _build(
        tmp_path,
        urls={"p1": roots},
        crawler=crawler,
        explicit_fetch={url: FetchMode.BROWSER for url in roots},
        crawl_defaults=CrawlRunConfig(max_workers=2, host_concurrency=1),
    )

    # Прежний отказ приходил сюда как ``CrawlError`` и оставлял корни без документов.
    assert result.errors == []
    assert len(backend.open_threads) == 3
    assert len(set(backend.open_threads + backend.navigate_threads)) == 1


# --------------------------------------------------------------------------- #
# (6) Путь без пула фазы: драйвер сам заводит именованный поток и сам его гасит
# --------------------------------------------------------------------------- #


def test_driver_without_shared_pool_still_uses_one_browser_thread() -> None:
    """Прямой вызов ``drive_roots`` без ``browser_pool``: поток один, и он не течёт.

    Фаза всегда передаёт пул, но у драйвера есть и самостоятельный вход (тесты, узкие
    вызовы). Там прежняя схема «пул на полосу» вернула бы ту же ошибку смены потока,
    поэтому пул заводит сам драйвер — и он же обязан его остановить.
    """
    threads: list[int] = []
    names: list[str] = []

    def _begin(plan: Any, *, fetch_pool: Any = None, browser_pool: Any = None) -> Any:
        assert browser_pool is not None  # общий пул доехал до корня
        entry = corpus_drain._WaveEntry(
            pid="p1",
            wu=plan.root_url,
            depth=0,
            kind="fetch",
            desc=LocalFetch(
                pid="p1",
                wu=plan.root_url,
                depth=0,
                fetch_plan=FetchPlan(fetch=FetchMode.BROWSER),
                active=None,
                engine_value="browser",
                prior_item=None,
                resume=None,
                browser_blocked=False,
                poolable=False,
                host="seq.test",
                scope_host=None,
                enforce_redirect_scope=False,
            ),
        )
        yield corpus_drain.WavePlan(
            entries=[entry], hosts=frozenset({"seq.test"}), non_poolable=True
        )
        return (None, [])

    def _fetch(desc: LocalFetch) -> RawPage:
        threads.append(threading.get_ident())
        names.append(threading.current_thread().name)
        return RawPage(url=desc.wu, content=b"<html/>", fetched_at="2026-01-01T00:00:00+00:00")

    driver_thread = threading.get_ident()
    timings: dict[str, Any] = {}
    errors: list[str] = []
    notes: list[str] = []

    outcomes = corpus_driver.drive_roots(
        [corpus_drain.RootPlan(root_url="https://seq.test/", target=None, sitemap_seeds=False, product_id="p1")],
        host_concurrency=2,
        begin=_begin,
        lane_width=lambda plan: 2,
        local_fetch=_fetch,
        timings_for=lambda pid: timings.setdefault(str(pid), _CorpusStageTimings()),
        errors=errors,
        notes=notes,
        label=" теста",
    )

    assert errors == []
    assert len(outcomes) == 1
    assert threads and threads[0] != driver_thread
    assert all(name.startswith(BROWSER_THREAD_PREFIX) for name in names)
    # Пул создан драйвером — драйвер его и остановил: поток не пережил вызов.
    assert _live_browser_threads() == []
    assert notes and "1 фетчей через один поток" in notes[0]


# --------------------------------------------------------------------------- #
# (5) Адаптер называет нарушителя, а не отдаёт сырой текст Playwright
# --------------------------------------------------------------------------- #


def test_playwright_backend_names_the_thread_owner() -> None:
    """Движок, созданный в вышедшем потоке, отвергает чужой вызов внятным текстом."""
    owners: list[threading.Thread] = []

    def _owner() -> None:
        owners.append(threading.current_thread())

    thread = threading.Thread(target=_owner, name="exited-browser-owner")
    thread.start()
    thread.join()

    backend = _PlaywrightBackend(
        cdp_url=None, user_data_dir=None, headless=True, channel=None, timeout_ms=1000
    )
    # Движок «уже создан» в потоке, которого больше нет (приватные поля — это и есть
    # состояние, которое проверяется: без playwright реальный запуск недоступен).
    backend._context = object()  # noqa: SLF001 - моделируем созданный движок
    backend._owner_thread = owners[0]  # noqa: SLF001 - владелец уже вышел

    with pytest.raises(CrawlError) as info:
        backend.open()

    message = str(info.value)
    # Отказ называет обоих: кто владелец и кто пришёл (сырой текст Playwright — нет).
    assert "принадлежит потоку" in message
    assert "exited-browser-owner" in message
    assert threading.current_thread().name in message