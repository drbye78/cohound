"""Runtime экстракции фазы корпуса (Ш5): пул, воркер-процесс, кэш, наблюдаемость памяти.

Модуль — **runtime**, а не фаза: он не знает ни о продуктах, ни о store, ни о порядке
персистенции. Всё, что здесь есть, работает с одним заданием экстракции
(:class:`ExtractRequest` → ``ExtractedDoc``) и с пулом, который эти задания исполняет.
Это позволяет composition root (``core/cli/wiring.py``) собирать исполнитель по
**публичному** контракту, а не по приватным именам 10-килобайтного ``corpus`` (L1 плана
декомпозиции, ``docs/reports/2026-10-30-large-modules-refactoring-plan.md`` §4).

**Граница проведена по данным, а не по вкусу.** Перенесено ровно то, что живёт между
«задание экстракции» и «результат экстракции»: payload (:class:`ExtractRequest`),
инъекция исполнителя (:class:`ExtractionPool`), воркер-функция и её глобальный
экстрактор, ключ и чтение межпрогонного кэша, гигиена памяти воркера и фазовый реестр
пулов (:class:`ExtractPools`). Ни один из перенесённых символов не пишет в store — порядок
записи фазы остаётся в :mod:`core.pipeline.corpus` и зафиксирован тестом
``tests/pipeline/test_corpus_write_order.py``.

**Публичный контракт.** Приватные имена сохранены как есть (их тела — предмет замеров и
докстрингов), а публичный набор объявлен алиасами ниже: ``ExtractRequest``,
``ExtractPools``, ``extract_worker``, ``run_extract``, ``write_worker_peak_rss``,
``EXTRACT_MAX_POOL_REBUILDS``. Алиас, а не обёртка: обёртка завела бы второй путь
исполнения и вторую провенанс-запись у одного и того же кода.

**Совместимость.** :mod:`core.pipeline.corpus` реэкспортирует перенесённые имена: 46
операторов импорта в 29 тестовых файлах адресуют их по старому модулю (§0.3 плана —
существующие тесты не редактируются).
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from collections.abc import Callable
from concurrent.futures import Executor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from core.domain import DocumentLevel, PdfPipeline
from core.pipeline.extract_cache import ExtractionCache
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchMode, RawPage
from core.util import memory as _memory

#: P0: размер порции потокового хеширования retained-байтов (resume).
_RESUME_READ_CHUNK = 1024 * 1024

#: P4/C1: ограниченное число пересборок мёртвого пула экстракции (краш/OOM/двойной таймаут
#: без единого завершения). После исчерпания — остаток прогона терминально ``FAILED`` с явной
#: записью в ``errors`` (без авто-ретрая и без inline-экстракции в родителе).
_EXTRACT_MAX_POOL_REBUILDS = 2

#: C1: сколько подряд идущих таймаутов **без единого завершения** считаются мёртвым пулом
#: (каждый таймаут — полное окно ожидания ``timeout``, поэтому два подряд = ``2 × timeout``).
_EXTRACT_DEAD_POOL_TIMEOUTS = 2


def _as_text(value: Any) -> Any:
    """Приводит enum-значение (``StrEnum``) к строке ``.value``; прочее — как есть."""
    return getattr(value, "value", value)


def _hash_file(path: Path) -> str:
    """SHA-256 файла потоково (1 MiB): файл не читается целиком ради хеша."""
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_RESUME_READ_CHUNK), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


#: Ключ фазового пула экстракции (C1/P7): ``(уровень документов, механизм PDF, окно)``.
#: Обе оси — свойства корня и определяют, каким движком будет извлечён документ, поэтому
#: ключ обязан их различать: переиспользовать пул другого механизма значило бы извлечь
#: текст не тем движком, чем объявлено (провенанс ``extractor_version``).
_ExtractPoolKey = tuple[DocumentLevel, PdfPipeline, int]


@dataclass(frozen=True)
class _ExtractRequest:
    """P4: payload одного задания экстракции — **только примитивы** (picklable by construction).

    Байты страницы **не** передаются: retention (I19) уже положил их в ``raw_path``; воркер
    перечитывает этот файл **потоково** (только для хеша кэша) и реконструирует ``RawPage``
    с ``source_path`` и пустым ``content`` — тело документа не поднимается в RAM воркера.
    ``route`` — ось извлечения, ``fetch_mode`` — ось получения (обе — ``StrEnum``-примитивы).
    """

    raw_path: str
    url: str
    fetched_at: str
    fetch_mode: FetchMode
    lang: str | None
    status_code: int | None
    content_type: str | None
    engine: str | None
    route: ExtractionRoute
    #: P5: уровень извлечения документных форматов (``crawl.document_level``). Едет в payload
    #: для самодостаточности задания и корректности под будущие режимы; C1: пул — фазовый,
    #: ключ ``(уровень, механизм PDF, окно)``, поэтому воркер кэширует конвертер по ключу.
    level: DocumentLevel = DocumentLevel.FULL
    #: P7: механизм извлечения PDF (``crawl.pdf_pipeline``). Едет в payload по той же
    #: причине, что и уровень: воркер обязан обслужить задание **своим** механизмом, а не
    #: тем, что случайно построил инициализатор другого пула.
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED
    #: Ф1.2: корень и бюджет межпрогонного кэша экстракции. Едут **примитивами** — сам
    #: объект кэша не пиклится (payload остаётся picklable by construction); воркер
    #: строит экземпляр из этих полей. ``None`` → кэш в воркере выключен.
    cache_root: str | None = None
    cache_max_bytes: int | None = None
    #: F0: каталог стока пика памяти воркера (``{runs}/{run_id}/workers``). Воркер пишет
    #: туда свой ``ru_maxrss``; ``None`` — пик не собирается (thread-пул/inline). Поле
    #: едет в payload, а не в результат: контракт ``_ExtractRequest → ExtractedDoc``
    #: остаётся портовым, наблюдаемость не попадает в документ.
    rss_dir: str | None = None


@dataclass(frozen=True)
class ExtractionPool:
    """P4: инъекция исполнителя экстракции из composition root (A1, ``pipeline ↛ adapters``).

    :param pool_factory: фабрика ``(int, DocumentLevel, *, PdfPipeline) → Executor``
        (размер = ``limits.extract_workers``; уровень и механизм PDF — свойства корня, C1:
        пул — фазовый по ключу ``(уровень, механизм, окно)``); ``local`` —
        ``ProcessPoolExecutor`` (forkserver, инициализатор строит экстрактор нужного уровня
        и механизма на воркер), ``split`` — ``ThreadPoolExecutor`` (``RemoteExtractor``
        I/O-bound; оси ``document_level``/``pdf_pipeline`` под ``split`` не действуют — R-S).
    :param worker: рабочая функция ``_ExtractRequest → ExtractedDoc``. Для процессов —
        module-level (picklable by reference); для потоков допустим bound/closure.
    :param supports_worker_recycling: профиль умеет рециклинг воркеров по счётчику заданий
        (``ProcessPoolExecutor(max_tasks_per_child=N)``). ``False`` (``split``/потоки) —
        ось ``extract_max_tasks_per_child`` не действует (не тихо: запись в ``errors``).
    :param worker_rss_dir: F0 — каталог стока пика памяти воркеров
        (``{runs}/{run_id}/workers``). Каждый воркер-процесс после задания пишет туда свой
        ``ru_maxrss`` (файл на ``pid``), родитель читает максимум
        (:meth:`peak_worker_rss_mb`). ``None`` — пик не собирается (``split``: воркеры на
        edge; потоковый пул: ``ru_maxrss`` потока — это RSS процесса, а не воркера).

    **Файл-сток, а не канал.** Пик воркера собирается файлом на ``pid`` в каталоге прогона,
    а не через ``Manager``/очередь/поле результата:

    * контракт ``_ExtractRequest → ExtractedDoc`` остаётся прежним — документ не обязан
      нести наблюдаемость, а ``ExtractedDoc`` не получает поля, которого нет у порта;
    * ``Manager`` поднимает лишний процесс и канал (память ровно там, где её считают), а
      очередь требует слива после каждого задания — то есть работу на каждом документе;
    * воркеры **рециклятся** (``extract_max_tasks_per_child``), поэтому файл на ``pid``
      перезаписывается новым процессом с тем же ``pid``: берётся максимум по всем файлам,
      то есть по всем поколениям воркеров;
    * запись атомарна (``tmp`` + ``replace``) — родитель не прочитает половину числа.

    Сток пишется в ``finally`` задания: пик нужен и у задания, упавшего на извлечении
    (OOM-подобный отказ — ровно тот случай, ради которого метрика и собирается).
    """

    pool_factory: Callable[..., Executor]
    worker: Callable[[_ExtractRequest], ExtractedDoc]
    supports_worker_recycling: bool = False
    worker_rss_dir: str | None = None

    def peak_worker_rss_mb(self) -> float | None:
        """Максимум ``ru_maxrss`` по воркерам пула, MB; ``None`` — не собирали/не было.

        Читает файлы сточных записей (:attr:`worker_rss_dir`); максимум берётся по **всем**
        файлам, потому что ``pid`` переиспользуется при рециклинге воркеров, а имя файла —
        это ``pid`` поколения, а не поколение. Нечитаемый/битый файл пропускается: метрика
        не выдумывается и не роняет прогон. ``None`` возвращается и при пустом каталоге
        (пул не создавался либо заданий не было) — «не измеряли» ≠ «ноль».
        """
        if not self.worker_rss_dir:
            return None
        try:
            paths = list(Path(self.worker_rss_dir).glob("*.rss"))
        except OSError:  # pragma: no cover — каталог недоступен
            return None
        peak: float | None = None
        for path in paths:
            try:
                value = float(path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):  # pragma: no cover — битая запись
                continue
            # Неположительный RSS — не факт, а мусор в стоке (отрицательного пика памяти
            # не бывает). Он отбрасывается ровно как нечитаемая запись: метрика не
            # выдумывается и не может оказаться отрицательной.
            if value <= 0:
                continue
            if peak is None or value > peak:
                peak = value
        return peak


#: P4: глобальный экстрактор воркер-процесса. Устанавливается инициализатором пула
#: (composition root строит адаптер **внутри** процесса — объекты не пересекают границу).
_WORKER_EXTRACTOR: ExtractorPort | None = None
#: P5: уровень глобального экстрактора воркера (``None`` — «подходит для любого», split).
_WORKER_EXTRACTOR_LEVEL: DocumentLevel | None = None
#: P7: механизм извлечения PDF глобального экстрактора воркера (``None`` — «любой», split).
_WORKER_EXTRACTOR_PIPELINE: PdfPipeline | None = None
#: P5/P7: фабрика экстрактора ``(level, pdf_pipeline)`` в воркере (защитный lazy-путь, если
#: пара задания не совпала с инициализированной). Передаётся ссылкой из composition root.
_WORKER_EXTRACTOR_FACTORY: (
    Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None
) = None


def set_worker_extractor(
    extractor: ExtractorPort | None,
    *,
    level: DocumentLevel | None = None,
    factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None = None,
    pdf_pipeline: PdfPipeline | None = None,
) -> None:
    """Устанавливает экстрактор воркер-процесса (зовётся ``initializer`` пула, P4/P5/P7).

    ``level``/``pdf_pipeline`` — оси установленного экстрактора (``None`` — универсальный,
    напр. split); ``factory`` — фабрика ``(level, pdf_pipeline)``, если задание принесло
    иную пару (защитный путь).
    """
    global _WORKER_EXTRACTOR, _WORKER_EXTRACTOR_LEVEL, _WORKER_EXTRACTOR_PIPELINE
    global _WORKER_EXTRACTOR_FACTORY
    _WORKER_EXTRACTOR = extractor
    _WORKER_EXTRACTOR_LEVEL = level
    _WORKER_EXTRACTOR_PIPELINE = pdf_pipeline
    _WORKER_EXTRACTOR_FACTORY = factory


def _worker_extractor_for(
    level: DocumentLevel, pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED
) -> ExtractorPort | None:
    """Экстрактор воркера для пары ``(level, pdf_pipeline)`` (кэш размера 1; C1).

    Если инициализированный экстрактор подходит (обе оси совпали или универсальны) — он и
    используется. Иначе (защитный путь) строится фабрикой и кэшируется: пара «другой
    механизм PDF» — не та же самая настройка, и обслуживать её чужой экстрактор значило бы
    извлечь текст не тем движком, чем объявлено.
    """
    if _WORKER_EXTRACTOR is not None and (
        _WORKER_EXTRACTOR_LEVEL is None or level == _WORKER_EXTRACTOR_LEVEL
    ) and (
        _WORKER_EXTRACTOR_PIPELINE is None
        or pdf_pipeline == _WORKER_EXTRACTOR_PIPELINE
    ):
        return _WORKER_EXTRACTOR
    if _WORKER_EXTRACTOR_FACTORY is not None:
        set_worker_extractor(
            _WORKER_EXTRACTOR_FACTORY(level, pdf_pipeline),
            level=level,
            factory=_WORKER_EXTRACTOR_FACTORY,
            pdf_pipeline=pdf_pipeline,
        )
        return _WORKER_EXTRACTOR
    return _WORKER_EXTRACTOR


def _extract_cache_key(raw_path: str | Path) -> str:
    """Ключ кэша экстракции для retained-файла: ``{content_hash}.bin`` → хеш байтов.

    ``_retain`` называет retained-файл ``{sha256(bytes)}.bin``, поэтому ``stem`` — ровно
    ``content_hash`` тех байтов, которые прочитает воркер. Родительская проба (C1/L4) и
    проба воркера (:func:`_run_extract`) обязаны давать один ключ: это контракт раскладки
    retention (I19), а не случайное совпадение.
    """
    return Path(raw_path).stem


def _page_with_source_path(page: RawPage, raw_path: Path) -> RawPage:
    """``RawPage`` с путём к retained-файлу — когда тело пришло staging-путём.

    Документ (``pdf``/office), допущенный как терминальный артефакт (P6b), приезжает с
    пустым ``content`` и файлом на диске: тело не буферизуется, чтобы не держать 100 MiB в
    RAM. Экстрактору нужен источник байтов, поэтому inline-путь (без пула экстракции)
    сообщает ему **путь** к тому же retained-файлу (``source_path``, I19) — так же, как это
    делает воркер пула (:func:`_run_extract`). Байты в RAM родителя при этом **не**
    читаются: читает адаптер, и только он (docling открывает файл сам).

    Страница (``content`` непуст) возвращается как есть — путь байт-в-байт прежний.
    Отсутствующий/нечитаемый файл — тоже ``page`` без изменений: ошибку честно поднимет
    экстрактор, а не этот хелпер.
    """
    if page.content:
        return page
    if not Path(raw_path).is_file():
        return page
    return replace(page, source_path=str(raw_path))


def _cache_extractor_version(
    extractor: ExtractorPort, page: RawPage, route: ExtractionRoute
) -> str:
    """Версия движка для ключа ``extract_cache`` — **фактического** движка страницы.

    Кэш пишется с ``ExtractedDoc.extractor_version`` (его штампует сам адаптер: страница —
    ``trafilatura-html-2``, PDF — ``docling-doc-3:full/cpu/docling-<build>/<policy>``), поэтому проба
    обязана спрашивать ту же величину. Композитная ``.version`` для этого не годится: она
    описывает набор движков композита (``trafilatura-html-2+docling-doc-3:…``) и не
    совпадает со штампом **ни одного** документа — кэш от такого ключа не давал попаданий
    вовсе (write-only).

    Композит отвечает через :meth:`core.cli.wiring._LocalExtractor.extractor_version_for`:
    маршрут резолвится там же, где исполняется извлечение, поэтому ключ пробы и ключ записи
    не могут разойтись. Экстрактор без такого метода (простые адаптеры, тестовые фейки,
    ``RemoteExtractor``) штампует ровно ``.version`` — правило то же, диспетчера нет.

    Ошибка резолвера **не** глушится: тихий откат на композитную версию вернул бы ровно тот
    дефект, ради которого этот помощник существует (кэш, который никогда не попадает).
    """
    resolver = getattr(extractor, "extractor_version_for", None)
    if callable(resolver):
        return str(resolver(page, route) or "")
    return str(getattr(extractor, "version", "") or "")


def _run_extract(extractor: ExtractorPort, request: _ExtractRequest) -> ExtractedDoc:
    """Перечитывает retained-байты (I19) и вызывает ``extractor.extract`` на том же ``RawPage``.

    Общий шаг модульного воркер-процесса (:func:`_extract_worker`) и потокового воркера
    (``split``): источник байтов — retained-файл, а не буфер родителя (I19).

    **Байты не поднимаются в RAM.** Retained-файл читается потоково (1 MiB) только ради
    ключа кэша; сам ``RawPage`` несёт ``source_path`` и пустой ``content`` — документ
    (десятки/сотни МБ) не копируется в RSS воркера лишь для того, чтобы адаптер записал
    его во временный файл и прочитал обратно. Экстрактор, который не умеет читать путь,
    работает по ``content`` как прежде (поле аддитивное, дефолт ``None``).

    Ключ кэша считается из **фактически прочитанных** байтов (потоковый ``sha256`` файла) и
    совпадает с родительским :func:`_extract_cache_key` (``raw_path.stem``) — I19 требует
    верификации от байтов, а не от имени файла. Потоковый хеш даёт ровно тот же ключ:
    это хеш тех же байтов файла.
    """
    # I19: верификация от содержимого файла, а не от его имени; чтение — потоковое,
    # поэтому размер документа не влияет на RSS воркера.
    key = _hash_file(Path(request.raw_path))
    page = RawPage(
        url=request.url,
        content=b"",
        source_path=str(request.raw_path),
        fetched_at=request.fetched_at,
        fetch_mode=request.fetch_mode,
        lang=request.lang,
        status_code=request.status_code,
        content_type=request.content_type,
        engine=request.engine,
    )
    # Ф1.2: кэш экстракции. Ключ — хеш байтов retained-файла (I19): хит принимается лишь
    # при совпадении ключа конверта с этими байтами, поэтому кэш не может подсунуть
    # неверифицированный контент. Пропускается ровно вызов экстрактора — хеширование
    # retained-байтов остаётся.
    level_text = _as_text(request.level)
    cache = (
        ExtractionCache(request.cache_root, max_bytes=request.cache_max_bytes)
        if request.cache_root
        else None
    )
    if cache is not None:
        # Ключ — версия того движка, который обслужит **эту** страницу (штамп адаптера),
        # иначе проба не совпадёт с записью и кэш останется write-only.
        cached = cache.get(
            key, _cache_extractor_version(extractor, page, request.route), level_text
        )
        if cached is not None:
            return cached
    extracted = extractor.extract(page, request.route)
    if cache is not None:
        cache.put(extracted, level=level_text, content_hash=key)
    return extracted


def _trim_worker_arenas() -> bool:
    """Отдаёт ОС свободные арены glibc после документа (операционный рычаг памяти).

    Зовётся **в воркер-процессе** (:func:`_extract_worker`) после того, как результат
    извлечения получен, и **до** возврата payload родителю: родитель получает документ,
    когда heap воркера уже ужат. Адаптер об этом не знает — память процесса не его дело.

    Замер (22.10, ``layout_tables``, один процесс): после 81-стр. PDF RSS 1679 → 1084 MB
    (≈600 MB ОС), второй trim позже 1406 → 1240 MB; без trim 4 мелких PDF поднимали
    процесс с 1679 до 2052 MB (арены, а не объективная потребность). Подробности —
    :mod:`core.util.memory`.

    **Никогда не поднимает.** Гигиена памяти не вправе ломать извлечение: сбой хука —
    ``False`` и запись документа, а не отказ задания.
    """
    try:
        return bool(_memory.release_free_arenas())
    except Exception:  # noqa: BLE001 — наблюдаемость/память не ломают экстракцию
        return False


def _worker_peak_rss_mb() -> float | None:
    """Пик RSS **текущего** процесса, MB (``resource.getrusage``); ``None`` — не прочитать.

    ``ru_maxrss`` на Linux — KiB и **монотонен** в пределах процесса, поэтому значение
    после задания и есть пик воркера на этот момент (включая память конвертера).
    Не-POSIX окружение/сбой — ``None``: метрика не выдумывается.
    """
    try:
        import resource

        kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError, ValueError):  # pragma: no cover — не-POSIX
        return None
    if kib <= 0:  # pragma: no cover — вырожденное значение
        return None
    return round(float(kib) / 1024.0, 1)


def _write_worker_peak_rss(directory: str | None) -> None:
    """Воркер: пишет свой пик RSS в сток ``{directory}/{pid}.rss`` (F0).

    Файл, а не результат документа: контракт ``_ExtractRequest → ExtractedDoc`` не должен
    нести наблюдаемость, а ``Manager``/очередь стоили бы канал и слив на каждом задании
    (см. :class:`ExtractionPool`). ``pid`` переиспользуется при рециклинге воркеров,
    поэтому в файле хранится **максимум**: новое поколение дописывает только большее.
    Запись атомарна (``tmp`` + ``replace``), чтобы родитель не прочитал половину числа.

    Сбой стока — не ошибка прогона: наблюдаемость вторична к экстракции (как
    :func:`_trim_worker_arenas`).
    """
    if not directory:
        return
    peak = _worker_peak_rss_mb()
    if peak is None:
        return
    try:
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        target = root / f"{os.getpid()}.rss"
        try:
            previous = float(target.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            previous = 0.0
        if peak <= previous:
            return
        temporary = target.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(f"{peak}\n", encoding="utf-8")
        os.replace(temporary, target)
    except OSError:  # pragma: no cover — сток недоступен
        return


def _extract_worker(request: _ExtractRequest) -> ExtractedDoc:
    """Module-level воркер экстракции для процессов (picklable by reference, P4/§7).

    Использует экстрактор, построенный инициализатором в этом процессе; ``store``/``session``
    воркеру недоступны by construction (payload — только примитивы).

    После каждого документа (и до возврата результата родителю) свободные арены glibc
    возвращаются ОС — ``malloc_trim(0)``, :func:`_trim_worker_arenas`. Без этого RSS
    воркера растёт храповиком: ``max_tasks_per_child`` рециклинг лишь откладывает OOM,
    а не отменяет его. Рычаг **операционный**: результат байт-в-байт тот же, ``run_id``
    не меняется, ``execution_change`` не нужен (§13).

    F0: в том же ``finally`` пик RSS процесса уходит в файл-сток
    (:func:`_write_worker_peak_rss`) — наблюдаемость памяти воркера без изменения
    контракта результата. Пишется и на отказе задания: OOM-подобный отказ — главный
    случай, ради которого метрика собирается.
    """
    if _WORKER_EXTRACTOR is None:  # pragma: no cover — защита от неверной сборки
        return ExtractedDoc(
            text="",
            content_hash="",
            extractor_version="",
            error="extraction worker без экстрактора (initializer не отработал)",
        )
    extractor = _worker_extractor_for(request.level, request.pdf_pipeline)
    if extractor is None:  # pragma: no cover — защита от неверной сборки
        return ExtractedDoc(
            text="",
            content_hash="",
            extractor_version="",
            error="extraction worker без экстрактора для уровня/механизма",
        )
    # ``finally``: арены возвращаются и на честном отказе извлечения (исключение
    # поднимается дальше как прежде — пул сам решает судьбу задания).
    try:
        return _run_extract(extractor, request)
    finally:
        _trim_worker_arenas()
        _write_worker_peak_rss(request.rss_dir)


@dataclass
class _ExtractPoolSlot:
    """Состояние одного ключа фазового пула экстракции (C1/L1)."""

    executor: Executor | None = None
    #: Число выполненных пересборок (bounded :data:`_EXTRACT_MAX_POOL_REBUILDS`).
    rebuilds: int = 0
    #: Пул исчерпал пересборки: остаток прогона по ключу — терминальный отказ, без inline.
    abandoned: bool = False
    #: Подряд идущие таймауты без единого завершения: ``2`` = ``2 × timeout`` без результата.
    stalled_timeouts: int = 0


class _ExtractPools:
    """C1/L1: фазовый реестр пулов экстракции — один пул на прогон, а не на seed-root.

    Ключ — ``(DocumentLevel, window)``: уровень документной экстракции и окно (число
    заданий «в полёте»). Пул переиспользуется **всеми** корнями и продуктами фазы, поэтому
    forkserver и загрузка весов конвертера платятся один раз, а не на каждый корень.
    ``shutdown(wait=True, cancel_futures=True)`` выполняется ровно один раз — в ``finally``
    фазы (:func:`build_corpus`), а не на корень.

    Реестр также несёт фазовое состояние отказов **на ключ**: счётчик пересборок и признак
    «исчерпал пересборки». Исчерпание не воскресает на следующем корне того же ключа
    (иначе бюджет пересборок обходился бы числом корней).

    Таймауты (:meth:`note_timeout`) считаются подряд идущими, пока пул не завершил ни одного
    задания: каждое ожидание длится ``timeout``, поэтому ``2`` подряд = ``2 × timeout`` без
    единого завершения — только тогда пул считается мёртвым и пересобирается (bounded).
    Одиночный зависший артефакт убивает только себя (``extraction_timeout``), не пул.
    """

    def __init__(self, *, max_rebuilds: int = _EXTRACT_MAX_POOL_REBUILDS) -> None:
        self._max_rebuilds = max_rebuilds
        self._slots: dict[_ExtractPoolKey, _ExtractPoolSlot] = {}
        #: Наблюдаемость операционного рычага памяти: сколько заданий воркеры пула
        #: довели до результата. В воркере на каждое такое задание приходится ровно один
        #: ``malloc_trim(0)`` (:func:`_extract_worker`), поэтому счётчик фазы — честная
        #: нижняя граница числа trim-вызовов, видимая оператору (обратного канала из
        #: дочернего процесса нет).
        self.completed = 0

    def slot(self, key: _ExtractPoolKey) -> _ExtractPoolSlot:
        """Состояние ключа (создаётся лениво; I/O нет)."""
        slot = self._slots.get(key)
        if slot is None:
            slot = _ExtractPoolSlot()
            self._slots[key] = slot
        return slot

    def executor(
        self, key: _ExtractPoolKey, factory: Callable[[], Executor]
    ) -> Executor:
        """Пул ключа, создаваемый ``factory`` при первом обращении (лениво)."""
        slot = self.slot(key)
        if slot.executor is None:
            slot.executor = factory()
        return slot.executor

    def note_completion(self, key: _ExtractPoolKey) -> None:
        """Задание завершилось (любым исходом): пул жив, серия таймаутов сброшена."""
        self.slot(key).stalled_timeouts = 0
        self.completed += 1

    def note_timeout(self, key: _ExtractPoolKey) -> bool:
        """Учесть таймаут артефакта; ``True`` — пул мёртв (``2 × timeout`` без завершений)."""
        slot = self.slot(key)
        slot.stalled_timeouts += 1
        return slot.stalled_timeouts >= _EXTRACT_DEAD_POOL_TIMEOUTS

    def terminate(self, key: _ExtractPoolKey) -> None:
        """Reaping зависших воркеров (``terminate_workers``, Python 3.14; best-effort)."""
        executor = self.slot(key).executor
        terminate = getattr(executor, "terminate_workers", None)
        if terminate is None:
            return
        # Reaping — best-effort: воркер мог умереть сам или не поддерживать terminate.
        # Молчание здесь верно, потому что след не теряется: пул всё равно сбрасывается
        # (:meth:`discard`/``rebuild``), а вызывающий пишет видимую запись о мёртвом пуле
        # в ``errors`` при исчерпании пересборок.
        with contextlib.suppress(Exception):
            terminate()

    def discard(self, key: _ExtractPoolKey) -> None:
        """Закрывает пул ключа синхронно и отменяет запланированное (P0/OOM)."""
        slot = self.slot(key)
        executor = slot.executor
        slot.executor = None
        if executor is None:
            return
        # Reaping best-effort: сбой ``shutdown`` не отменяет факт «слот сброшен» — новый
        # пул ключа создаётся заново, а исчерпание пересборок видно вызывающему
        # (``rebuild`` → ``False`` → терминальный отказ в ``errors``).
        with contextlib.suppress(Exception):
            try:
                # P0/OOM: дождаться реального завершения воркеров и отменить запланированное.
                # ``wait=False`` оставлял бы старые процессы живыми поверх следующего корня
                # (двойной рабочий набор → OOM).
                executor.shutdown(wait=True, cancel_futures=True)
            except TypeError:  # фейки/старые исполнители без cancel_futures
                executor.shutdown(wait=True)

    def rebuild(self, key: _ExtractPoolKey) -> bool:
        """Discard + bounded rebuild; ``False`` — пересборки исчерпаны (ключ abandon'ен)."""
        self.discard(key)
        slot = self.slot(key)
        slot.stalled_timeouts = 0
        if slot.rebuilds >= self._max_rebuilds:
            slot.abandoned = True
            return False
        slot.rebuilds += 1
        return True

    def abandoned(self, key: _ExtractPoolKey) -> bool:
        """Исчерпал ли ключ пересборки (остаток прогона — терминальный отказ)."""
        return self.slot(key).abandoned

    def shutdown(self) -> None:
        """Единственная точка ``shutdown(wait=True, cancel_futures=True)`` фазы."""
        for key in list(self._slots):
            self.discard(key)


#: Публичный контракт модуля (L1): composition root и фаза берут **эти** имена, а не
#: приватные. Алиасы, а не обёртки: тело у публичного и приватного имени одно, поэтому
#: не может быть «двух экстракций» — одной публичной и одной приватной.
ExtractRequest = _ExtractRequest
ExtractPools = _ExtractPools
extract_worker = _extract_worker
run_extract = _run_extract
write_worker_peak_rss = _write_worker_peak_rss
EXTRACT_MAX_POOL_REBUILDS = _EXTRACT_MAX_POOL_REBUILDS

__all__ = [
    "EXTRACT_MAX_POOL_REBUILDS",
    "ExtractPools",
    "ExtractRequest",
    "ExtractionPool",
    "extract_worker",
    "run_extract",
    "set_worker_extractor",
    "write_worker_peak_rss",
]
