"""coHound — frontier обхода in-site ссылок и контрольные оси краула (D1/D2).

Модуль собирает **отдельную** цепочку приоритетов для новых осей контроля краула —
``CrawlConfig`` (application default, ``deploy.yaml:crawl``) < ``CrawlRunConfig``
(``run.yaml:crawl``, run profile) < :class:`~core.config.domain_profiles.DomainProfile`
(per-site) — и BFS-frontier одного **seed-root**.

Почему отдельная цепочка (решение заморожено): существующий ``resolve_crawl_target``
(``core.pipeline.crawl_sources``) резолвит оси ``fetch/extract/strategy/wait_for/rate_limit``
по слоям ``crawl_sources.yaml`` и трогать его порядок нельзя (24 layering-теста). Новые оси
(``max_workers``/``max_depth``/``max_documents``/``exclude_paths``/``exclude_documents``) идут
своим путём и **не** добавляются в ``crawl_sources.yaml``.

**Scope — per seed-root, не «SSRF-чек прошёл».** Обнаруженный URL ставится в очередь, только
если (1) :func:`core.util.security.validate_url` его принимает, (2) host **объявлен** для
этого корня (см. ниже), (3) путь укладывается в path-prefix корня по **границе сегмента**
(``/documents`` матчит ``/documents`` и ``/documents/x``, но **не** ``/documents-archive``;
правило из ``fetch_plan``), (4) URL не зачёркнут блэклистом. SSRF-проверка **одна** не
является проверкой scope.

**Scope = объявленные хосты, не «любой поддомен» (C, ``execution_change`` §13).** До 22.10
host-scope корня включал **любой** поддомен его хоста (``_host_matches`` — семантика
*профилей*, а не краула). Живой прогон показал цену: корень корпоративного портала
(``max_documents: 800``) израсходовал бюджет на учебный поддомен — раздатки курсов, которых
оператор не объявлял нигде, — и корень документации продукта (собственный seed, 13 страниц)
не получил ни одного своего элемента: прогон до него не дошёл. Теперь поддомен допускается,
только если он **объявлен** (``scope_hosts``: ключ ``domain_profiles.yaml`` или seed-корень
продукта) — и не отдан собственному корню (хост другого корня продукта исключается
вызывающим из ``scope_hosts``, иначе у «своего» корня нет собственного бюджета). ``www.``
эквивалентен базовому хосту с обеих сторон (``example.org`` ≡ ``www.example.org``).
Отказ **видим**: :attr:`Frontier.out_of_scope_subdomains`
считает отвергнутые поддомены по хостам (точный счётчик + ограниченные примеры), а
:attr:`Frontier.accepted_foreign_hosts` — принятые элементы с чужого (объявленного) хоста:
следующий прогон отвечает числом на вопрос «сколько дали поддомены». ``METHODOLOGY_VERSION``
при этом **не** меняется: изменились условия измерения (``crawl_config_digest``/
``seeds_digest``/``run_id``), а не правила методологии.

**Терминальный артефакт — исключение из (3), не из (1)/(2)/(4) (P6b).** Документ
(``pdf``/``docx``/``pptx``/``xlsx``) и заявленный контейнер (``zip``/``7z``/``rar``) **не
продолжают навигацию**, поэтому path-prefix к ним не применяется: ``/docs/`` может ссылаться
на ``/files/report.pdf``, и это не «выход из раздела». Host scope, SSRF-валидация и
``..``-traversal-guard остаются обязательными, блэклист оператора
(``exclude_paths``/``exclude_documents``) побеждает всегда. Обход **видим**: он считается и
даёт ограниченный список примеров (``artifacts_outside_prefix``), которые попадают в
провенанс фазы рядом с ``skipped_blacklisted``/``skipped_documents`` — молчаливый обход был бы
дефектом.

**Блэклисты (D1/D2).** ``exclude_paths`` — glob по **пути** URL (``fnmatchcase``,
case-sensitive); ``exclude_documents`` — glob по **basename** (последний сегмент пути, напр.
``*.pdf``). Никаких regex/подстрок: предсказуемо и ReDoS-безопасно. Оба применяются **и при
постановке в очередь, и перед фетчем**.

**Seed-root** — корень, от которого ведётся scope: top-level URL (из ``--urls``/feed-channel)
или запись каталога. Seeds имеют глубину 0; ссылка со страницы глубины ``d`` → глубина ``d+1``.

Нейтрален к предметной категории (§1.3): домены/паттерны — данные.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase
from urllib.parse import unquote, urlsplit, urlunsplit

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain.enums import (
    ArchiveDescent,
    DocumentLevel,
    EmbedDevice,
    EmbedPrecision,
    PdfDevice,
    PdfPipeline,
)
from core.domain.models import (
    DEFAULT_ARCHIVE_MAX_MEMBER_BYTES,
    DEFAULT_ARCHIVE_MAX_TOTAL_UNCOMPRESSED_BYTES,
    DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES,
    DEFAULT_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_RESPONSE_BYTES,
    CrawlRunConfig,
    RateLimitSpec,
    canonical_rate_limit,
)
from core.pipeline.fetch_plan import select_domain_profile
from core.pipeline.resources import workers_from_budget
from core.util.document_formats import (
    SUPPORTED_ARCHIVE_FORMATS,
    is_terminal_artifact_url,
    normalize_attachment_formats,
)
from core.util.rate_limit import normalize_host
from core.util.security import validate_url

__all__ = [
    "CRAWL_SEMANTICS",
    "DEFAULT_DOCUMENT_TIMEOUT_S",
    "SCOPE_ARTIFACT_OUTSIDE_PREFIX",
    "SCOPE_IN",
    "SCOPE_OUT",
    "CrawlLimits",
    "Frontier",
    "blacklist_match",
    "decoded_scope_path",
    "is_blacklisted",
    "normalize_scope_host",
    "resolve_crawl_limits",
    "resolve_embed_device",
    "resolve_embed_precision",
    "resolve_pdf_device",
    "root_path_prefix",
    "url_form_key",
]

#: Исходы scope-допуска (:meth:`Frontier.scope_admission`): URL в scope корня.
SCOPE_IN = "in_scope"
#: Исход: терминальный артефакт вне path-prefix, но на своём host — допущен (P6b).
SCOPE_ARTIFACT_OUTSIDE_PREFIX = "artifact_outside_prefix"
#: Исход: вне scope (чужой host / SSRF / traversal / не-артефакт вне префикса).
SCOPE_OUT = "out_of_scope"

#: Потолок **примеров** обхода path-prefix в провенансе (счётчик — точный).
ARTIFACT_BYPASS_EXAMPLES_MAX = 10

#: Потолок примеров отвергнутых поддоменов **на хост** в провенансе (счётчик — точный).
SUBDOMAIN_REJECT_EXAMPLES_MAX = 3

#: Потолок **примеров** схлопнутых форм одного URL в провенансе (счётчик — точный).
DUPLICATE_FORM_EXAMPLES_MAX = 10

#: Слаг **семантики краула** — версия правил обхода, а не осей (F3 плана 17).
#:
#: Входит в :meth:`CrawlLimits.as_dict` **безусловно** и через ``crawl_config_digest``
#: попадает в ``run_id``: семантика решает **состав корпуса** (порядок изъятия frontier,
#: момент решения о ``max_documents``, выбор победителя контент-дедупа), поэтому два
#: прогона с одинаковыми осями, но разной семантикой — разные измерения. Число осей
#: (``EXECUTION_CONTRACT_FIELDS``) при этом не меняется: носитель — дайджест краула,
#: у которого уже есть эта роль (``robots``/``max_depth``).
#:
#: ``crawl-2`` — порядок ``(depth, url)`` и бюджет на изъятии. ``crawl-3`` (F5 плана 17) —
#: краул больше не конвертирует: добытый документ становится строкой ``extract_queue``, а
#: канонический порядок входа Pass B — ``(product_id, url, chunk_no)``. Победитель
#: контент-дедупа поэтому перестал зависеть от порядка фетча: он равен ``min(url)`` среди
#: равных ``doc_hash`` (то же правило, что и в пост-проходе ``canonicalize_doc_urls``) — то
#: есть **семантика корпуса изменилась**, и это измерение, а не оптимизация. Bump — при
#: следующей смене семантики обхода/дедупа, одной строкой и вместе с golden-переаннотацией
#: (``execution_change``, §9).
CRAWL_SEMANTICS = "crawl-3"


def url_form_key(url: str) -> str:
    """Ключ дедупа, схлопывающий формы одного URL, различающиеся хвостовым ``/``.

    ``/page`` и ``/page/`` — одна страница, но два элемента очереди при дедупе по точной
    строке: измерено 1164 из 2402 ``fetched`` (48.5 %) у одного продукта
    (``2026-10-29-crawl-url-duplication``). Поэтому ключ снимает **один** завершающий
    ``/`` (путь ``/`` не трогается: это корень), а пустой путь приводится к ``/`` —
    ``https://host`` и ``https://host/`` тоже одна страница.

    Ключ — **только** для сравнения: сохраняемый в очереди URL не нормализуется, иначе
    форма, которую увидел автор ссылки, исчезла бы из провенанса. Seed'ы правило не
    применяют вовсе: ``p`` и ``p/`` — разные утверждения автора, потому что seed задаёт
    path-prefix корня (``09`` §7.2).
    """
    parts = urlsplit(url)
    path = parts.path
    if path.endswith("/") and path != "/":
        path = path[:-1]
    elif path == "":
        path = "/"
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


#: Потолок времени извлечения **одного** документа/члена контейнера
#: (ось ``crawl.document_timeout_s``), секунды.
#:
#: Значение **равно прежнему зашитому поведению**: до появления оси тот же потолок жил
#: константой ``_EXTRACT_TASK_TIMEOUT_S = 1800.0`` в :mod:`core.pipeline.corpus`
#: (``future.result(timeout=...)`` на одну задачу экстракции). Новая ось обязана
#: сохранить это число дефолтом: иначе первый же прогон после правки получил бы другой
#: ``run_id`` и другой (не измеренный) сценарий таймаута без изменения измерения (A3).
#: 1800 с — guard от зависания воркера, а не SLA: замер layout_tables на 665-стр. PDF —
#: 866 с (``docs/architecture/05-dependency-stack.md`` §2).
DEFAULT_DOCUMENT_TIMEOUT_S = 1800.0


def normalize_scope_host(host: str) -> str:
    """Нормализация хоста для сравнения scope: нижний регистр, без хвостовой точки,
    ``www.`` эквивалентен базовому хосту (``www.example.org`` ≡ ``example.org``).

    Эквивалентность ``www.`` — не косметика: оператор объявляет ``example.org``, а ссылки
    сайта ведут на ``www.example.org`` (и наоборот: корень ``www.example.org`` при ключе
    профиля ``example.org``). Без нормализации обе стороны теряли бы половину сайта.
    """
    text = (host or "").strip().lower().rstrip(".")
    return text[4:] if text.startswith("www.") else text


@dataclass(frozen=True)
class CrawlLimits:
    """Эффективные оси контроля краула после разрешения трёхуровневой цепочки (D1/D2).

    ``max_depth`` уже с дефолтом 0 (текущее поведение: только seeds). ``max_documents`` —
    ``None`` = без лимита. ``max_workers`` (D4, конкурентность) резолвится и попадает в
    провенанс; ``max_workers`` управляет **сетевым** fetch-пулом, ``extract_workers`` (P4) —
    отдельной осью **экстракции** (CPU-параллелизм). Обе оси — ``int | None`` (``None``/``1``
    ⇒ пул не создаётся).

    **Только ``max_workers`` — ось контракта (F0/§2.10).** ``extract_workers``/
    ``extract_max_tasks_per_child`` резолвятся и публикуются (провенанс, манифест,
    ``metrics.json.resources.scheduling``), но в :meth:`as_dict` — а значит и в
    ``crawl_config_digest``/``run_id`` — **не** входят: параллелизм извлечения не может
    изменить состав корпуса, тогда как конкурентность fetch взаимодействует с усечением
    ``max_documents``. Иначе выведенное из бюджета хоста число воркеров делало бы ``run_id``
    функцией машины (см. :meth:`as_dict`).
    """

    max_workers: int | None = None
    #: F1 (план 17): сколько **host'ов** ведёт один процесс краула. ``None``/``1`` —
    #: сегодняшнее поведение (одна полоса за раз). Единица — host, потому что лимитер
    #: пейсит per-host (`core/util/rate_limit.py:53`), а внутри host параллельность
    #: пропускной способности не даёт (замер 2026-10-31). ``host_concurrency_explicit``
    #: отмечает объявление на app/run слое: только тогда ключ попадает в ``as_dict``.
    host_concurrency: int | None = None
    host_concurrency_explicit: bool = False
    #: Темп запросов (ось ``rate_limit``) в **канонической строковой** форме
    #: (:func:`core.domain.models.canonical_rate_limit`). Это **условие измерения**, а не
    #: рычаг расписания: per-site значение перекрывает объявленную run-level ось, то есть
    #: может нейтрализовать ``run.yaml:crawl.rate_limit`` для конкретного хоста; кроме
    #: того, темп решает исход ``429``/``5xx`` и взаимодействует с усечением
    #: ``max_documents`` (сколько страниц успело попасть в очередь). Резолвится ровно той
    #: же формулой, что исполнение (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`):
    #: site > run, app-слой темпа не объявляет.
    #:
    #: ``rate_limit_explicit`` отмечает объявление на site/run слое: только тогда ключ
    #: попадает в :meth:`as_dict` — прогон без темпа обязан сохранить прежний
    #: ``crawl_config_digest``/``run_id`` (§13).
    rate_limit: str | None = None
    rate_limit_explicit: bool = False
    #: F6 (план 17, §4.3): watermark бэклога конверсии, МБ retained-файлов строк
    #: ``pending``+``claimed``. ``None`` — предел не объявлен (приём не приостанавливается,
    #: ``backlog_bytes`` всё равно публикуется числом). Это **условие измерения**, а не
    #: рычаг расписания: достижение предела останавливает приём, то есть меняет состав
    #: корпуса (какие документы успели попасть в очередь). Поэтому ось входит в
    #: ``as_dict()`` при объявлении и объявление — ``execution_change`` (§13).
    #: ``extract_backlog_mb_explicit`` отмечает объявление на app/run слое.
    extract_backlog_mb: int | None = None
    extract_backlog_mb_explicit: bool = False
    #: F7 (17 §2): **объявленная карта шардов** — ``((node, (host, …)), …)`` в
    #: канонической форме (узлы и host'ы отсортированы, host'ы уникальны). ``None`` —
    #: шардов нет. Ключ в :meth:`as_dict` появляется **только при объявлении** (как
    #: ``host_concurrency``): карта меняет состав корпуса, значит обязана менять
    #: ``run_id``, но прогон без неё не должен сдвигаться.
    #:
    #: Почему карта целиком, а не «мой шард»: в кластере все узлы объявляют одну и ту же
    #: карту, и ``run_id`` у них обязан быть **один** — это один прогон, нарезанный на
    #: шарды, а не N разных измерений. Идентификатор узла в дайджест **не** входит (это
    #: deployment provenance, как ``owner`` сеанса) и живёт в провенансе сеанса.
    host_shards: tuple[tuple[str, tuple[str, ...]], ...] | None = None
    extract_workers: int | None = None
    #: F0: ``True`` — ``extract_workers`` выведен из бюджета памяти
    #: (:func:`core.pipeline.resources.workers_from_budget`), а не объявлен на слое
    #: app/run/site. Наблюдаемость расписания: в дайджест не входит (это факт хоста).
    extract_workers_derived: bool = False
    #: P4/OOM: рециклинг воркер-процессов локальной экстракции (``None`` — ось не задана).
    #: Значение резолвится той же цепочкой app < run < site, что и ``extract_workers``.
    extract_max_tasks_per_child: int | None = None
    #: P0/OOM: эффективный потолок одного ответа/retained-файла (байты). Дефолт 20 MiB
    #: действует всегда; ``max_response_bytes_explicit`` отмечает, что ось задана на
    #: одном из слоёв (только тогда ключ попадает в ``as_dict``/дайджест).
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    max_response_bytes_explicit: bool = False
    #: P6: потоковый потолок допущенного артефакта-контейнера (байты). Страница остаётся
    #: под ``max_response_bytes``; артефакт свыше него уходит в staging-файл, а не в RAM.
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    max_artifact_bytes_explicit: bool = False
    #: P6: заявленные форматы-контейнеры (класс «артефакт»). Дефолт — все поддерживаемые;
    #: сужение набора делает незaявленный контейнер обычным бинарным артефактом.
    attachment_formats: tuple[str, ...] = SUPPORTED_ARCHIVE_FORMATS
    attachment_formats_explicit: bool = False
    #: P6: границы спуска в контейнер (провенанс/дайджест; исполняет следующий слой).
    archive_max_member_bytes: int = DEFAULT_ARCHIVE_MAX_MEMBER_BYTES
    archive_max_member_bytes_explicit: bool = False
    archive_max_uncompressed_bytes: int = DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES
    archive_max_uncompressed_bytes_explicit: bool = False
    archive_max_members: int | None = None
    archive_max_members_explicit: bool = False
    #: B3: кумулятивный потолок распакованного объёма **на корень спуска** (байты). Оси
    #: ``archive_max_*`` ограничивают один контейнер; без кумулятивной оси архив из 10 000
    #: вложенных архивов «в бюджете» каждый исчерпал бы CPU/диск. Ось операционная
    #: (``execution_change``, §13).
    archive_max_total_uncompressed_bytes: int = (
        DEFAULT_ARCHIVE_MAX_TOTAL_UNCOMPRESSED_BYTES
    )
    archive_max_total_uncompressed_bytes_explicit: bool = False
    archive_descent: ArchiveDescent = ArchiveDescent.FULL
    archive_descent_explicit: bool = False
    max_depth: int = 0
    max_documents: int | None = None
    #: Операторская ось «не ограничивать, а измерять»: максимум известных URL
    #: ревалидации **на продукт** за цикл. ``None`` — ось не задана (ревалидируются
    #: все известные URL; поведение байт-в-байт). Заданное значение откладывает хвост,
    #: помечает его ``stale`` в ``url_state`` и поднимает видимое усечение продукта
    #: (``revalidate_max_documents``), а не молча пропускает.
    revalidate_max_documents: int | None = None
    exclude_paths: tuple[str, ...] = ()
    exclude_documents: tuple[str, ...] = ()
    #: P5: уровень извлечения документных форматов (ось ``crawl.document_level``). Дефолт
    #: ``full`` = сегодняшнее эффективное поведение. Резолвится той же цепочкой app < run <
    #: site; per seed-root (один корень = один уровень).
    document_level: DocumentLevel = DocumentLevel.FULL
    #: P7: механизм извлечения PDF (ось ``crawl.pdf_pipeline``). Дефолт ``structured`` =
    #: сегодняшнее поведение (docling: layout + таблицы + OCR). ``text_layer`` — только
    #: текстовый слой без моделей. Резолвится той же цепочкой app < run < site; ось
    #: входит в ``as_dict()`` **всегда**: механизм определяет текст документа, то есть
    #: состав корпуса (``execution_change``, §13), а не расписание.
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED
    #: P9: **устройство** моделей извлечения PDF (ось ``crawl.pdf_device``). Дефолт
    #: ``cpu`` — поведение прогонов, ось не объявивших, остаётся прежним (модели на CPU).
    #: ``auto`` не существует намеренно (R5): объявленная ось обязана действовать, а
    #: «устройство выбрал движок» — ложный провенанс (тот же класс, что env-модель
    #: оценщика, §7.4). Резолвится цепочкой app < run: устройство — свойство **хоста
    #: исполнения**, а не сайта, поэтому per-site слоя у оси нет.
    #:
    #: В :meth:`as_dict` **не** входит: устройство уже названо ``extractor_stack``
    #: (``docling-doc-3/<device>/docling-<build>/<policy>``) и ``extractor_version`` документа, то
    #: есть входит в ``run_id`` и в ключ ``extract_cache``. Второй раз в
    #: ``crawl_config_digest`` это был бы второй носитель одного материала: расхождение
    #: прогонов нельзя было бы разобрать по полям.
    pdf_device: PdfDevice = PdfDevice.CPU
    #: P8: таймаут **документа** (``crawl.document_timeout_s``) — потолок ожидания
    #: извлечения одного документа/члена контейнера, секунды. Дефолт равен прежнему
    #: зашитому значению (:data:`DEFAULT_DOCUMENT_TIMEOUT_S` = 1800.0), поэтому объявление
    #: оси не меняет поведение прогона, который её не объявлял. В :meth:`as_dict` входит
    #: **только при явном задании** — как ``max_response_bytes``/``archive_*``: иначе ключ
    #: появился бы у всех прогонов и ``crawl_config_digest``/``run_id`` сменились бы без
    #: изменения измерения (§13). Задание оси — ``execution_change``: потолок решает, какой
    #: документ успел стать корпусом (медленный источник), то есть условие измерения.
    document_timeout_s: float = DEFAULT_DOCUMENT_TIMEOUT_S
    document_timeout_s_explicit: bool = False
    #: D1/§10: **эффективная** egress-политика ``robots.txt`` для корня (включена/выключена).
    #: Это условие измерения, а не рычаг пропускной способности: гейт ``robots.txt`` решает,
    #: какие документы вообще допускаются в корпус, поэтому он входит в ``as_dict()``
    #: **всегда**. Резолвится цепочкой app < run < site (сегодня поле несёт только app-слой
    #: ``deploy.yaml:crawl.robots``); ``None`` на всех слоях = автодетект — robots включён
    #: ровно при обходе ссылок (``max_depth > 0``) или наличии per-site ``sitemap``.
    robots: bool = False

    def as_dict(self) -> dict[str, object]:
        """Канонический словарь для провенанса/манифеста (сортированно-стабильно).

        **Асимметрия осей параллелизма (F0/§2.10).** ``max_workers`` (конкурентность
        **fetch**) входит в словарь всегда, ``extract_workers``/
        ``extract_max_tasks_per_child`` (параллелизм **извлечения**) — не входят вовсе.
        Причина не в удобстве, а в разной связи с составом корпуса:

        * конкурентность fetch взаимодействует с усечением ``max_documents`` — сколько
          страниц успело попасть в очередь до исчерпания бюджета, зависит от того, в
          каком порядке и с какой параллельностью шёл обход. Меняя её, можно получить
          **другой корпус**, то есть другое измерение;
        * параллелизм извлечения состав корпуса изменить не может: документ уже добыт и
          ретенирован, чанки — детерминированная функция его текста и уровня. Это
          расписание, а не условие измерения.

        Поэтому первое — ось контракта (``run_id``/``execution_change``), второе —
        наблюдённый факт (``metrics.json.resources.scheduling``). Если бы выведенное из
        бюджета число воркеров входило в дайджест, ``run_id`` стал бы функцией хоста:
        та же ошибка, что env-модель у оценщика (§7.4) — идентичность прогона перестала
        бы описывать то, что реально измерялось.

        Опциональные оси (потолок ревалидации, артефакты) включаются **только когда
        заданы**: ``None`` не добавляет ключ, поэтому дайджест ``crawl_config_digest``
        прогонов без этих осей не меняется (обратная совместимость §13).

        ``rate_limit`` — тоже условие измерения, а не расписание: per-site значение
        перекрывает объявленную run-level ось (``run.yaml:crawl.rate_limit``), поэтому без
        него оператор мог бы нейтрализовать объявленную ось для конкретного хоста, не
        сдвинув ``run_id``. Темп влияет на состав корпуса и на исход ``429``/``5xx``
        (измеренный всплеск ``504`` на медленном хосте) и взаимодействует с усечением
        ``max_documents`` — тот же довод, что у ``max_workers``. Ключ появляется **только
        при объявлении**: прогон без темпа сохраняет прежний словарь, а run-level ось
        по-прежнему видна отдельно как манифестное поле ``crawl_rate_limit``.

        ``robots`` — исключение: он попадает в словарь **всегда**, как ``max_depth``,
        потому что его дефолт — реальное поведение, а не отсутствующая ось. Гейт
        ``robots.txt`` меняет **состав корпуса** (какие документы вообще допущены), то
        есть является условием измерения; прогон с robots и прогон без него обязаны
        иметь разный ``crawl_config_digest``, иначе один ``run_id`` описывал бы два
        разных корпуса (I19/§15).

        ``semantics`` (:data:`CRAWL_SEMANTICS`) — второй такой ключ, но по другой причине:
        это не ось, а **версия правил обхода** (порядок ``(depth, url)``, бюджет
        ``max_documents`` на изъятии, победитель контент-дедупа). Правила меняют состав
        корпуса при тех же осях, поэтому прогон, снятый прежней семантикой, обязан иметь
        другой ``run_id`` — безусловность здесь и есть механизм (§13).
        """
        payload: dict[str, object] = {
            "max_workers": self.max_workers,
            "max_depth": self.max_depth,
            "max_documents": self.max_documents,
            "exclude_paths": list(self.exclude_paths),
            "exclude_documents": list(self.exclude_documents),
            "document_level": self.document_level.value,
            "pdf_pipeline": self.pdf_pipeline.value,
            "robots": self.robots,
            # F3 (план 17): слаг **семантики краула** — безусловный ключ, как ``robots``.
            # Порядок обхода frontier (``(depth, url)``) и бюджет ``max_documents``,
            # решаемый на изъятии, меняют **состав корпуса** на ограниченных прогонах,
            # поэтому обязаны менять ``run_id``. Носитель — именно ``crawl_config_digest``
            # (:func:`core.pipeline.report.crawl_control_digest`): ``EXECUTION_CONTRACT_FIELDS``
            # (32 поля, ``digests.py``) не трогаются, слаг входит в контракт через этот
            # дайджест. Ключ безусловный намеренно: это не ось, которую оператор может
            # объявить или не объявить, а версия правил — прогон, снятый прежней
            # семантикой, несравним с новым при тех же осях, и один ``run_id`` над двумя
            # разными корпусами был бы ложью о провенансе (§13). Значение покрывает и
            # F3B (победитель контент-дедупа ``min(url)``): смена канонического URL —
            # тоже смена корпуса. Bump ``crawl-3`` — при следующей смене семантики
            # краула (не осей: оси входят в словарь сами). Обратной сборки
            # ``CrawlLimits(**as_dict())`` в дереве нет — словарь только сериализуется.
            "semantics": CRAWL_SEMANTICS,
        }
        # Та же обратная совместимость: ключ появляется только при явно заданной оси,
        # поэтому дайджест прогонов без неё не меняется, а при задании — честно меняется.
        if self.document_timeout_s_explicit:
            payload["document_timeout_s"] = self.document_timeout_s
        # F1 (план 17): ось параллельности краула. Только при объявлении — иначе дайджест
        # прогонов без неё не меняется, а при задании — честно меняется.
        #
        # Пин ``run_id`` прогона **без осей** — не константа кода, а величина, которую
        # фиксирует эталон (замер ``2026-10-31-crawl-parallelism-limits.md``), и её
        # держит инвариант теста ``test_host_concurrency_default_does_not_change_digest``
        # («ключ не просачивается из дефолта»). Поэтому здесь не записан ни прежний, ни
        # текущий хеш: значение меняется объявлением, а комментарий с числом устаревает
        # молча. Текущее значение — ``./hound.py plan --run-yaml packages/ru-pg-dbms/run.yaml``.
        # Оно сдвигалось осознанно вместе со слагом семантики: ``crawl-2`` (F3) и
        # ``crawl-3`` (F5), а не от объявления осей — слаг входит в те же
        # ``crawl_config_digest`` безусловно.
        if self.host_concurrency_explicit:
            payload["host_concurrency"] = self.host_concurrency
        # Темп — условие измерения (перекрытие run-level оси, исход ``429``/``5xx``,
        # взаимодействие с усечением ``max_documents``). Только при объявлении: иначе
        # прогон без темпа получил бы новый ``run_id`` без изменения измерения (§13).
        if self.rate_limit_explicit and self.rate_limit is not None:
            payload["rate_limit"] = self.rate_limit
        # F6 (план 17, §4.3): watermark бэклога. Только при объявлении — иначе дайджест
        # прогонов без оси не меняется. При объявлении ключ честно меняет ``run_id``:
        # остановка приёма по бэклогу меняет состав корпуса, то есть измерение.
        if self.extract_backlog_mb_explicit and self.extract_backlog_mb is not None:
            payload["extract_backlog_mb"] = self.extract_backlog_mb
        # F7 (17 §2): объявленная карта шардов — **только при объявлении**. Она входит
        # безусловно в собственной форме (узлы/host'ы отсортированы): карта решает, какие
        # host'ы попадут в корпус, то есть является условием измерения. Идентификатор
        # узла сюда не входит намеренно — иначе узлы одного кластера получили бы разные
        # ``run_id`` на одном прогоне.
        if self.host_shards is not None:
            payload["host_shards"] = [
                {"node": node, "hosts": sorted(hosts)}
                for node, hosts in sorted(self.host_shards, key=lambda entry: entry[0])
            ]
        if self.revalidate_max_documents is not None:
            payload["revalidate_max_documents"] = self.revalidate_max_documents
        if self.max_response_bytes_explicit:
            payload["max_response_bytes"] = self.max_response_bytes
        # P6: новые оси — только когда заданы (дефолт не меняет дайджест старых прогонов).
        if self.max_artifact_bytes_explicit:
            payload["max_artifact_bytes"] = self.max_artifact_bytes
        if self.attachment_formats_explicit:
            payload["attachment_formats"] = list(self.attachment_formats)
        if self.archive_max_member_bytes_explicit:
            payload["archive_max_member_bytes"] = self.archive_max_member_bytes
        if self.archive_max_uncompressed_bytes_explicit:
            payload["archive_max_uncompressed_bytes"] = (
                self.archive_max_uncompressed_bytes
            )
        if self.archive_max_members_explicit and self.archive_max_members is not None:
            payload["archive_max_members"] = self.archive_max_members
        if self.archive_max_total_uncompressed_bytes_explicit:
            payload["archive_max_total_uncompressed_bytes"] = (
                self.archive_max_total_uncompressed_bytes
            )
        if self.archive_descent_explicit:
            payload["archive_descent"] = self.archive_descent.value
        return payload


def resolve_pdf_device(app: object, run: object | None = None) -> PdfDevice:
    """Устройство моделей извлечения PDF по объявленным слоям: app < run (P9).

    Цепочка та же, что у остальных осей краула, но **без** per-site слоя: устройство —
    свойство хоста исполнения, а не сайта. Поэтому формула здесь одна, а не три
    (``_pick`` с двумя слагаемыми): подставлять ``None`` третьим слагаемым значило бы
    обещать per-site слой, которого нет.

    Дефолт ``cpu`` — **объявленное** значение, а не «что выберет движок»: ``auto``
    запрещён (R5), иначе прогон на машине с CUDA молча исполнялся бы на GPU, а
    ``extractor_version`` называл бы устройство, которого вход не объявлял.

    Функция чистая (никаких импортов torch): доступность CUDA — отдельная проверка
    (:func:`core.pipeline.resources.cuda_vram_mb`), а не часть резолва.
    """
    run_value = getattr(run, "pdf_device", None) if run is not None else None
    value = _pick(getattr(app, "pdf_device", None), run_value, None)
    # ``app``/``run`` объявлены ``object`` (терпимость к контейнеру ``DeployConfig``),
    # поэтому ``getattr`` даёт ``Any``; на самом слое значение — поле Pydantic
    # ``PdfDevice``. ``PdfDevice(...)`` здесь канонизирует значение, а не проверяет тип.
    return PdfDevice(PdfDevice.CPU if value is None else value)


def resolve_embed_device(app: object, run: object | None = None) -> EmbedDevice:
    """Устройство эмбеддера по объявленным слоям: app < run.

    Цепочка та же, что у :func:`resolve_pdf_device`, и **без** per-site слагаемого:
    устройство — свойство хоста исполнения, а не сайта. Подставлять ``None`` третьим
    слагаемым значило бы обещать per-site слой, которого нет.

    Дефолт ``cpu`` — **объявленное** значение, а не «что выберет torch»: ``auto``
    запрещён (R5), иначе прогон на машине с CUDA молча считал бы векторы на GPU, а
    ``embed_device``/``pb``/ключ ``embed_cache`` называли бы устройство, которого вход
    не объявлял (измеренный дефект ``14`` оговорки 5).

    Функция чистая (никаких импортов torch): доступность CUDA — отдельная проверка
    (:func:`core.cli.wiring.require_embed_device`), а не часть резолва.

    :param app: app-слой (:class:`core.config.deploy.EmbedConfig`). Допускается и
        контейнер (:class:`DeployConfig`): тогда берётся его секция ``embed``. Терпимость
        к контейнеру намеренная — иначе вызов с ``deploy`` вместо ``deploy.embed`` молча
        вернул бы ``cpu`` и объявил бы неисполнявшееся устройство (тот же класс, что тихий
        фолбэк).
    :param run: run-слой (:class:`core.domain.models.EmbedRunConfig`) или ``None``.
    """
    app_layer = getattr(app, "embed", app)
    run_layer = getattr(run, "embed", run) if run is not None else None
    run_value = getattr(run_layer, "device", None) if run_layer is not None else None
    value = _pick(getattr(app_layer, "device", None), run_value, None)
    # Слои приходят из ``object``-параметра (``getattr`` → ``Any``), но на самом слое
    # значение — поле Pydantic ``EmbedDevice``: канонизация, а не проверка типа.
    return EmbedDevice(EmbedDevice.CPU if value is None else value)


def resolve_embed_precision(app: object, run: object | None = None) -> EmbedPrecision:
    """Точность счёта вектора по объявленным слоям: app < run (ось ``embed.precision``).

    Цепочка та же, что у :func:`resolve_embed_device`, и из тех же соображений:
    точность — свойство стека/хоста исполнения, а не сайта, поэтому per-site слагаемого
    нет; дефолт ``fp32`` **объявлен** (это поведение до появления оси), а не выведен из
    окружения — иначе прогон на хосте с «удобным» autocast считал бы векторы иначе, чем
    объявляет ``embed_precision``/``pb``/ключ ``embed_cache``.

    Функция чистая (никаких импортов torch): *исполнимость* объявленной точности
    проверяется там, где выбирается эмбеддер
    (:func:`core.cli.wiring._resolve_embedder`), — «не поддержано» обязан быть отказ, а
    не тихий fp32.

    :param app: app-слой (:class:`core.config.deploy.EmbedConfig`); допускается контейнер
        (:class:`DeployConfig`) — тогда берётся его секция ``embed`` (та же терпимость,
        что у :func:`resolve_embed_device`: иначе вызов с ``deploy`` вернул бы дефолт и
        объявил неисполнявшуюся точность).
    :param run: run-слой (:class:`core.domain.models.EmbedRunConfig`) или ``None``.
    """
    app_layer = getattr(app, "embed", app)
    run_layer = getattr(run, "embed", run) if run is not None else None
    run_value = getattr(run_layer, "precision", None) if run_layer is not None else None
    value = _pick(getattr(app_layer, "precision", None), run_value, None)
    # Слои приходят из ``object``-параметра (``getattr`` → ``Any``), но на самом слое
    # значение — поле Pydantic ``EmbedPrecision``: канонизация, а не проверка типа.
    return EmbedPrecision(EmbedPrecision.FP32 if value is None else value)


def _pick[T](app: T | None, run: T | None, site: T | None) -> T | None:
    """Высший не-``None`` приоритет: site > run > app (application default).

    Функция generic, а не «``object`` на входе и на выходе»: слои резолва типизированы
    (``CrawlConfig``/``CrawlRunConfig``/``DomainProfile``), поэтому тип значения известен
    из объявления поля. ``object`` на выходе не добавлял точности — он **стирал** её, и
    каждая ось ниже приводилась к ``int``/``str`` вручную, хотя проверять там нечего.
    """
    if site is not None:
        return site
    if run is not None:
        return run
    return app


def _declared_host_shards(
    app: object,
) -> tuple[tuple[str, tuple[str, ...]], ...] | None:
    """Объявленная карта шардов app-слоя в канонической форме (17 §2, F7).

    Источник — ``deploy.yaml: crawl.shards`` (единственный слой: шард — свойство
    развёртывания, а не прогона и не сайта). ``None`` — карты нет (поведение прежнее).

    Канон здесь, а не в конфиге: ``as_dict``/``run_id`` обязаны быть независимы от
    порядка объявления (два узла с одной картой дают **один** ``run_id``), а сам
    ``CrawlConfig`` остаётся формой ввода (порядок узлов — операторский, для чтения).
    Host'ы уже нормализованы правилом лимитера
    (:func:`core.util.rate_limit.normalize_host`) валидатором конфига.
    """
    shards = getattr(app, "shards", None)
    if not shards:
        return None
    return tuple(
        (str(shard.node), tuple(sorted(shard.hosts)))
        for shard in sorted(shards, key=lambda entry: str(entry.node))
    )


def _app_explicit(app: object, name: str) -> bool:
    """Задана ли ось **явно** в app-слое (Pydantic ``model_fields_set``).

    Дефолт app не должен делать дайджест другим (байт-идентичность старых прогонов §13).
    """
    return name in getattr(app, "model_fields_set", set())


def _axis_explicit(
    app: object, name: str, run_value: object, site_value: object
) -> bool:
    """«Явно задано» для оси: site/run-слой не ``None`` либо app-поле передано явно."""
    return bool(
        site_value is not None
        or run_value is not None
        or _app_explicit(app, name)
    )


def resolve_crawl_limits(
    url: str,
    *,
    app: CrawlConfig,
    run: CrawlRunConfig | None = None,
    profiles: Mapping[str, DomainProfile] | None = None,
    derive_extract_workers: bool = False,
    mem_budget_mb: int | None = None,
) -> CrawlLimits:
    """Разрешает :class:`CrawlLimits` для ``url`` по цепочке app < run < site.

    Per-site слой выбирается лучшим ``host[+path]``-профилем домена
    (:func:`core.pipeline.fetch_plan.select_domain_profile`) — тем же матчингом, что и
    ``FetchPlan``. Для каждого вида проксимального слоя побеждает высший не-``None``.

    **F0: окно экстракции из бюджета памяти.** Если ``extract_workers`` не задан ни на
    одном слое (app/run/site), ``derive_extract_workers=True`` и бюджет объявлен
    (``mem_budget_mb``), окно выводится как
    ``floor((budget − parent_floor) / peak(level))``
    (:func:`core.pipeline.resources.workers_from_budget`). Условие «экстрактор локальный»
    проверяет **вызывающий** (под ``split`` воркеры живут на edge и там свой бюджет):
    сюда приходит уже решённый флаг.

    Бюджет — **явный аргумент**, а не чтение окружения: у чистого пайплайна нет права
    зависеть от памяти хоста (иначе тесты и ``run_id``-независимость стали бы функцией
    машины). Composition root резолвит его один раз
    (:func:`core.pipeline.resources.resolve_mem_budget_mb`) и передаёт дальше.

    Для ``document_level=off`` окно не выводится: документной экстракции нет, а ноль
    воркеров означал бы «не помещается» — два разных нуля смешивать нельзя.

    **P7: ``pdf_pipeline`` и окно извлечения.** Пики памяти в
    :mod:`core.pipeline.resources` измерены для docling (``structured``); для
    ``text_layer`` измерения ещё нет, поэтому окно выводится по **структурным** пикам —
    консервативно (воркеров меньше, чем поместилось бы), но без выдуманного числа.
    Это видимое ограничение, а не потолок: оператор может объявить
    ``crawl.extract_workers`` явно.
    """
    profile = select_domain_profile(url, profiles or {})

    # Per-site слой читается **с типизированного профиля**, а не через ``getattr`` по
    # имени-строке: имя стирало тип до ``object``, и каждая ось ниже приводилась к
    # ``int``/``str`` вручную — то есть проверка типа подменялась приведением. Имена
    # полей здесь те же, что объявлены в :class:`DomainProfile`, и это проверяет mypy.
    site_max_workers = profile.max_workers if profile is not None else None
    site_extract_workers = profile.extract_workers if profile is not None else None
    site_extract_max_tasks = (
        profile.extract_max_tasks_per_child if profile is not None else None
    )
    site_max_response_bytes = profile.max_response_bytes if profile is not None else None
    site_max_depth = profile.max_depth if profile is not None else None
    site_max_documents = profile.max_documents if profile is not None else None
    site_revalidate_max_documents = (
        profile.revalidate_max_documents if profile is not None else None
    )
    site_exclude_paths = profile.exclude_paths if profile is not None else None
    site_exclude_documents = profile.exclude_documents if profile is not None else None
    site_document_level = profile.document_level if profile is not None else None
    site_pdf_pipeline = profile.pdf_pipeline if profile is not None else None
    site_document_timeout = profile.document_timeout_s if profile is not None else None
    site_max_artifact_bytes = profile.max_artifact_bytes if profile is not None else None
    site_attachment_formats = profile.attachment_formats if profile is not None else None
    site_archive_max_member_bytes = (
        profile.archive_max_member_bytes if profile is not None else None
    )
    site_archive_max_uncompressed_bytes = (
        profile.archive_max_uncompressed_bytes if profile is not None else None
    )
    site_archive_max_members = (
        profile.archive_max_members if profile is not None else None
    )
    site_archive_max_total_uncompressed_bytes = (
        profile.archive_max_total_uncompressed_bytes if profile is not None else None
    )
    site_archive_descent = profile.archive_descent if profile is not None else None

    run_max_workers = run.max_workers if run is not None else None
    # F1 (план 17): app < run, без per-site слоя — число полос есть свойство узла.
    run_host_concurrency = (
        getattr(run, "host_concurrency", None) if run is not None else None
    )
    app_host_concurrency = getattr(app, "host_concurrency", None)
    host_concurrency = (
        run_host_concurrency
        if run_host_concurrency is not None
        else app_host_concurrency
    )
    # Темп: цепочка **той же формулы**, что исполнение (``resolve_fetch_plan``) —
    # ``site > run``, app-слой темпа не объявляет вовсе. Вторая копия правила разошлась бы
    # с ``FetchPlan`` молча: ``CrawlLimits`` попал бы в дайджест с одним темпом, а
    # лимитер исполнял бы другой (это проверяется тестом «одна формула»). Канонизация —
    # ``canonical_rate_limit``: ``{min_s: 0.05, max_s: 0.2}`` и ``{5e-02, 2e-01}`` дают
    # одну строку, то есть один дайджест.
    run_rate_limit = getattr(run, "rate_limit", None) if run is not None else None
    site_rate_limit = profile.rate_limit if profile is not None else None
    rate_limit_spec = (
        site_rate_limit if site_rate_limit is not None else run_rate_limit
    )
    rate_limit = canonical_rate_limit(
        rate_limit_spec if isinstance(rate_limit_spec, RateLimitSpec) else None
    )
    run_extract_workers = (
        getattr(run, "extract_workers", None) if run is not None else None
    )
    # F6 (план 17, §4.3): watermark бэклога — app < run, без per-site слоя (бэклог — это
    # диск и потребитель узла, а не свойство сайта).
    app_extract_backlog_mb = getattr(app, "extract_backlog_mb", None)
    run_extract_backlog_mb = (
        getattr(run, "extract_backlog_mb", None) if run is not None else None
    )
    extract_backlog_mb = _pick(app_extract_backlog_mb, run_extract_backlog_mb, None)
    run_extract_max_tasks = (
        getattr(run, "extract_max_tasks_per_child", None) if run is not None else None
    )
    run_max_response_bytes = (
        getattr(run, "max_response_bytes", None) if run is not None else None
    )
    run_max_depth = run.max_depth if run is not None else None
    run_max_documents = run.max_documents if run is not None else None
    run_revalidate_max_documents = (
        getattr(run, "revalidate_max_documents", None) if run is not None else None
    )
    run_exclude_paths = run.exclude_paths if run is not None else None
    run_exclude_documents = run.exclude_documents if run is not None else None
    run_document_level = (
        getattr(run, "document_level", None) if run is not None else None
    )
    # P6: оси допуска контейнеров и границ спуска (та же цепочка app < run < site).
    run_max_artifact_bytes = (
        getattr(run, "max_artifact_bytes", None) if run is not None else None
    )
    run_attachment_formats = (
        getattr(run, "attachment_formats", None) if run is not None else None
    )
    run_archive_max_member_bytes = (
        getattr(run, "archive_max_member_bytes", None) if run is not None else None
    )
    run_archive_max_uncompressed_bytes = (
        getattr(run, "archive_max_uncompressed_bytes", None)
        if run is not None
        else None
    )
    run_archive_max_members = (
        getattr(run, "archive_max_members", None) if run is not None else None
    )
    # B3: кумулятивный бюджет **корня** спуска (все уровни вложенности). Отдельная ось,
    # потому что оси ``archive_max_*`` ограничивают один контейнер.
    run_archive_max_total_uncompressed_bytes = (
        getattr(run, "archive_max_total_uncompressed_bytes", None)
        if run is not None
        else None
    )
    run_archive_descent = (
        getattr(run, "archive_descent", None) if run is not None else None
    )

    max_workers = _pick(app.max_workers, run_max_workers, site_max_workers)
    # P5: уровень документной экстракции резолвится **до** окна извлечения: пик памяти —
    # функция уровня (``resources.DOCLING_PEAK_MB``), поэтому вывести воркеры без уровня
    # нельзя. Порядок резолва (app < run < site) от перестановки не меняется.
    document_level = _pick(
        getattr(app, "document_level", None),
        run_document_level,
        site_document_level,
    )
    resolved_level = (
        document_level
        if isinstance(document_level, DocumentLevel)
        else DocumentLevel(document_level)
        if document_level is not None
        else DocumentLevel.FULL
    )
    # P9: устройство моделей извлечения PDF — та же цепочка app < run, но **без**
    # per-site слоя: устройство принадлежит хосту исполнения, а не сайту (два корня на
    # одной машине не могут исполняться на разных устройствах). Резолвится **до** окна
    # извлечения: под ``cuda`` вторым ограничением окна становится видеопамять
    # (:func:`core.pipeline.resources.workers_from_budget`). Формула одна —
    # :func:`resolve_pdf_device`, её же зовёт composition root для ``extractor_stack``:
    # вторая копия разошлась бы с манифестом молча.
    resolved_pdf_device = resolve_pdf_device(app, run)
    extract_workers = _pick(
        getattr(app, "extract_workers", None),
        run_extract_workers,
        site_extract_workers,
    )
    # F0: окно не объявлено ни на одном слое ⇒ вывести из бюджета памяти (только для
    # локального экстрактора — флаг ставит вызывающий). ``off`` исключён: воркеров не
    # требуется, и 0 там означал бы не «не помещается», а «не нужно».
    extract_workers_derived = False
    if (
        extract_workers is None
        and derive_extract_workers
        and resolved_level is not DocumentLevel.OFF
        and mem_budget_mb is not None
    ):
        extract_workers = workers_from_budget(
            resolved_level, mem_budget_mb, pdf_device=resolved_pdf_device
        )
        extract_workers_derived = True
    extract_max_tasks = _pick(
        getattr(app, "extract_max_tasks_per_child", None),
        run_extract_max_tasks,
        site_extract_max_tasks,
    )
    app_max_response_bytes = getattr(app, "max_response_bytes", None)
    max_response_bytes = _pick(
        app_max_response_bytes, run_max_response_bytes, site_max_response_bytes
    )
    # «Явно задано» — поле пришло на site/run-слое либо было передано в app-конструктор.
    # Дефолт app не делает дайджест другим (байт-идентичность старых прогонов §13).
    max_response_bytes_explicit = bool(
        site_max_response_bytes is not None
        or run_max_response_bytes is not None
        or "max_response_bytes" in getattr(app, "model_fields_set", set())
    )
    max_depth = _pick(app.max_depth, run_max_depth, site_max_depth)
    effective_max_depth = 0 if max_depth is None else int(max_depth)
    # D1/§10: egress-политика ``robots.txt``. Поле несёт только app-слой
    # (``deploy.yaml:crawl.robots``), поэтому цепочка app < run < site сводится к app;
    # ``getattr`` с дефолтом оставляет место для будущих слоёв без второй формулы.
    # ``None`` = автодетект — то же правило, что исполняет корпус (``max_depth > 0`` или
    # per-site ``sitemap``), чтобы поведение и ``crawl_config_digest`` не разошлись.
    explicit_robots = _pick(
        getattr(app, "robots", None),
        getattr(run, "robots", None) if run is not None else None,
        getattr(profile, "robots", None) if profile is not None else None,
    )
    site_sitemap = getattr(profile, "sitemap", None) if profile is not None else None
    robots = (
        bool(explicit_robots)
        if explicit_robots is not None
        else (effective_max_depth > 0 or bool(site_sitemap))
    )
    max_documents = _pick(
        app.max_documents, run_max_documents, site_max_documents
    )
    revalidate_max_documents = _pick(
        getattr(app, "revalidate_max_documents", None),
        run_revalidate_max_documents,
        site_revalidate_max_documents,
    )
    exclude_paths = _pick(
        app.exclude_paths, run_exclude_paths, site_exclude_paths
    )
    exclude_documents = _pick(
        app.exclude_documents, run_exclude_documents, site_exclude_documents
    )
    document_level = _pick(
        getattr(app, "document_level", None),
        run_document_level,
        site_document_level,
    )
    # P7: механизм извлечения PDF — та же цепочка app < run < site (site > run > app).
    run_pdf_pipeline = (
        getattr(run, "pdf_pipeline", None) if run is not None else None
    )
    pdf_pipeline = _pick(
        getattr(app, "pdf_pipeline", None),
        run_pdf_pipeline,
        site_pdf_pipeline,
    )
    # P8: таймаут документа — та же цепочка app < run < site. ``None`` на всех слоях =
    # ось не объявлена ⇒ действует прежнее зашитое значение (:data:`DEFAULT_DOCUMENT_TIMEOUT_S`),
    # поэтому ``as_dict`` не получает ключ и ``run_id`` не меняется.
    run_document_timeout = (
        getattr(run, "document_timeout_s", None) if run is not None else None
    )
    document_timeout = _pick(
        getattr(app, "document_timeout_s", None),
        run_document_timeout,
        site_document_timeout,
    )
    # P6: контейнеры. ``None`` на site/run — «наследовать»; app-дефолт всегда есть.
    app_max_artifact_bytes = getattr(app, "max_artifact_bytes", None)
    max_artifact_bytes = _pick(
        app_max_artifact_bytes, run_max_artifact_bytes, site_max_artifact_bytes
    )
    app_attachment_formats = getattr(app, "attachment_formats", None)
    attachment_formats = _pick(
        app_attachment_formats, run_attachment_formats, site_attachment_formats
    )
    app_archive_max_member_bytes = getattr(app, "archive_max_member_bytes", None)
    archive_max_member_bytes = _pick(
        app_archive_max_member_bytes,
        run_archive_max_member_bytes,
        site_archive_max_member_bytes,
    )
    app_archive_max_uncompressed_bytes = getattr(
        app, "archive_max_uncompressed_bytes", None
    )
    archive_max_uncompressed_bytes = _pick(
        app_archive_max_uncompressed_bytes,
        run_archive_max_uncompressed_bytes,
        site_archive_max_uncompressed_bytes,
    )
    app_archive_max_members = getattr(app, "archive_max_members", None)
    archive_max_members = _pick(
        app_archive_max_members, run_archive_max_members, site_archive_max_members
    )
    app_archive_max_total_uncompressed_bytes = getattr(
        app, "archive_max_total_uncompressed_bytes", None
    )
    archive_max_total_uncompressed_bytes = _pick(
        app_archive_max_total_uncompressed_bytes,
        run_archive_max_total_uncompressed_bytes,
        site_archive_max_total_uncompressed_bytes,
    )
    app_archive_descent = getattr(app, "archive_descent", None)
    archive_descent = _pick(
        app_archive_descent, run_archive_descent, site_archive_descent
    )

    return CrawlLimits(
        max_workers=None if max_workers is None else int(max_workers),
        host_concurrency=(
            None if host_concurrency is None else int(host_concurrency)
        ),
        host_concurrency_explicit=_axis_explicit(
            app, "host_concurrency", run_host_concurrency, None
        ),
        # Темп: объявление на site/run слое включает ключ дайджеста. Значение уже
        # канонизировано выше — сравнение строковое и не зависит от формата чисел.
        rate_limit=rate_limit,
        rate_limit_explicit=_axis_explicit(
            app, "rate_limit", run_rate_limit, site_rate_limit
        ),
        extract_backlog_mb=(
            None
            if extract_backlog_mb is None
            else int(extract_backlog_mb)
        ),
        extract_backlog_mb_explicit=_axis_explicit(
            app, "extract_backlog_mb", run_extract_backlog_mb, None
        ),
        # F7 (17 §2): карта шардов — app-слой без run/site: шард принадлежит
        # развёртыванию. ``None`` — не объявлена (дайджест и ``run_id`` прежние).
        host_shards=_declared_host_shards(app),
        extract_workers=(
            None if extract_workers is None else int(extract_workers)
        ),
        # F0: ``True`` — окно выведено из бюджета памяти, а не объявлено оператором.
        # Наблюдаемость расписания (``metrics.json.resources.scheduling``); в дайджест
        # не входит (см. :meth:`CrawlLimits.as_dict`).
        extract_workers_derived=extract_workers_derived,
        extract_max_tasks_per_child=(
            None
            if extract_max_tasks is None
            else int(extract_max_tasks)
        ),
        max_response_bytes=(
            DEFAULT_MAX_RESPONSE_BYTES
            if max_response_bytes is None
            else int(max_response_bytes)
        ),
        max_response_bytes_explicit=max_response_bytes_explicit,
        max_artifact_bytes=(
            DEFAULT_MAX_ARTIFACT_BYTES
            if max_artifact_bytes is None
            else int(max_artifact_bytes)
        ),
        max_artifact_bytes_explicit=_axis_explicit(
            app, "max_artifact_bytes", run_max_artifact_bytes, site_max_artifact_bytes
        ),
        attachment_formats=normalize_attachment_formats(
            attachment_formats
        ),
        attachment_formats_explicit=_axis_explicit(
            app,
            "attachment_formats",
            run_attachment_formats,
            site_attachment_formats,
        ),
        archive_max_member_bytes=(
            DEFAULT_ARCHIVE_MAX_MEMBER_BYTES
            if archive_max_member_bytes is None
            else int(archive_max_member_bytes)
        ),
        archive_max_member_bytes_explicit=_axis_explicit(
            app,
            "archive_max_member_bytes",
            run_archive_max_member_bytes,
            site_archive_max_member_bytes,
        ),
        archive_max_uncompressed_bytes=(
            DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES
            if archive_max_uncompressed_bytes is None
            else int(archive_max_uncompressed_bytes)
        ),
        archive_max_uncompressed_bytes_explicit=_axis_explicit(
            app,
            "archive_max_uncompressed_bytes",
            run_archive_max_uncompressed_bytes,
            site_archive_max_uncompressed_bytes,
        ),
        archive_max_members=(
            None if archive_max_members is None else int(archive_max_members)
        ),
        archive_max_members_explicit=_axis_explicit(
            app, "archive_max_members", run_archive_max_members, site_archive_max_members
        ),
        archive_max_total_uncompressed_bytes=(
            DEFAULT_ARCHIVE_MAX_TOTAL_UNCOMPRESSED_BYTES
            if archive_max_total_uncompressed_bytes is None
            else int(archive_max_total_uncompressed_bytes)
        ),
        archive_max_total_uncompressed_bytes_explicit=_axis_explicit(
            app,
            "archive_max_total_uncompressed_bytes",
            run_archive_max_total_uncompressed_bytes,
            site_archive_max_total_uncompressed_bytes,
        ),
        archive_descent=ArchiveDescent(
            ArchiveDescent.FULL if archive_descent is None else archive_descent
        ),
        archive_descent_explicit=_axis_explicit(
            app, "archive_descent", run_archive_descent, site_archive_descent
        ),
        max_depth=effective_max_depth,
        robots=robots,
        max_documents=(
            None if max_documents is None else int(max_documents)
        ),
        revalidate_max_documents=(
            None
            if revalidate_max_documents is None
            else int(revalidate_max_documents)
        ),
        exclude_paths=tuple(exclude_paths or ()),
        exclude_documents=tuple(exclude_documents or ()),
        document_level=DocumentLevel(
            DocumentLevel.FULL if document_level is None else document_level
        ),
        pdf_pipeline=PdfPipeline(
            PdfPipeline.STRUCTURED if pdf_pipeline is None else pdf_pipeline
        ),
        pdf_device=resolved_pdf_device,
        document_timeout_s=(
            DEFAULT_DOCUMENT_TIMEOUT_S
            if document_timeout is None
            else float(document_timeout)
        ),
        document_timeout_s_explicit=_axis_explicit(
            app, "document_timeout_s", run_document_timeout, site_document_timeout
        ),
    )


def blacklist_match(url: str, limits: CrawlLimits) -> str | None:
    """Возвращает заматчивший паттерн блэклиста или ``None`` (D1/D2, N5).

    ``exclude_paths`` — по пути URL, ``exclude_documents`` — по basename. ``fnmatchcase``
    (case-sensitive) без regex/подстрок.

    **Leading-slash tolerance (N5).** Паттерн пути принимается и с ведущим ``/``, и без
    него: ``docs/*`` нормализуется к ``/docs/*`` (путь URL всегда начинается с ``/``),
    поэтому ``docs/*`` матчит ``/docs/x`` как и ``/docs/*``. Паттерн-``/`` (зачёркивает
    host-корень) сохраняется.

    **Caveat ``exclude_documents`` (N5).** Сравнивается basename **пути** URL (файл —
    сегмент пути, напр. ``/docs/report.pdf``). Имя файла, переданное **только** в
    query-строке (``/download?file=report.pdf``), basename не является и паттерном не
    ловится — это задокументированное ограничение (см. ``03`` §D.2), а не догадка.
    """
    path = urlsplit(url).path or "/"
    for pattern in limits.exclude_paths:
        normalized = "/" + pattern.lstrip("/")
        if fnmatchcase(path, normalized):
            return pattern
    base = path.rsplit("/", 1)[-1]
    for pattern in limits.exclude_documents:
        if fnmatchcase(base, pattern):
            return pattern
    return None


def is_blacklisted(url: str, limits: CrawlLimits) -> bool:
    """Зачёркнут ли ``url`` блэклистом (обёртка над :func:`blacklist_match`)."""
    return blacklist_match(url, limits) is not None


def root_path_prefix(root_url: str) -> str:
    """Path-prefix seed-root: ``""`` — нет префикса (host-корень), иначе путь без хвоста ``/``.

    ``https://h/`` → ``""`` (весь host в scope); ``https://h/documents/`` → ``/documents``.
    """
    path = urlsplit(root_url).path or "/"
    if path in ("", "/"):
        return ""
    return path.rstrip("/")


def _path_within(url_path: str, prefix: str) -> bool:
    """Матч пути по границе сегмента (та же семантика, что ``fetch_plan.py``)."""
    if not prefix:
        return True
    return url_path == prefix or url_path.startswith(prefix + "/")


def decoded_scope_path(path: str) -> str | None:
    """Percent-декодирует путь для scope-сравнения; ``None`` — dot-segment ``..`` (N2).

    Сырой путь сравнивать нельзя: ``%2e%2e`` сервер декодирует в ``..`` и обходит
    path-префикс (``/documents/%2e%2e/private`` обслуживается вне ``/documents``). Любой
    сегмент ``..`` **после** декодирования — вне scope (traversal), в т.ч. буквальный
    ``..`` (нулевая нормализация ``urllib`` его не сворачивает).
    """
    decoded = unquote(path or "/")
    if any(segment == ".." for segment in decoded.split("/")):
        return None
    return decoded


class Frontier:
    """Приоритетная BFS-очередь одного seed-root: scope/блэклист/лимиты при постановке.

    **Порядок обхода — ключ ``(depth, url)``, а не порядок поступления** (F3 плана 17,
    семантика ``crawl-2``). Очередь — двоичная куча (:mod:`heapq`) с ключом ``(depth, url)``:
    :meth:`next` отдаёт минимальную глубину, а внутри одной глубины — лексикографически
    меньший URL. Причина не в эстетике: при FIFO состав корпуса зависел от того, какая
    страница какой волны завершилась первой, то есть от планировщика (``max_workers``,
    задержки сети), а не от объявленного входа. Порядок, выведенный из ключа, делает
    «набор добытого» функцией объявленных входов — без этого требования T3/T4
    (порядко-независимость корпуса) недоказуемы в принципе.

    **Бюджет ``max_documents`` решается на изъятии, а не на постановке** (F3/A2):
    :meth:`discover`/:meth:`add_seed` ставят всё, что проходит scope/блэклист/глубину/дедуп
    (эти решения от порядка не зависят), а :meth:`next` возвращает ``None``, когда
    ``fetched`` достиг потолка, — с явной пометкой усечения. Так отказ перестаёт быть
    функцией момента готовности.

    Ключ дедупликации — нормализованный URL (см. ``core.util.html_links``) плюс
    :func:`url_form_key`, который схлопывает формы одного URL, различающиеся хвостовым ``/``
    (схлопывание видимо: :attr:`duplicate_form_count`). ``fetched`` считает успешно
    добытые (retained) страницы этого корня — для ``max_documents``. ``truncated``/
    ``truncated_reason`` фиксируют усечение (потолок документов или глубины) для
    провенанса (D1/G — «честность усечения»).
    """

    def __init__(
        self,
        root_url: str,
        limits: CrawlLimits,
        *,
        scope_hosts: Iterable[str] | None = None,
        shard_hosts: Iterable[str] | None = None,
    ) -> None:
        self.root_url = root_url
        parsed = urlsplit(root_url)
        self.root_host = (parsed.hostname or "").lower()
        self.path_prefix = root_path_prefix(root_url)
        self.limits = limits
        #: C (execution_change): **объявленные** хосты, которые дополнительно к host
        #: корня допускаются в scope (поддомены из ``domain_profiles.yaml``/seed-корней).
        #: Пусто ⇒ scope строго на host корня: поддомен, которого оператор не объявлял,
        #: больше не затягивается (учебный поддомен из корня корпоративного портала).
        #: Хосты, объявленные собственным seed-корнем продукта, вызывающий сюда **не**
        #: передаёт — у такого хоста свой корень и свой бюджет (иначе бюджет корня-
        #: предка снова тратится на чужой хост).
        self.scope_hosts = frozenset(
            host
            for host in (normalize_scope_host(raw) for raw in (scope_hosts or ()))
            if host
        )
        #: F7 (17 §2): host'ы **своего шарда** (``None`` — шардов нет, фильтра нет).
        #: Это **не** scope-предикат и он не подменяет :meth:`host_in_scope`: scope
        #: отвечает «принадлежит ли URL этому корню», шард — «моё ли это железо».
        #: Порядок проверок поэтому жёсткий: сперва scope, затем шард (см.
        #: :meth:`scope_admission`/`shard_admission`). Смешать их значило бы объявить
        #: чужой host «вне scope» — ложное утверждение о продукте, а не о развёртывании.
        #: Нормализация здесь та же, что у лимитера
        #: (:func:`core.util.rate_limit.normalize_host`): ключ вежливости — это и есть
        #: ключ шарда. Поэтому ``www.example.com`` и
        #: ``example.com`` — **разные** ключи (у лимитера на них разные бюджеты), тогда
        #: как scope считает их одним сайтом. Оператор объявляет в шарде те host'ы,
        #: которые реально фетчатся; несовпадение видно счётчиком ``host_out_of_shard``,
        #: а не молчаливым дропом.
        self.shard_hosts = (
            None
            if shard_hosts is None
            else frozenset(
                host for host in (normalize_host(raw) for raw in shard_hosts) if host
            )
        )
        #: Очередь **discovery** — двоичная куча с ключом ``(depth, url)`` (F3 плана 17).
        #: Кортеж именно в этом порядке полей: сравнение идёт по глубине, затем по URL,
        #: поэтому индекс ``0`` — глубина, ``1`` — URL (прежний FIFO-порядок полей был
        #: обратным, поэтому все чтения головы переписаны явно).
        self._queue: list[tuple[int, str]] = []
        #: Очередь **ревалидации** известных URL (depth-0 листья) — отдельная куча с тем
        #: же ключом. Отдельная не для скорости, а для бюджета: ревалидация его **не**
        #: расходует (``09`` §6.3), поэтому при исчерпанном ``max_documents`` она обязана
        #: обслуживаться, а discovery — нет. Один общий heap этого не выражает: узел
        #: discovery с меньшим ключом навсегда закрыл бы голову. Пока бюджет есть,
        #: :meth:`next` берёт минимум из двух голов — порядок совпадает с единым heap.
        self._revalidation_queue: list[tuple[int, str]] = []
        self._seen: set[str] = set()
        #: Ключи дедупа (:func:`url_form_key`) — **вторая** форма одного URL не даёт
        #: второго GET. Хранится отдельно от ``_seen``: точные строки нужны для
        #: ``is_revalidation`` и провенанса, а схлопывание форм — только для решения
        #: «ставить ли в очередь».
        self._seen_forms: set[str] = set()
        #: Схлопнутые формы URL: точный счётчик + ограниченные примеры
        #: (:data:`DUPLICATE_FORM_EXAMPLES_MAX`). Тихое отбрасывание URL запрещено —
        #: это тот же класс дефекта, что «тихий обход path-prefix».
        self.duplicate_form_count = 0
        self.duplicate_form_examples: list[str] = []
        self._duplicate_form_seen: set[str] = set()
        #: Известные URL ревалидации (depth-0 листья): поставлены отдельным путём,
        #: не расходуют ``max_documents`` и не расширяют BFS исходящими ссылками.
        self._revalidation: set[str] = set()
        self.fetched = 0
        #: Фактически добытые ревалидационные страницы (не 304/reuse): отдельный
        #: счётчик, потому что ``fetched``/``pages_fetched`` остаются discovery-only.
        self.revalidation_fetched = 0
        self.truncated = False
        self.truncated_reason: str | None = None
        #: План 20 §3: начало дренажа этого корня (ставит ``root_lifecycle``; ``None`` —
        #: значение не проставлено, например в одиночных проверках фронтьера).
        self.started_at: str | None = None
        #: План 20 §3: сколько URL корня отказано **robots.txt**. Отказ не делает обход
        #: неполным «по усечению», но часть материала сайт запретил — снятие не вправе
        #: назвать корень исчерпанным. Счётчик ведёт дренаж (``_commit_outcome``).
        self.robots_refused = 0
        #: Seeds/ссылки, отброшенные блэклистом (наблюдаемость: «не молчаливый дроп»).
        self.skipped_blacklisted: list[str] = []
        #: P6b: терминальные артефакты, допущенные **вне** path-prefix корня (точный счётчик
        #: + ограниченные примеры). Обход обязан быть видимым в провенансе фазы.
        self.artifacts_outside_prefix_count = 0
        self.artifacts_outside_prefix: list[str] = []
        self._bypass_seen: set[str] = set()
        #: P5: документы, пропущенные уровнем ``off`` (pre-fetch по суффиксу / post-fetch
        #: по content-type). Записи ``{"url", "reason"}`` (`url_suffix`/`content_type`) —
        #: решение обязано быть видимым (никогда не тихо), и питать канал усечения/аномалии.
        self.skipped_documents: list[dict[str, str]] = []
        #: C (execution_change): отвергнутые поддомены корня — точный счётчик по хостам
        #: (``{"edu.example.org": 664}``) и ограниченные примеры URL. Отказ виден:
        #: «сколько дали поддомены» — число, а не догадка (A: ничего не теряется молча).
        self.out_of_scope_subdomains: dict[str, int] = {}
        self.out_of_scope_subdomain_examples: dict[str, list[str]] = {}
        self._subdomain_reject_seen: set[str] = set()
        #: Принятые элементы с хоста, отличного от хоста корня (объявленный поддомен):
        #: сколько бюджета корня ушло «не на свой хост» — видно по корню.
        self.accepted_foreign_hosts: dict[str, int] = {}
        #: F7 (17 §2): host'ы **чужого шарда**, не поставленные в очередь: точный счётчик
        #: по host'ам + ограниченные примеры URL. Причина `host_out_of_shard` — не
        #: «нет доступа» и не «вне scope»: материал принадлежит другому узлу, и этот узел
        #: его не видел. Публикуется числом (по образцу ``out_of_scope_subdomains``) —
        #: иначе шардированный узел молча отдавал бы часть корпуса за целый.
        self.host_out_of_shard: dict[str, int] = {}
        self.host_out_of_shard_examples: dict[str, list[str]] = {}
        self._shard_reject_seen: set[str] = set()
        #: F8 (17 §7.2 п.2): объявленная вежливость **по host'ам**, к которым этот корень
        #: делал живые попытки: ``{host: {"min_s": …, "max_s": …}}``. Носитель — корень, а
        #: не конфиг: один корень штатно фетчит несколько host'ов (``domain_profiles``), и
        #: у них разные минимумы. Значение пишет драйвер (:func:`note_rate_limit`) из
        #: **того** плана, которым пойдёт запрос, — второго резолва нет.
        self.rate_limits: dict[str, dict[str, float]] = {}

    # -- scope -------------------------------------------------------------- #

    def host_in_scope(self, host: str) -> bool:
        """Допущен ли ``host`` в scope корня (C: объявленные хосты, ``www.`` эквивалентен).

        Предикат (псевдокод)::

            h = normalize(host); r = normalize(root_host)
            return h == r or (h.endswith("." + r) and h in scope_hosts)

        * host корня — всегда (в т.ч. ``www.``-вариант: ``example.org`` ≡ ``www.example.org``);
        * поддомен корня — только если объявлен оператором (``scope_hosts``);
        * чужой host и необъявленный поддомен — вне scope.

        Отличие от :func:`core.pipeline.fetch_plan._host_matches` намеренное: там семантика
        **профиля** (поддомен наследует оси родительского ключа — это верно и нужно), здесь —
        семантика **краула** (наследование осей ≠ право тратить бюджет корня).
        """
        norm = normalize_scope_host(host)
        root = normalize_scope_host(self.root_host)
        if not norm or not root:
            return False
        if norm == root:
            return True
        if not norm.endswith("." + root):
            return False
        return norm in self.scope_hosts

    def in_scope(self, url: str, *, terminal_artifact: bool = False) -> bool:
        """Проходит ли ``url`` scope корня (SSRF + объявленный host + path-prefix по границе).

        ``terminal_artifact=True`` — path-prefix **не применяется** (P6b: документ или
        заявленный контейнер не продолжает навигацию, поэтому «раздел» к нему не относится).
        Флаг при этом **не** универсальный пропуск: URL обязан действительно принадлежать
        классу (:meth:`is_terminal_artifact`), иначе — ``False``. Host scope (C: только
        объявленные хосты), SSRF-валидация и traversal-guard остаются обязательными: обход
        path-prefix не должен становиться выходом из scope.

        Путь перед сравнением percent-декодируется, а ``..``-сегменты отвергаются (N2):
        иначе ``%2e%2e`` обошёл бы префикс — в том числе при ``terminal_artifact=True``.
        """
        try:
            validate_url(url)
        except ValueError:
            return False
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        if not host or not self.host_in_scope(host):
            return False
        decoded = decoded_scope_path(parsed.path or "/")
        if decoded is None:
            return False
        if terminal_artifact:
            return self.is_terminal_artifact(url)
        return _path_within(decoded, self.path_prefix)

    def is_terminal_artifact(self, url: str) -> bool:
        """Класс «терминальный артефакт» по URL-суффиксу (pre-fetch; P6b).

        Документ (``pdf``/``docx``/``pptx``/``xlsx``) либо контейнер из заявленного набора
        ``attachment_formats``. Пост-фетч класс по ``Content-Type`` здесь не нужен: URL к
        этому моменту уже в очереди.
        """
        return is_terminal_artifact_url(url, self.limits.attachment_formats)

    def scope_admission(self, url: str) -> str:
        """Исход scope-допуска: ``SCOPE_IN`` | ``SCOPE_ARTIFACT_OUTSIDE_PREFIX`` | ``SCOPE_OUT``.

        Единственная точка решения «в scope / терминальный артефакт вне префикса / вне
        scope». Обход **не** записывается здесь: вызывающий фиксирует его
        :meth:`note_artifact_bypass` только когда URL действительно принят в очередь —
        иначе провенанс объявлял бы обход, которого не было (блэклист/бюджет/глубина).
        """
        if self.in_scope(url):
            return SCOPE_IN
        if self.in_scope(url, terminal_artifact=True):
            return SCOPE_ARTIFACT_OUTSIDE_PREFIX
        self.note_out_of_scope_subdomain(url)
        return SCOPE_OUT

    def note_out_of_scope_subdomain(self, url: str) -> None:
        """Учесть отвергнутый **поддомен** хоста корня (C: отказ не молчаливый).

        Считается только поддомен (``x.root``; сам host корня и ``www.``-вариант — не
        поддомен), точным счётчиком по хостам + ограниченными примерами на хост
        (:data:`SUBDOMAIN_REJECT_EXAMPLES_MAX`). Дедуп по URL: один и тот же отвергнутый
        URL предлагается со многих страниц, а «сколько дали поддомены» — вопрос про URL,
        не про число упоминаний. Запись попадает в провенанс корня
        (``out_of_scope_subdomains``).
        """
        host = normalize_scope_host(urlsplit(url).hostname or "")
        root = normalize_scope_host(self.root_host)
        if not host or not root or host == root or not host.endswith("." + root):
            return
        if url in self._subdomain_reject_seen:
            return
        self._subdomain_reject_seen.add(url)
        self.out_of_scope_subdomains[host] = self.out_of_scope_subdomains.get(host, 0) + 1
        examples = self.out_of_scope_subdomain_examples.setdefault(host, [])
        if len(examples) < SUBDOMAIN_REJECT_EXAMPLES_MAX:
            examples.append(url)

    def note_host_accept(self, url: str) -> None:
        """Учесть принятый элемент с хоста, отличного от хоста корня (C).

        «Сколько бюджета корня ушло на объявленный поддомен» — число рядом с бюджетом
        корня. Вызывается только по факту постановки в очередь (не при отказе).
        """
        host = normalize_scope_host(urlsplit(url).hostname or "")
        root = normalize_scope_host(self.root_host)
        if not host or host == root:
            return
        self.accepted_foreign_hosts[host] = self.accepted_foreign_hosts.get(host, 0) + 1

    # -- шард узла (F7) ------------------------------------------------------ #

    def host_in_shard(self, host: str) -> bool:
        """Принадлежит ли ``host`` **моему** шарду (``True``, если шардов нет).

        Ключ — тот же, что у лимитера (:func:`core.util.rate_limit.normalize_host`):
        шардирование единицы вежливости не вводит второй ключ. Предикат отдельный от
        :meth:`host_in_scope` намеренно: scope отвечает на вопрос о **продукте** («этот
        URL принадлежит корню»), шард — о **развёртывании** («это моё железо»). Отказ по
        шарду, записанный как «вне scope», был бы ложным утверждением о продукте.
        """
        if self.shard_hosts is None:
            return True
        return normalize_host(host) in self.shard_hosts

    def shard_admission(self, url: str) -> bool:
        """Допускает ли мой шард ``url``; при отказе пишет видимую запись.

        Вызывается **рядом** со scope-проверкой и только после неё (иначе чужой host
        считался бы «вне scope»). Отказ виден счётчиком по host'ам + примерами
        (:data:`SUBDOMAIN_REJECT_EXAMPLES_MAX` на host) — «пропустили host» никогда не
        бывает молчаливым.
        """
        if self.shard_hosts is None:
            return True
        host = normalize_host(urlsplit(url).hostname or "")
        if host in self.shard_hosts:
            return True
        if url not in self._shard_reject_seen:
            self._shard_reject_seen.add(url)
            self.host_out_of_shard[host] = self.host_out_of_shard.get(host, 0) + 1
            examples = self.host_out_of_shard_examples.setdefault(host, [])
            if len(examples) < SUBDOMAIN_REJECT_EXAMPLES_MAX:
                examples.append(url)
        # Материал этого корня лежит на другом узле ⇒ корпус **не** полон по
        # построению. Это усечение, а не отказ доступа: причина названа, а не
        # спрятана (иначе coverage объявил бы полный охват на части корпуса — I2/I2c).
        self._mark_truncated("host_out_of_shard")
        return False

    def note_artifact_bypass(self, url: str) -> None:
        """Записать видимый обход path-prefix терминальным артефактом (P6b).

        Счётчик точный и дедуплицированный по URL; примеры ограничены
        (:data:`ARTIFACT_BYPASS_EXAMPLES_MAX`) — сотни PDF одного раздела не должны топить
        провенанс. Запись попадает в ``artifacts_outside_prefix`` провенанса фазы.
        """
        if url in self._bypass_seen:
            return
        self._bypass_seen.add(url)
        self.artifacts_outside_prefix_count += 1
        if len(self.artifacts_outside_prefix) < ARTIFACT_BYPASS_EXAMPLES_MAX:
            self.artifacts_outside_prefix.append(url)

    def blacklisted(self, url: str) -> bool:
        """Блэклист (путь + basename) — см. :func:`blacklist_match`."""
        return is_blacklisted(url, self.limits)

    # -- capacity ----------------------------------------------------------- #

    @property
    def has_capacity(self) -> bool:
        """Есть ли место под ещё одну **добытую** страницу по ``max_documents``.

        Предикат бюджета **изъятия**: им пользуется :meth:`next` (и дренаж через
        ``corpus_drain._has_fetch_capacity``), а не постановка в очередь. На постановке бюджет не
        проверяется намеренно (F3/A2): иначе набор поставленных — а значит и добытых —
        URL зависел бы от порядка завершения страниц, то есть от планировщика.
        """
        return self.limits.max_documents is None or self.fetched < self.limits.max_documents

    def note_fetched(self) -> None:
        """Учесть успешно добытую (retained) страницу этого корня."""
        self.fetched += 1

    def note_robots_refusal(self) -> None:
        """План 20 §3: учесть отказ ``robots.txt`` в этом корне (видимость неполноты)."""
        self.robots_refused += 1

    def note_document_skip(self, url: str, reason: str) -> None:
        """P5: отметить пропуск документа уровнем ``off`` (видимость решения).

        ``reason`` — ``url_suffix`` (pre-fetch skip) либо ``content_type`` (post-fetch).
        Идемпотентности нет намеренно: дубликат-совпадение не пропускается, но URL в
        потоке дренажа уникален (dedup frontier).
        """
        self.skipped_documents.append({"url": url, "reason": reason})

    def note_rate_limit(self, host: str | None, spec: RateLimitSpec | None) -> None:
        """F8 (17 §7.2 п.2): запомнить объявленную вежливость попытки этого корня.

        ``host`` — ключ лимитера (``LocalFetch.host``); нормализуется тем же правилом
        (:func:`core.util.rate_limit.normalize_host`), иначе в провенансе завелось бы два
        ключа на один сайт и сравнение с наблюдённым интервалом шло бы не с тем числом.
        ``spec`` — ``rate_limit`` **того плана, которым идёт запрос**
        (``desc.fetch_plan.rate_limit`` после резолва профиля сайта): именно поэтому
        значение не вычисляется вторым путём — второй резолв разошёлся бы с фактическим
        пейсингом (тот же класс дефекта, что «второе вычисление ``route``» у потребителя
        очереди).

        Пишется **как объявлено**, включая ``min_s == 0`` («без троттлинга»): ноль —
        объявленный факт, отсутствие ключа у host'а — «не объявлено». Разница решает,
        можно ли вычислить ``ratio`` (§7.2 №3): ``null`` против ``1.0``.

        Первое значение для host'а побеждает: объявленный минимум — свойство плана
        (объявленный вход), а не наблюдение, и повторная попытка того же host'а не может
        переписать вход измерения. Расхождение между **корнями** одного host'а читатель
        обязан назвать (``declared_min_interval_variants``), а не усреднить.
        """
        key = normalize_host(host or "")
        if not key or spec is None:
            return
        self.rate_limits.setdefault(
            key, {"min_s": float(spec.min_s), "max_s": float(spec.max_s)}
        )

    def note_consumed(self, count: int) -> None:
        """Учесть уже израсходованный бюджет корня (retained-страницы resume, B2).

        Инициализация из durable-элементов сеанса: resumed root **не может** превысить
        ``max_documents`` при повторном прогоне того же сеанса (иначе бюджет начинался бы
        с нуля и лишние страницы тихо добывались бы заново).
        """
        if count > 0:
            self.fetched += int(count)

    def mark_truncated(self, reason: str) -> None:
        """Отметить политико-усечённый обход (первый повод побеждает; D1/G, D3)."""
        self._mark_truncated(reason)

    # -- enqueue ------------------------------------------------------------ #

    def add_seed(self, url: str) -> str:
        """Ставит seed (глубина 0); seed **задаёт** scope, поэтому scope-чек опущен.

        Блэклист применяется и к seeds (D1/H: отфильтрованы seeds, записи каталогов и
        обнаруженные URL). Возвращает статус: ``"queued"`` | ``"blacklisted"`` | ``"dup"``
        | ``"host_out_of_shard"`` (F7: seed принадлежит чужому узлу — отказ видим,
        счётчиком ``host_out_of_shard``, и он же ставит усечение корня).

        Дедуп seed'а — **только по точной форме**: ``p`` и ``p/`` остаются разными
        утверждениями автора, потому что seed задаёт path-prefix корня (``09`` §7.2).
        Нормализованный ключ при этом попадает в множество форм, чтобы обнаруженная
        ссылка другой формы не дала второй GET.

        Бюджет ``max_documents`` здесь **не** проверяется (F3/A2): решение о нём
        принимается на изъятии (:meth:`next`), иначе набор поставленных URL зависел бы
        от порядка завершения страниц. Статус ``"truncated"`` поэтому больше не
        возвращается — усечение ставит :meth:`next`/дренаж.
        """
        if url in self._seen:
            return "dup"
        if not self.shard_admission(url):
            return "host_out_of_shard"
        if self.blacklisted(url):
            self.skipped_blacklisted.append(url)
            return "blacklisted"
        self._seen.add(url)
        self._seen_forms.add(url_form_key(url))
        heapq.heappush(self._queue, (0, url))
        self.note_host_accept(url)
        return "queued"

    def add_revalidate_seed(self, url: str) -> str:
        """Ставит известный URL ревалидации (depth-0 лист) **вне** бюджета discovery.

        Отдельный путь от :meth:`add_seed` намеренно: ``max_documents`` — потолок
        **документов, которые добавляет обход** (discovery), а не повторной проверки
        известной страницы. Иначе продукт с N известными URL исчерпывал бы бюджет и
        ложно рапортовал ``truncated``/``complete=False`` на каждом цикле.

        Семантика, которую путь **сохраняет**:

        * scope корня проверяется (URL вне scope — забота вызывающего: orphan-бакет),
          SSRF-валидация — внутри :meth:`in_scope`;
        * терминальный артефакт (документ/заявленный контейнер) известного URL вне
          path-prefix **допускается** (P6b: он не продолжает навигацию); host scope и
          SSRF обязательны, блэклист побеждает, обход видим
          (:meth:`note_artifact_bypass`);
        * блэклист — с видимой записью ``skipped_blacklisted`` (не тихий дроп);
        * дедуп — известный URL, переоткрытый ссылкой из BFS, не получает второй GET
          (304-экономия); форма, отличающаяся хвостовым ``/``, считается тем же URL и
          схлопывается **видимо** (:attr:`duplicate_form_count`).

        Возвращает ``"queued"`` | ``"blacklisted"`` | ``"out_of_scope"`` | ``"dup"``
        | ``"host_out_of_shard"`` (F7: известный URL принадлежит чужому узлу — отказ
        видим и ставит усечение корня, а не молча выпадает из корпуса).
        """
        if self._known_in_any_form(url):
            return "dup"
        admission = self.scope_admission(url)
        if admission == SCOPE_OUT:
            return "out_of_scope"
        # F7: шард проверяется **после** scope и до блэклиста — «чужое железо» не
        # расходует запись ``skipped_blacklisted`` и не притворяется scope-отказом.
        if not self.shard_admission(url):
            return "host_out_of_shard"
        if self.blacklisted(url):
            self.skipped_blacklisted.append(url)
            return "blacklisted"
        if admission == SCOPE_ARTIFACT_OUTSIDE_PREFIX:
            self.note_artifact_bypass(url)
        self._seen.add(url)
        self._seen_forms.add(url_form_key(url))
        self._revalidation.add(url)
        heapq.heappush(self._revalidation_queue, (0, url))
        self.note_host_accept(url)
        return "queued"

    def is_revalidation(self, url: str) -> bool:
        """Поставлен ли ``url`` путём ревалидации (лист, не расходует бюджет)."""
        return url in self._revalidation

    def next_is_revalidation(self) -> bool:
        """Голова **обслуживаемого** порядка — URL ревалидации (проверка бюджета)."""
        head = self._served_head()
        return head is not None and head[1]

    def revalidation_head_count(self, limit: int) -> int:
        """Сколько подряд URL ревалидации в голове обслуживаемого порядка (≤ ``limit``).

        Порядок — тот же, в котором отдаёт :meth:`next`: при исчерпанном бюджете
        обслуживается только ревалидация (весь её блок), иначе две головы сливаются по
        ключу ``(depth, url)``. Внутренняя раскладка кучи порядком обхода не является,
        поэтому первые ``limit`` элементов берутся через :func:`heapq.nsmallest`.
        """
        if limit <= 0:
            return 0
        if not self.has_capacity:
            return min(limit, len(self._revalidation_queue))
        discovery = heapq.nsmallest(limit, self._queue)
        revalidation = heapq.nsmallest(limit, self._revalidation_queue)
        next_discovery = discovery[0] if discovery else None
        count = 0
        for key in revalidation:
            if count >= limit:
                break
            if next_discovery is not None and next_discovery < key:
                # узел discovery в голове — непрерывный блок ревалидации кончился
                break
            count += 1
        return count

    def discover(self, url: str, depth: int) -> bool:
        """Ставит обнаруженный URL глубины ``depth`` при прохождении scope/блэклиста.

        Терминальный артефакт (документ/заявленный контейнер) вне path-prefix корня
        **допускается** (P6b), но host scope, SSRF-валидация, traversal-guard и блэклист
        оператора действуют как прежде — блэклист побеждает. Обход фиксируется
        (:meth:`note_artifact_bypass`) только по факту постановки в очередь.

        Бюджет ``max_documents`` здесь **не** проверяется (F3/A2): отказы, зависящие от
        момента готовности страниц, сделали бы состав корпуса функцией планировщика.
        Поэтому отказ остаётся только по scope/блэклисту/глубине/дедупу — эти решения
        от порядка не зависят.

        Дедуп — по **нормализованному** ключу (:func:`url_form_key`): ``/page`` и
        ``/page/`` дают один элемент очереди, а схлопывание видно в
        :attr:`duplicate_form_count`. В очередь идёт **исходная** форма URL.
        """
        if self._known_in_any_form(url):
            return False
        if depth > self.limits.max_depth:
            return False
        admission = self.scope_admission(url)
        if admission == SCOPE_OUT:
            return False
        if not self.shard_admission(url):
            return False
        if self.blacklisted(url):
            self.skipped_blacklisted.append(url)
            return False
        if admission == SCOPE_ARTIFACT_OUTSIDE_PREFIX:
            self.note_artifact_bypass(url)
        self._seen.add(url)
        self._seen_forms.add(url_form_key(url))
        heapq.heappush(self._queue, (depth, url))
        self.note_host_accept(url)
        return True

    def offer(self, links: Sequence[str], parent_depth: int) -> None:
        """Предлагает ссылки страницы глубины ``parent_depth`` к обходу.

        При ``parent_depth < max_depth`` ссылки ставятся на ``parent_depth + 1``. На
        границе (``parent_depth == max_depth``) ссылки уже не ставятся, но наличие
        in-scope/не-блэклистных ссылок фиксируется как усечение по глубине — **тихая**
        штатная остановка BFS, но честная для отчётности (D1/G). Должно вызываться только
        при ``max_depth > 0`` (иначе links не извлекаются вовсе — байт-в-байт поведение).
        """
        if parent_depth >= self.limits.max_depth:
            for url in links:
                if self.scope_admission(url) == SCOPE_OUT:
                    continue
                # F7: ссылка на host чужого шарда — не «глубина кончилась»: она
                # принадлежит другому узлу, и это уже названо ``host_out_of_shard``.
                if not self.shard_admission(url):
                    continue
                if not self.blacklisted(url):
                    self._mark_truncated("max_depth")
                    break
            return
        for url in links:
            self.discover(url, parent_depth + 1)

    # -- drain -------------------------------------------------------------- #

    def _served_head(self) -> tuple[tuple[int, str], bool] | None:
        """Голова **обслуживаемого** порядка: ``((depth, url), is_revalidation)``.

        Пока бюджет не исчерпан, обслуживаемый порядок — слияние двух куч по ключу
        ``(depth, url)`` (то же, что единый heap). При исчерпанном ``max_documents``
        discovery не обслуживается вовсе, а ревалидация обслуживается: она бюджет не
        расходует (``09`` §6.3). ``None`` — обслуживать нечего.
        """
        discovery = self._queue[0] if self._queue else None
        revalidation = (
            self._revalidation_queue[0] if self._revalidation_queue else None
        )
        if not self.has_capacity:
            return None if revalidation is None else (revalidation, True)
        if discovery is None:
            return None if revalidation is None else (revalidation, True)
        if revalidation is None or discovery < revalidation:
            return (discovery, False)
        return (revalidation, True)

    def peek(self) -> tuple[str, int] | None:
        """Голова обслуживаемого порядка ``(url, depth)`` **без** изъятия.

        Нужна дренажу: сперва посмотреть, что будет изъято (с учётом бюджета
        ``max_documents`` — окно экстракции в ёмкость не входит, F5 плана 17), и
        только потом изымать — иначе узел, снятый с очереди и отвергнутый по
        ёмкости, терял бы позицию в счёте «в очереди осталось N».
        """
        head = self._served_head()
        if head is None:
            return None
        depth, url = head[0]
        return url, depth

    def next(self) -> tuple[str, int] | None:
        """Следующий ``(url, depth)`` по ключу ``(depth, url)`` или ``None``.

        ``None`` — очередь пуста **или** бюджет ``max_documents`` исчерпан
        (``fetched >= max_documents``), а обслуживаемых узлов не осталось (обнаруженные
        ещё лежат в очереди, ревалидации нет). Второй случай **не** тихий: ставится
        усечение ``max_documents`` (первый повод побеждает), поэтому «осталось N, но не
        изымаем» видно в провенансе корня (``queue_remaining``).

        Ревалидация известных URL бюджет не расходует (``09`` §6.3), поэтому её
        элементы изымаются и при исчерпанном бюджете discovery: она не «документ,
        который добавляет обход».
        """
        head = self._served_head()
        if head is None:
            if self._queue and not self.has_capacity:
                # Discovery остался, но бюджет исчерпан — усечение обязано быть видимым.
                self._mark_truncated("max_documents")
            return None
        (depth, url), is_revalidation = head
        if is_revalidation:
            heapq.heappop(self._revalidation_queue)
        else:
            heapq.heappop(self._queue)
        return url, depth

    def __bool__(self) -> bool:
        return bool(self._queue) or bool(self._revalidation_queue)

    def __len__(self) -> int:
        return len(self._queue) + len(self._revalidation_queue)

    def with_limits(self, limits: CrawlLimits) -> Frontier:
        """Копия frontier с урезанными лимитами (seam для D4/D3 без мутации).

        Шард узла (``shard_hosts``, F7) переносится **как есть**: это свойство
        развёртывания, а не лимит корня — копия с урезанными осями не должна внезапно
        начинать брать host'ы чужого узла.
        """
        clone = Frontier(
            self.root_url,
            limits,
            scope_hosts=self.scope_hosts,
            shard_hosts=self.shard_hosts,
        )
        clone._seen = set(self._seen)
        clone._seen_forms = set(self._seen_forms)
        clone.duplicate_form_count = self.duplicate_form_count
        clone.duplicate_form_examples = list(self.duplicate_form_examples)
        clone._duplicate_form_seen = set(self._duplicate_form_seen)
        clone._queue = list(self._queue)  # копия массива кучи остаётся корректной кучей
        clone._revalidation_queue = list(self._revalidation_queue)
        clone._revalidation = set(self._revalidation)
        clone.fetched = self.fetched
        clone.skipped_blacklisted = list(self.skipped_blacklisted)
        clone.skipped_documents = [dict(rec) for rec in self.skipped_documents]
        clone.artifacts_outside_prefix_count = self.artifacts_outside_prefix_count
        clone.artifacts_outside_prefix = list(self.artifacts_outside_prefix)
        clone._bypass_seen = set(self._bypass_seen)
        clone.out_of_scope_subdomains = dict(self.out_of_scope_subdomains)
        clone.out_of_scope_subdomain_examples = {
            host: list(urls)
            for host, urls in self.out_of_scope_subdomain_examples.items()
        }
        clone._subdomain_reject_seen = set(self._subdomain_reject_seen)
        clone.accepted_foreign_hosts = dict(self.accepted_foreign_hosts)
        clone.host_out_of_shard = dict(self.host_out_of_shard)
        clone.host_out_of_shard_examples = {
            host: list(urls) for host, urls in self.host_out_of_shard_examples.items()
        }
        clone._shard_reject_seen = set(self._shard_reject_seen)
        # F8 (17 §7.2): карта объявленной вежливости — часть состояния корня, а не
        # настроение момента: клон (``max_documents``-усечение/переигрывание) обязан
        # видеть уже записанные host'ы, иначе провенанс клона потерял бы их.
        clone.rate_limits = {
            host: dict(spec) for host, spec in self.rate_limits.items()
        }
        return clone

    # -- внутреннее --------------------------------------------------------- #

    def note_duplicate_form(self, url: str) -> None:
        """Учесть схлопнутую форму URL (``/page/`` при известном ``/page``).

        Счётчик точный и дедуплицированный по строке URL: один и тот же ``/page/``
        предлагается со многих страниц, а вопрос провенанса — «сколько URL схлопнуто»,
        не «сколько упоминаний». Примеры ограничены
        (:data:`DUPLICATE_FORM_EXAMPLES_MAX`).
        """
        if url in self._duplicate_form_seen:
            return
        self._duplicate_form_seen.add(url)
        self.duplicate_form_count += 1
        if len(self.duplicate_form_examples) < DUPLICATE_FORM_EXAMPLES_MAX:
            self.duplicate_form_examples.append(url)

    def _known_in_any_form(self, url: str) -> bool:
        """Известен ли URL в любой форме: точная строка либо ключ без хвостового ``/``.

        Точное совпадение — обычный дубль (молча, как прежде). Совпадение по
        нормализованному ключу — схлопывание формы: оно **видимо**
        (:meth:`note_duplicate_form`), потому что URL не ставится в очередь.
        """
        if url in self._seen:
            return True
        if url_form_key(url) in self._seen_forms:
            self.note_duplicate_form(url)
            return True
        return False

    def _mark_truncated(self, reason: str) -> None:
        """Фиксирует усечение (первый повод побеждает; не перезаписываем причину)."""
        if not self.truncated:
            self.truncated = True
            self.truncated_reason = reason
