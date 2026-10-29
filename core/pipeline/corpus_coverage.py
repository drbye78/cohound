"""Покрытие корпуса и статус продукта в прогоне (Ш5, Ф0/16 §3.2).

Модуль вынесен из :mod:`core.pipeline.corpus` (полоса L2 плана декомпозиции): расчёт
покрытия и статуса продукта не требует ни краулера, ни экстрактора, ни индекса, поэтому
проверяется прямым вызовом, а не через полный харнесс ``build_corpus``.

**Имя честно называет две вещи.** Здесь не только расчёт: :func:`_persist_corpus_coverage`
пишет строки ``corpus_coverage`` в store. Разделять «расчёт» и «запись» на этом шаге
нельзя — это изменило бы порядок записи (оракул ``test_corpus_write_order.py``), поэтому
модуль называется по тому, что делает, а не по желаемой чистоте.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

from core.domain import (
    BLOCKING_FAILURE_KINDS,
    CorpusCoverage,
    CrawlItemStatus,
    CrawlSessionItem,
    FailureKind,
    RunProductStatus,
)


def derive_corpus_coverage(
    *,
    product_id: str,
    items: Iterable[CrawlSessionItem],
    truncated: bool,
    computed_at: str,
    manual_urls: Iterable[str] = (),
    partial_docs: Iterable[str] = (),
) -> CorpusCoverage:
    """Строит :class:`CorpusCoverage` продукта из персистентных элементов сеанса (Ф0).

    Детерминированно: ``expected_sources`` — число элементов, ``observed_sources`` —
    дошедшие до ``FETCHED``, ``blocked`` — счётчики ``FAILED`` по ``error_kind`` (аудит).

    **Порог неполноты — три независимых условия.**

    1. *Нечитаемый origin.* Origin (``netloc`` URL) считается **нечитаемым**, только
       если из него не получено ни одного ``FETCHED`` элемента и при этом был хотя бы
       один недочёт (любой не-``FETCHED`` исход, кроме ``not_found``). Полностью тёмный
       сайт не может доказать отсутствие никогда.
    2. *In-scope блокирующий сбой.* Любой ``FAILED`` элемент с ``error_kind`` из
       :data:`core.domain.enums.BLOCKING_FAILURE_KINDS` (``robots_disallow``,
       ``parser_failed``, ``extraction_timeout``, ``attempts_exhausted``,
       ``dlp_blocked``, ``quarantined``) делает покрытие неполным **по построению**,
       даже если origin в целом читаем: часть материала продукта осталась ненаблюдённой,
       и ``❌`` на таком корпусе не является доказательством отсутствия. Причина
       блокировки видна в ``blocked``/``partial_blocks`` и в производном
       :attr:`CorpusCoverage.blocking_failures`.
    3. *Ни одного наблюдения.* ``observed_sources == 0`` при ``expected_sources > 0``
       делает покрытие неполным **по построению**: корпус, в котором не прочитано ни
       одного документа, не может доказывать отсутствие. ``not_found`` на каждом URL —
       наблюдение отсутствия *этих URL*, а не доказательство, что у продукта нет
       возможности; полностью ``pending``/``running`` сеанс — отсутствие наблюдения
       вовсе. ``reason`` при этом остаётся ``None``: неполноту объясняют
       ``expected_sources``/``observed_sources``/``blocked``/``unreadable_origins``.

    ``complete = (not truncated) and (not unreadable_origins) and (no blocking failure) and (observed_sources > 0)``;
    частичное извлечение входит в условие через блокирующий класс ``partial_extraction``.

    Явно **не** блокируют ``complete``:

    * ``not_found`` (HTTP 404/410) — разрешённое наблюдение отсутствия: origin ответил,
      что ресурса нет; остаётся в ``blocked`` как аудит. Сам по себе класс не блокирует,
      но корпус, где **все** URL вернули ``not_found``, неполон по условию (3);
    * ``source_unavailable`` — транзиентная недоступность читаемого origin'а; полностью
      тёмный origin по-прежнему ловится условием (1);
    * ``redirect_out_of_scope`` — уход за границу охвата по определению;
    * сбои конвейера оценки (``search_failed``/``evaluation_failed``/
      ``validation_failed``) — не наблюдения корпуса.

    ``partial_blocks`` фиксирует не-``not_found`` сбои каждого origin'а: человек видит
    частичные отказы, но они больше не «тихие» — блокирующий класс означает неполноту.

    **Частичное извлечение (``partial_docs``).** URL документов, которые экстрактор
    прочитал **не целиком** (``CorpusDoc.partial``: docling ``PARTIAL_SUCCESS``, битая
    страница pdfium). Формально это не отказ — элемент остаётся ``FETCHED``, текст
    вошёл в корпус, — но непрочитанная часть страниц есть ненаблюдённый материал. Если
    её не учесть, документ, прочитанный на 7 страницах из 2763, выглядит как полностью
    прочитанный, молчание страниц читается как «вендор не заявил», и ``❌`` публикуется
    на корпусе, где половина документа не наблюдалась. Поэтому каждый такой документ
    считается блокирующим классом ``partial_extraction``: ``complete=False``, причина
    видна в ``blocked`` и в ``partial_blocks`` по origin'у. Число — **документы**, а не
    страницы: страницы публикуются отдельно (``CorpusDoc.page_count``/
    ``pages_converted``) и в класс сбоя не подмешиваются.

    **Ручной артефакт закрывает пробел, а не отказ (13-manual-artifacts §3.3).**
    ``manual_urls`` — URL, чей материал **предъявлен** объявленным артефактом
    (``corpus_docs.origin=manual``). Такой блокирующий ``FAILED`` остаётся в ``blocked`` и
    ``partial_blocks`` (аудит: краулу отказано), но больше не делает покрытие неполным и не
    делает origin «нечитаемым»: материал наблюдён, хоть и другим путём. Их число —
    :attr:`CorpusCoverage.resolved_manually`. Заменяется не факт, а его последствие для
    полноты: карта не должна выглядеть так, будто краул прочитал сайт.

    **Человеческий escape hatch.** ``scope_attested_by`` остаётся единственным способом
    опубликовать ``❌`` на неполном корпусе: человек осознанно принимает риск, что
    ненаблюдённая часть сайта могла бы содержать параметр. Машинный ``❌`` в этом случае
    понижается до ``pending``-кандидата (I2c/I6/I7). Где материал предъявлен, подпись риска
    не нужна — нужен документ.
    """
    expected = 0
    observed = 0
    blocked: dict[str, int] = {}
    fetched_by_origin: dict[str, int] = {}
    #: origin → число «недостач» (не-FETCHED исходов, кроме ``not_found``).
    losses_by_origin: dict[str, int] = {}
    partial_blocks: dict[str, dict[str, int]] = {}
    resolved_urls = {str(url) for url in manual_urls}
    resolved_manually = 0
    blocking_values = {kind.value for kind in BLOCKING_FAILURE_KINDS}
    blocking_failures = False
    for item in items:
        expected += 1
        origin = (
            (urlsplit(item.url).netloc or "").lower()
            or item.url.strip().lower()
            or "unknown"
        )
        if item.status is CrawlItemStatus.FETCHED:
            observed += 1
            fetched_by_origin[origin] = fetched_by_origin.get(origin, 0) + 1
            continue
        kind = item.error_kind if item.status is CrawlItemStatus.FAILED else None
        if kind:
            blocked[kind] = blocked.get(kind, 0) + 1
        if kind == FailureKind.NOT_FOUND.value:
            # Разрешённое наблюдение отсутствия: не потеря и не блокировка покрытия.
            continue
        # Ручной артефакт (§3.3): отказ **остаётся** аудитом (``blocked`` уже посчитан
        # выше, ``partial_blocks`` — здесь), но перестаёт быть потерей: материал наблюдён
        # другим путём, поэтому origin не «нечитаем», а покрытие не «неполно».
        if str(item.url) in resolved_urls:
            if kind:
                counts = partial_blocks.setdefault(origin, {})
                counts[kind] = counts.get(kind, 0) + 1
                # Счётчик называет **закрытые пробелы**, поэтому считает блокирующие
                # классы: неблокирующий отказ (``source_unavailable``/``not_found``/
                # ``redirect_out_of_scope``) пробела не открывал, и приписывать его
                # подаче значило бы завышать её вклад.
                if kind in blocking_values:
                    resolved_manually += 1
            continue
        losses_by_origin[origin] = losses_by_origin.get(origin, 0) + 1
        if kind:
            counts = partial_blocks.setdefault(origin, {})
            counts[kind] = counts.get(kind, 0) + 1
            if kind in blocking_values:
                blocking_failures = True

    unreadable_origins = sorted(
        origin for origin in losses_by_origin if fetched_by_origin.get(origin, 0) == 0
    )
    # Частичное извлечение: документ **получен**, но прочитана лишь часть. Формально не
    # отказ (элемент ``FETCHED``), фактически — часть материала ненаблюдена, поэтому
    # класс блокирующий: ``complete=False``, причина в ``blocked`` и в ``partial_blocks``
    # по origin'у. Без этого «7 страниц из 2763» молчало бы как «прочитано всё», а
    # молчание страниц стало бы ``❌`` (I2/I2c).
    partial_origin_counts: dict[str, int] = {}
    for raw_url in partial_docs:
        text_url = str(raw_url or "")
        origin = (
            (urlsplit(text_url).netloc or "").lower()
            or text_url.strip().lower()
            or "unknown"
        )
        partial_origin_counts[origin] = partial_origin_counts.get(origin, 0) + 1
    if partial_origin_counts:
        partial_total = sum(partial_origin_counts.values())
        kind = FailureKind.PARTIAL_EXTRACTION.value
        blocked[kind] = blocked.get(kind, 0) + partial_total
        for origin, count in partial_origin_counts.items():
            counts = partial_blocks.setdefault(origin, {})
            counts[kind] = counts.get(kind, 0) + count
        blocking_failures = True
    # 13-manual-artifacts §3.3: «ни одного наблюдения» — не только про краул. Материал,
    # предъявленный человеком, наблюдён; ``observed_sources`` при этом остаётся счётчиком
    # **краула** (строка не должна утверждать, что сайт прочитан), а ручная часть названа
    # отдельно — ``resolved_manually``.
    observed_any = observed > 0 or resolved_manually > 0
    return CorpusCoverage(
        product_id=product_id,
        complete=(not truncated)
        and not unreadable_origins
        and not blocking_failures
        and observed_any,
        expected_sources=expected,
        observed_sources=observed,
        blocked=dict(sorted(blocked.items())),
        unreadable_origins=unreadable_origins,
        partial_blocks={
            origin: dict(sorted(partial_blocks[origin].items()))
            for origin in sorted(partial_blocks)
        },
        truncated=truncated,
        resolved_manually=resolved_manually,
        computed_at=computed_at,
    )


#: Машинные причины zero-source покрытия (``CorpusCoverage.reason``).
#:
#: ``reason`` — **наблюдательное** поле (свободная строка в модели и колонка TEXT в БД),
#: не перечисление ``docs/status-dictionary.yaml``: расширение значений не меняет смысл
#: правил покрытия и не является методологической границей (§13). ``failure_kind`` ячеек
#: остаётся ``no_sources`` (:func:`core.pipeline.evaluate.evaluate_phase`), поэтому
#: словарь классов сбоя не расширяется.
ZERO_SOURCE_NO_SEEDS = "no_seeds"
"""Список seed'ов продукта пуст (ключ ``urls`` есть, значений нет) либо ``urls`` не задан."""
ZERO_SOURCE_ALL_SEEDS_REJECTED = "all_seeds_rejected"
"""Seed'ы были, но ни один не поставлен в очередь: блэклист/``max_documents``/пустая
развёртка источника-каталога. Конкретный URL называет per-seed запись ``errors``."""
ZERO_SOURCE_NO_URLS_ENTRY = "no_urls_entry"
"""У продукта плана нет записи в отображении ``urls`` вовсе."""


def _product_notes(events: Sequence[Mapping[str, Any]]) -> list[str]:
    """Строковые заметки фазы по границам продуктов (операторский канал).

    ``log_entries`` фазы **не сериализуется** ни в один файл: CLI превращает в
    ``results[].notes`` только ``str``-записи. Поэтому структурные dict-события
    (``product_start``/``product_end``/``product_skipped``) сами по себе оператору
    невидимы — здесь они дублируются одной строкой на продукт. Это не ``errors``:
    продукт с бюджетом ``N`` и нулём отказов — норма, а не сбой.

    Строка ``product_end`` называет ровно те величины, которых раньше не было видно
    (сколько корней/seed'ов/элементов/документов и сколько это заняло), поэтому
    «продукт съел весь бюджет» и «продукт почти ничего не дал» перестают выглядеть
    одинаково в отчёте. ``product_skipped`` — единственный след продукта плана, вообще
    не попавшего в цикл (``--only-product``/``--only-failed``/пустой ``urls``).

    :param events: структурные записи границ продукта (порядок сохраняется).
    :returns: заметки в порядке событий; ``product_start`` не дублируется (граница
        «начат» нужна живьём, в отчёте она бесполезна).
    """
    notes: list[str] = []
    for event in events:
        kind = event.get("event")
        if kind == "product_skipped":
            notes.append(
                f"corpus: продукт {event.get('product_id')} пропущен — нет записи в "
                "urls (фильтр --only-product/--only-failed или пустой seed-набор)"
            )
        elif kind == "product_end":
            reason = event.get("reason")
            tail = f", причина {reason}" if reason else ""
            # Частичное извлечение называется в строке продукта только когда оно есть:
            # «прочитано не целиком» — факт, который оператор обязан увидеть до отчёта,
            # а прогон без частичных документов не должен обрастать шумом.
            partial = int(event.get("partial_docs") or 0)
            partial_note = (
                f", документов прочитано частично {partial} (охват неполон)" if partial else ""
            )
            # v5: признак частичности мог не попасть в версию (строка до схемы 5) —
            # переиспользование такого документа не должно читаться как «прочитан весь».
            unknown = int(event.get("partial_unknown") or 0)
            unknown_note = (
                f", документов с неизвестной частичностью {unknown} "
                "(признак в версии не записан)"
                if unknown
                else ""
            )
            # v5: смена частичности — тот же ключ (байты/движок/уровень) дал другой объём
            # работы. Молчание здесь прятало бы и деградацию, и восстановление.
            changes = event.get("partiality_changes") or []
            changes_note = (
                ", частичность изменилась: "
                + "; ".join(
                    f"{item.get('url')} ({item.get('was')} → {item.get('now')})"
                    for item in changes[:3]
                )
                + (f" и ещё {len(changes) - 3}" if len(changes) > 3 else "")
                if changes
                else ""
            )
            notes.append(
                f"corpus: продукт {event.get('product_id')} — "
                f"корней {event.get('roots')}, seed'ов поставлено "
                f"{event.get('seeds_enqueued')} из {event.get('seeds_declared')}, "
                f"элементов {event.get('session_items')} "
                f"(fetched {event.get('fetched')}, failed {event.get('failed')}), "
                f"документов {event.get('docs')}{partial_note}{unknown_note}{changes_note}, "
                f"{event.get('elapsed_seconds')} с{tail}"
            )
    return notes


def _final_product_status(
    *,
    zero_source_reason: str | None,
    physical_ok: bool,
    coverage: CorpusCoverage | None,
) -> RunProductStatus:
    """Итоговый статус продукта в прогоне — из **измеренных** фактов фазы (16 §3.2).

    Правило выведено из того, что фаза уже знает, а не из новых источников:

    1. ``BLOCKED`` — корпус без источников: причина названа (``no_seeds`` /
       ``all_seeds_rejected`` / ``no_urls_entry``). Материала не было, а не «не удалось»;
    2. ``FAILED`` — партиция физически не зафиксирована (``physical_ok=False``): документы
       есть, индекса нет, ``corpus_chunks`` не записаны («disk first, DB second», K2/§13);
    3. ``COMPLETE`` — покрытие посчитано и полно;
    4. ``PARTIAL`` — всё остальное: покрытие неполно **либо** не считалось (``coverage is
       None``, нет сеанса). «Не доказано полно» — это не «полно»; выдать ``complete`` из
       отсутствия измерения значило бы объявить охват, которого никто не мерил.

    Перечисление — доменное (:class:`~core.domain.enums.RunProductStatus`), а не строки
    здесь: ``run_product.status`` читают отчёт и ``hound diff``, и расходиться в написании
    они не должны. ``error_kind`` строки пишет вызывающий (причина zero-source), ``error``
    — свободный текст; здесь их нет намеренно: статус — классификация, а не сообщение.
    """
    if zero_source_reason is not None:
        return RunProductStatus.BLOCKED
    if not physical_ok:
        return RunProductStatus.FAILED
    if coverage is not None and coverage.complete:
        return RunProductStatus.COMPLETE
    return RunProductStatus.PARTIAL


def _persist_corpus_coverage(
    store: Any,
    run_id: str,
    session_id: str,
    truncated_products: set[str],
    computed_at: str,
    *,
    products: Iterable[str] | None = None,
    reasons: Mapping[str, str | None] | None = None,
    resolved_manually: Mapping[str, Iterable[str]] | None = None,
    partial_docs: Mapping[str, Iterable[str]] | None = None,
) -> dict[str, CorpusCoverage]:
    """Вычисляет и персистит покрытие **каждого** продукта плана (Ф0, O3).

    Продукт без элементов сеанса (нулевой seed-набор / preflight обойдён / все seed'ы
    отклонены) получает строку ``expected_sources=0, complete=False`` и **истинную**
    причину в ``reason`` — отсутствие покрытия не должно читаться как «неизвестно» и не
    должно давать тихий ``ok``. Значения — :data:`ZERO_SOURCE_NO_SEEDS`,
    :data:`ZERO_SOURCE_ALL_SEEDS_REJECTED`, :data:`ZERO_SOURCE_NO_URLS_ENTRY`;
    вызывающий (:func:`_build_corpus`) знает фактический путь, поэтому передаёт карту
    ``reasons`` (продукт → причина). Без карты (прямой вызов/старые тесты) — прежнее
    ``no_seeds``: значение по умолчанию не придумывает новую причину.
    Продукт с элементами, но без единого ``FETCHED`` (все ``not_found``/``pending``),
    получает ``complete=False`` из :func:`derive_corpus_coverage` с ``reason=None``:
    неполноту объясняют счётчики, а не новая причина.

    ``resolved_manually`` — ``product_id → [url]``, чей материал предъявлен объявленным
    ручным артефактом (13-manual-artifacts §3.3): отказ остаётся аудитом, но перестаёт
    делать покрытие неполным. Значение по умолчанию — пусто (поведение прежнее).

    ``partial_docs`` — ``product_id → [url]`` документов, прочитанных **не целиком**
    (:attr:`~core.domain.models.CorpusDoc.partial`). Каждый такой документ делает
    покрытие неполным с причиной ``partial_extraction``: непрочитанные страницы —
    ненаблюдённый материал, и объявлять охват полным на нём нельзя. Значение по
    умолчанию — пусто (прогон без частичных документов ведёт себя как прежде).

    Список не различает «измерено в этом прогоне» и «восстановлено из durable-записи
    версии» (``304``-переиспользование): оба — наблюдения одного и того же документа, и
    второе не становится слабее от того, что прибор не вызывался повторно (v5).

    :returns: ``product_id → CorpusCoverage`` — ровно те строки, которые записаны. Нужен
        писателю ``run_product`` (16 §3.2): ``coverage_complete`` — та же величина, что
        легла в ``corpus_coverage``, а не второй её расчёт (два носителя одной величины
        разошлись бы). Вызывающий, которому покрытие не нужно, возврат игнорирует.
    """
    written: dict[str, CorpusCoverage] = {}
    by_product: dict[str, list[CrawlSessionItem]] = {}
    for item in store.iter_crawl_session_items(session_id):
        by_product.setdefault(item.product_id, []).append(item)
    planned = {str(product_id) for product_id in (products or ())}
    known_reasons = dict(reasons or {})
    for product_id in sorted(planned | set(by_product)):
        items = by_product.get(product_id, [])
        if not items:
            coverage = CorpusCoverage(
                product_id=product_id,
                complete=False,
                expected_sources=0,
                observed_sources=0,
                computed_at=computed_at,
                # Ключ, присутствующий со значением ``None``, — честное «причина
                # счётчиками не названа»; отсутствие ключа — прежний ``no_seeds``.
                reason=known_reasons.get(product_id, ZERO_SOURCE_NO_SEEDS),
            )
        else:
            coverage = derive_corpus_coverage(
                product_id=product_id,
                items=items,
                truncated=product_id in truncated_products,
                computed_at=computed_at,
                # 13-manual-artifacts §3.3: URL, чей материал предъявлен артефактом.
                manual_urls=(resolved_manually or {}).get(product_id, ()),
                # I2/I2c: документы, прочитанные не целиком (причина
                # ``partial_extraction`` в ``blocked``).
                partial_docs=(partial_docs or {}).get(product_id, ()),
            )
        store.save_corpus_coverage(run_id, coverage)
        written[product_id] = coverage
    return written

