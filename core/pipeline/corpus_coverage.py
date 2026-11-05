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
    CorpusRootCoverage,
    CrawlItemStatus,
    CrawlSessionItem,
    FailureKind,
    RunProductStatus,
)
from core.pipeline.digests import snapshot_url_key
from core.ports.state import CaptureRow


def _root_coverages(
    declared_roots: Iterable[str],
    captures: Iterable[CaptureRow],
    declared_space_digest: str | None = None,
) -> list[CorpusRootCoverage]:
    """Снятия объявленных корней продукта: по одному блоку на корень (план 20 §11.2).

    Для каждого **объявленного** корня берётся самое новое неотозванное снятие
    (``superseded_by IS NULL``) по ``finished_at``, при равенстве — по ``started_at``.
    Снятие берётся только у объявленного корня: снятие постороннего корня не удостоверяет
    пространство продукта.

    Адреса сравниваются **канонической формой** (:func:`snapshot_url_key`), как у снимка
    корпуса: измерено 05.11.2026 на стенде, что живой путь записал адрес percent-encoded,
    а проход по сохранённым байтам — декодированным. Форма записи не вправе решать,
    удостоверён корень или нет.

    **Сверка пространства.** Снятие описывает пространство, исчерпанное обходом: режимы
    политики и лимиты краула (``crawl_space_digest``). Поверхность решений
    (``decision``/``network_mode``/``corpus_source``) в отпечаток не входит: она зависит
    от решения по часам и от ``--refresh-scope``, то есть отвечает «когда смотрели», а не
    «что исчерпано». Поэтому отпечаток снятия сверяется с **объявленным**
    ``declared_space_digest`` прогона. Известны обе стороны и различаются ⇒
    ``policy_matched=False``, и ``complete`` корня принудительно ``False``: обход
    удостоверяет другое пространство. Хотя бы одна сторона неизвестна ⇒
    ``policy_matched=None`` и ``complete`` не меняется. Это **отступление от буквы §11.2**
    («чей ``policy_digest`` совпадает»), названное осознанно: отсутствие отпечатка —
    отсутствие данных, а не факт несовпадения; объявлять по нему неполноту значило бы
    решать по незнанию (A3). Решает только наблюдённое несовпадение.

    Нет снятия ⇒ ``complete=False, capture_id=None`` — «корень не удостоверён», а не
    «покрытие неизвестно»: отсутствие носителя и есть неполнота.
    """
    newest: dict[str, CaptureRow] = {}
    for capture in captures:
        if capture.superseded_by:
            continue
        key = snapshot_url_key(capture.root_url)
        current = newest.get(key)
        if current is None or _capture_rank(capture) > _capture_rank(current):
            newest[key] = capture
    declared: dict[str, str] = {}
    for raw in declared_roots:
        declared.setdefault(snapshot_url_key(raw), str(raw))
    roots: list[CorpusRootCoverage] = []
    for key in sorted(declared):
        chosen = newest.get(key)
        capture_policy = None if chosen is None else chosen.policy_digest
        policy_matched: bool | None = None
        if declared_space_digest and capture_policy:
            policy_matched = declared_space_digest == capture_policy
        complete = False if chosen is None else bool(chosen.complete)
        if policy_matched is False:
            # Обход шёл в другом пространстве (другие режимы/лимиты): исчерпание одного
            # не удостоверяет другое.
            complete = False
        roots.append(
            CorpusRootCoverage(
                root_url=declared[key],
                capture_id=None if chosen is None else chosen.capture_id,
                complete=complete,
                observed_how=None if chosen is None else chosen.observed_how,
                observed_at=None if chosen is None else chosen.finished_at,
                policy_digest=capture_policy,
                policy_matched=policy_matched,
            )
        )
    return roots


def _capture_rank(capture: CaptureRow) -> tuple[str, str, str]:
    """Порядок снятий для выбора новейшего: ``(finished_at, started_at, capture_id)``.

    Снятие без ``finished_at`` (обход не закончен) сортируется **раньше** любого
    датированного: незавершённый обход не может вытеснить состоявшееся удостоверение.
    """
    return (
        str(capture.finished_at or ""),
        str(capture.started_at or ""),
        str(capture.capture_id or ""),
    )


def derive_corpus_coverage(
    *,
    product_id: str,
    items: Iterable[CrawlSessionItem],
    truncated: bool,
    computed_at: str,
    declared_roots: Iterable[str] = (),
    captures: Iterable[CaptureRow] = (),
    declared_space_digest: str | None = None,
    manual_urls: Iterable[str] = (),
    partial_docs: Iterable[str] = (),
) -> CorpusCoverage:
    """Строит :class:`CorpusCoverage` продукта: полнота по **снятиям**, диагностика — по сеансу.

    Детерминированно: ``expected_sources`` — число элементов, ``observed_sources`` —
    дошедшие до ``FETCHED``, ``blocked`` — счётчики ``FAILED`` по ``error_kind`` (аудит).

    **Решение ``complete`` — по снятиям (план 20 §11.2, I2d).** Прежняя редакция считала
    полноту по элементам **этого** сеанса, и проход без сети (``frozen``/``offline``) на
    том же корпусе объявлялся неполным: элементов у него нет вовсе, «нет сети» читалось
    как «нет наблюдений». Измерено 05.11.2026 на ``demo-absence``: ``auto`` дал 4 × ``❌``,
    ``frozen`` — 4 × ``failed``/``status=null`` при унаследованном снятии ``complete=true``.
    Теперь:

    1. :attr:`CorpusCoverage.roots` **непусты**, и у **каждого** корня ``complete=True``
       (``complete`` выбирается из новейшего неотозванного снятия корня, см.
       :func:`_root_coverages`). Корень без снятия даёт ``complete=False`` — «не
       удостоверён». ``observed_sources == 0`` больше **не** самостоятельное условие:
       его роль исполняет корень без ``complete=1``. ``declared_space_digest`` —
       отпечаток удостоверяемого пространства прогона (режимы политики + лимиты краула):
       разошедшийся с ним отпечаток снятия обнуляет ``complete`` корня (снятие удостоверяет
       другое пространство). Отсутствие одной из сторон сверять не с чем —
       ``policy_matched=None``, ``complete`` не меняется (A3: незнание не выдаётся за
       несовпадение; см. :func:`_root_coverages`).
    2. **И** в текущем сеансе нет неразрешённых блокирующих фактов: ``truncated``,
       нечитаемый origin, ``FAILED`` с ``error_kind`` из
       :data:`core.domain.enums.BLOCKING_FAILURE_KINDS`, ``partial_extraction``.
       Эти условия проверяются **только при наличии сеанса** (есть элементы): у прохода
       без сети их не с чем сверять, и «неизвестно» не выдаётся за «провалено». Защита
       I2c от этого не ослабевает — живой сеанс с блокирующим сбоем по-прежнему даёт
       ``complete=False`` даже при удостоверяющем снятии.

    ``truncated`` — решение прогона (продукт назван усечённым/пропущенным политикой), а не
    наблюдение отдельного элемента, поэтому действует и без элементов.

    ``expected_sources``/``observed_sources``/``blocked``/``unreadable_origins``/
    ``partial_blocks``/``truncated`` остаются **диагностикой** (аудит): они объясняют,
    почему живой обход неполон, но больше не решают ``complete`` за отсутствием сеанса.

    **Нечитаемый origin** (для диагностики) — origin (``netloc`` URL), из которого не
    получено ни одного ``FETCHED`` элемента и был хотя бы один недочёт (любой
    не-``FETCHED`` исход, кроме ``not_found``). Полностью тёмный сайт не может доказать
    отсутствие никогда.

    **Блокирующий сбой** — любой ``FAILED`` элемент с ``error_kind`` из
    :data:`core.domain.enums.BLOCKING_FAILURE_KINDS` (``robots_disallow``,
    ``parser_failed``, ``extraction_timeout``, ``attempts_exhausted``,
    ``dlp_blocked``, ``quarantined``): часть материала продукта осталась ненаблюдённой,
    и ``❌`` на таком корпусе не является доказательством отсутствия. Причина видна в
    ``blocked``/``partial_blocks`` и в производном :attr:`CorpusCoverage.blocking_failures`.

    Явно **не** блокируют ``complete``:

    * ``not_found`` (HTTP 404/410) — разрешённое наблюдение отсутствия: origin ответил,
      что ресурса нет; остаётся в ``blocked`` как аудит;
    * ``source_unavailable`` — транзиентная недоступность читаемого origin'а; полностью
      тёмный origin ловится нечитаемостью;
    * ``redirect_out_of_scope`` — уход за границу охвата по определению;
    * сбои конвейера оценки (``search_failed``/``evaluation_failed``/
      ``validation_failed``) — не наблюдения корпуса.

    **Частичное извлечение (``partial_docs``).** URL документов, которые экстрактор
    прочитал **не целиком** (``CorpusDoc.partial``: docling ``PARTIAL_SUCCESS``, битая
    страница pdfium). Формально это не отказ — элемент остаётся ``FETCHED``, текст
    вошёл в корпус, — но непрочитанная часть страниц есть ненаблюдённый материал. Каждый
    такой документ считается блокирующим классом ``partial_extraction`` **в живом
    сеансе**: ``complete=False``, причина видна в ``blocked`` и в ``partial_blocks`` по
    origin'у. Число — **документы**, а не страницы.

    **Ручной артефакт (13-manual-artifacts §3.3).** ``manual_urls`` — URL, чей материал
    **предъявлен** объявленным артефактом (``corpus_docs.origin=manual``). Такой
    блокирующий ``FAILED`` остаётся в ``blocked``/``partial_blocks`` (аудит: краулу
    отказано), но больше не делает origin «нечитаемым» и не делает сеанс неполным.
    Полноту корня это не удостоверяет: ``adopted``-снятие — не обход, и ``bounded`` им
    не удостоверяется (I2d).

    **Человеческий escape hatch.** ``scope_attested_by`` остаётся единственным способом
    опубликовать ``❌`` на неполном корпусе: человек осознанно принимает риск, что
    ненаблюдённая часть сайта могла бы содержать параметр. Где материал предъявлен,
    подпись риска не нужна — нужен документ.
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
    # План 20 §11.2: полнота — по снятиям объявленных корней, а не по элементам сеанса.
    # Снятие выбирается здесь же: один расчёт на два представления (``roots`` и
    # ``complete``) — второго предиката нет.
    roots = _root_coverages(
        declared_roots, captures, declared_space_digest
    )
    roots_ok = bool(roots) and all(root.complete for root in roots)
    # Сеансовые условия действуют, только когда сеанс **есть** (есть элементы): у прохода
    # без сети их не с чем сверять, и «неизвестно» не выдаётся за «провалено».
    session_blocked = False
    if expected > 0:
        unreadable_origins = sorted(
            origin for origin in losses_by_origin if fetched_by_origin.get(origin, 0) == 0
        )
        session_blocked = (
            bool(unreadable_origins) or blocking_failures
        )
    else:
        unreadable_origins = []
    return CorpusCoverage(
        product_id=product_id,
        # ``truncated`` — решение прогона (продукт усечён или пропущен политикой), а не
        # наблюдение элемента, поэтому действует и без элементов.
        complete=roots_ok and not truncated and not session_blocked,
        roots=roots,
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
ONLY_FAILED_REPAIR_REASON = "only_failed_repair"
"""Прогон ``--only-failed``: в работе были только ранее проваленные URL, остальной корпус
продукта в этом прогоне не читался. Поэтому покрытие **не** ``complete``, даже если все
элементы ремонта дошли до ``FETCHED``: «полно» здесь означало бы «полно относительно
ремонта», а читалось бы как «сайт прочитан» (I2/I2c)."""
HOST_OUT_OF_SHARD_REASON = "host_out_of_shard"
"""F7 (17 §2): объявленные host'ы продукта покрыты шардом узла **не полностью** —
часть корпуса принадлежит другому узлу. Такой продукт получает ``complete=False`` с
этой причиной даже при всех ``FETCHED``: «всё, что мы пытались, добыто» — не «корпус
собран», и непокрытый host нельзя выдавать за ``exhaustive`` (I2b/I2c: ``❌`` из
неполного корпуса недоказуем). Значение то же, что ``Frontier.truncated_reason`` и
машинный токен провенанса сеанса, — один словарь на причину, а не три написания."""


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
            # План 19 §P2: пропуск по политике краула назван **в строке продукта**:
            # «корень не дал документов» и «сеть запретил режим» — разные факты, и
            # второй не должен читаться как сбой (I4).
            skips = [str(item) for item in (event.get("policy_skips") or [])]
            policy_note = (
                ", пропущено политикой краула корней "
                f"{len(skips)}: "
                + "; ".join(skips[:3])
                + (f" и ещё {len(skips) - 3}" if len(skips) > 3 else "")
                if skips
                else ""
            )
            notes.append(
                f"corpus: продукт {event.get('product_id')} — "
                f"корней {event.get('roots')}, seed'ов поставлено "
                f"{event.get('seeds_enqueued')} из {event.get('seeds_declared')}, "
                f"элементов {event.get('session_items')} "
                f"(fetched {event.get('fetched')}, failed {event.get('failed')}), "
                f"документов {event.get('docs')}{partial_note}{unknown_note}{changes_note}"
                f"{policy_note}, {event.get('elapsed_seconds')} с{tail}"
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
    incomplete_reasons: Mapping[str, str] | None = None,
    declared_roots: Mapping[str, Iterable[str]] | None = None,
    captures: Mapping[str, Iterable[CaptureRow]] | None = None,
    declared_space_digest: str | None = None,
) -> dict[str, CorpusCoverage]:
    """Вычисляет и персистит покрытие **каждого** продукта плана (Ф0, O3).

    Продукт без элементов сеанса (нулевой seed-набор / preflight обойдён / все seed'ы
    отклонены / проход без сети) получает **истинную** причину в ``reason`` — отсутствие
    наблюдения не должно читаться как «неизвестно» и не должно давать тихий ``ok``.
    ``complete`` при этом решают **снятия** (план 20 §11.2): корень без ``complete=1``
    оставляет ``complete=False`` (нулевой seed-набор, все seed'ы отклонены), а
    удостоверенный прошлым обходом корпус остаётся полным и без сети (``frozen``) — там
    ``reason`` называет прогон, а не приговор покрытию. Значения причины —
    :data:`ZERO_SOURCE_NO_SEEDS`, :data:`ZERO_SOURCE_ALL_SEEDS_REJECTED`,
    :data:`ZERO_SOURCE_NO_URLS_ENTRY`; вызывающий (:func:`_build_corpus`) знает
    фактический путь, поэтому передаёт карту ``reasons`` (продукт → причина). Без карты
    (прямой вызов/старые тесты) — прежнее ``no_seeds``: значение по умолчанию не
    придумывает новую причину.
    Продукт с элементами, но без единого ``FETCHED`` (все ``not_found``/``pending``),
    получает ``complete=False``, если корень не удостоверён снятием; ``reason`` при этом
    остаётся ``None`` — неполноту объясняют ``roots``/счётчики, а не новая причина.

    ``resolved_manually`` — ``product_id → [url]``, чей материал предъявлен объявленным
    ручным артефактом (13-manual-artifacts §3.3): отказ остаётся аудитом (``blocked``) и
    перестаёт делать **сеанс** неполным; полноту корня ручная подача не удостоверяет —
    ``adopted``-снятие не является обходом (I2d). Значение по умолчанию — пусто
    (поведение прежнее).

    ``partial_docs`` — ``product_id → [url]`` документов, прочитанных **не целиком**
    (:attr:`~core.domain.models.CorpusDoc.partial`). Каждый такой документ делает
    покрытие неполным с причиной ``partial_extraction``: непрочитанные страницы —
    ненаблюдённый материал, и объявлять охват полным на нём нельзя. Значение по
    умолчанию — пусто (прогон без частичных документов ведёт себя как прежде).

    Список не различает «измерено в этом прогоне» и «восстановлено из durable-записи
    версии» (``304``-переиспользование): оба — наблюдения одного и того же документа, и
    второе не становится слабее от того, что прибор не вызывался повторно (v5).

    ``incomplete_reasons`` — ``product_id → машинный токен``: прогон **по построению** не
    наблюдал продукт целиком, даже если все его элементы дошли до ``FETCHED``. Сегодня
    единственный производитель — ремонт провалов (``--only-failed``: в работе были только
    ранее проваленные URL, остальной корпус не читался) с токеном
    :data:`ONLY_FAILED_REPAIR_REASON`. Такая строка получает ``complete=False`` и названную
    причину: иначе ремонт одного URL объявлял бы покрытие полным, а ``❌`` публиковался бы
    на корпусе, который в этом прогоне не наблюдался. Пустая карта — поведение прежнее.

    ``declared_roots`` — ``product_id → [объявленный корень]``, ``captures`` —
    ``product_id → [снятие]`` (``state.iter_captures``). Пара — **носитель полноты**
    (план 20 §11.2): :func:`derive_corpus_coverage` строит из неё ``roots`` и решает
    ``complete``. Путь **без сеанса** (``frozen``/``offline``) получает полноту из снятий
    прошлых прогонов: у него нет элементов, но корпус удостоверен датированным обходом.
    Пустая карта — «корни не переданы», и тогда ``complete=False`` (носителя нет), а не
    «всё полно».

    ``declared_space_digest`` — отпечаток **удостоверяемого пространства** прогона
    (режимы политики + ``crawl_config_digest``; :func:`crawl_space_digest`): снятие,
    сделанное в другом пространстве, удостоверяет другое исчерпание, поэтому разошедшийся
    отпечаток обнуляет ``complete`` корня. Поверхность решений
    (``crawl_policy_digest``) сюда не входит: она отвечает «когда смотрели» и является
    входом ``run_id``. ``None`` — сверять не с чем (незнание не выдаётся за несовпадение,
    A3).

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
    roots_by_product = declared_roots or {}
    captures_by_product = captures or {}
    #: Продукты, которые этот прогон по построению не наблюдал целиком (ремонт провалов).
    scope_incomplete = dict(incomplete_reasons or {})
    for product_id in sorted(planned | set(by_product)):
        items = by_product.get(product_id, [])
        # Полнота — **один** путь для обоих случаев: снятие корня удостоверяет корпус и
        # когда сеанс пуст (проход без сети), и когда он полон. Второй предикат для
        # «нет элементов» разошёлся бы с первым — ровно дефект замера 05.11.2026.
        coverage = derive_corpus_coverage(
            product_id=product_id,
            items=items,
            truncated=product_id in truncated_products,
            computed_at=computed_at,
            declared_roots=roots_by_product.get(product_id, ()),
            captures=captures_by_product.get(product_id, ()),
            # Сверка удостоверяемого пространства снятия с объявленным (I2d §11.2).
            declared_space_digest=declared_space_digest,
            # 13-manual-artifacts §3.3: URL, чей материал предъявлен артефактом.
            manual_urls=(resolved_manually or {}).get(product_id, ()),
            # I2/I2c: документы, прочитанные не целиком (причина
            # ``partial_extraction`` в ``blocked``).
            partial_docs=(partial_docs or {}).get(product_id, ()),
        )
        if not items:
            # Ключ, присутствующий со значением ``None``, — честное «причина
            # счётчиками не названа»; отсутствие ключа — прежний ``no_seeds``.
            # План 20 §11.2: причина называет **прогон** (наблюдал ли он), полноту решают
            # снятия; поэтому ``reason`` не переписывает ``complete``.
            coverage = coverage.model_copy(
                update={"reason": known_reasons.get(product_id, ZERO_SOURCE_NO_SEEDS)}
            )
        token = scope_incomplete.get(product_id)
        if token:
            # Прогон не наблюдал продукт целиком (ремонт провалов): «все элементы
            # дошли до FETCHED» здесь означает «дошли те, что были в ремонте».
            coverage = coverage.model_copy(
                update={"complete": False, "reason": token}
            )
        store.save_corpus_coverage(run_id, coverage)
        written[product_id] = coverage
    return written

