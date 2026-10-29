"""Результат сборки корпуса (Ш5) — ``IndexBuild`` (L0: вынесен из ``corpus.py``).

Модуль держит **только** объявление результата фазы: сборка + продукт-партиции +
retention-счётчики. Поведения здесь нет — это носитель данных, который читают
``serp_loop`` (раунды SERP), ``runner`` (PhaseResult) и ``report`` (манифест).

Зачем отдельный модуль (L0, план декомпозиции §4): ``serp_loop`` импортировал
``IndexBuild`` из ``corpus`` и тянул за собой 10k строк фазы корпуса — из-за этого
``import core.pipeline.serp_loop`` стоил как ``import core.pipeline.corpus``.
Объявление переехало без правок; ``corpus`` реэкспортирует имя, публичная поверхность
пакета не меняется.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.domain import IndexManifestEntry


@dataclass
class IndexBuild:
    """Результат сборки корпуса (Ш5): сборка + продукт-партиции + retention-документы.

    :param index_build_id: детерминированный идентификатор иммутабельной сборки (§B.1/§13).
    :param entries: по одной ``IndexManifestEntry`` на продукт-партицию (I17).
    :param doc_count: сколько документов корпуса прошло фазу (сумма по продуктам). Сами
        ``CorpusDoc`` **не** едут в результате: их носитель — ``corpus_docs``, и читаются
        они оттуда (``store.iter_corpus_docs``). Поле-список здесь означало бы, что фаза
        пересекает границу в памяти и держит все документы до конца прогона (Д-2/A4).
    :param chunk_count: суммарное число проиндексированных чанков.
    :param crawl_pending: L1a-наблюдаемость: элементы сеанса в очереди (``PENDING``/``RUNNING``).
    :param crawl_fetched: L1a: элементы ``FETCHED`` (сырые байты сохранены = «ready» для
        материализации).
    :param crawl_done: L1a: зеркало ``crawl_fetched`` (элемент ``FETCHED`` = готов к
        извлечению; отдельного ``done`` на уровне элемента нет — сеанс трекает краул, не
        экстракцию). Поле сохранено для обратной совместимости наблюдаемости.
    :param crawl_failed: L1a: терминально сбойные элементы (``FAILED``; ``CANCELLED`` —
        если у модели нет отдельного поля — складывается сюда только для счётчика).
    :param doc_counters: честные счётчики документов по продукту (09 §6.3):
        ``{"total": n, "new": n, "reused": n}``. ``new`` — документ прошёл
        экстракцию+нарезку в этом прогоне; ``reused`` — версия переиспользована из
        durable-субстрата (304/тот же ``content_hash``). Пустая карта — legacy-путь
        (``state is None``): счётчики не выдумываются.
    :param duplicates: **наблюдаемость** дедупа по ``content_hash`` внутри продукта:
        ``{"product_id": n}`` — сколько документов схлопнулось (алиасы/перепечатки).
        Это не ошибка и не смена статуса: дедуп легитимен (I17 — партиция продукта),
        но должен быть виден, иначе цепочка «25 версий-алиасов → 1 документ» теряется
        молча. Пустая карта — схлопываний не было.
    :param duplicate_count: сумма ``duplicates`` (аддитивный счётчик для операторских
        сводок). **Не входит** в ``index_build_id``: это наблюдение, а не состав
        композиции (пин — ``tests/pipeline/test_corpus.py``).
    :param partitions_reused: сколько продукт-партиций переиспользовано с диска без
        вызова ``embedder.embed`` (09 §8.5). Ноль — ни одна партиция не переиспользована
        (первый прогон/нет ``index_root``); значение честное, не оценка.
    :param revalidation_orphans: наблюдение (не ``errors``, не смена статуса I12):
        известные URL инкрементального продукта, не попадающие ни в один авторский корень.
        Ключ ``product_id:origin`` → ``{"selected": N, "fetched": M}``; один synthetic-бакет
        на (продукт, origin). При операторском потолке ``revalidate_max_documents``
        добавляются ``"deferred": K`` (отложенные) и ``"skipped": S`` (неисполнимые);
        leftover-группа каталога пишется под ``product_id:catalog:{url}`` с
        ``"via": "catalog"``. Бакеты **не** пишутся в ``truncation`` — иначе синтетическая
        причина демотировала бы ``❌`` без вины авторского корня.
    """

    index_build_id: str
    entries: list[IndexManifestEntry] = field(default_factory=list)
    #: Счётчик документов фазы. Документы живут в ``corpus_docs``; возвращать их списком
    #: значило бы удерживать весь корпус в памяти до конца прогона (Д-2/A4).
    doc_count: int = 0
    chunk_count: int = 0
    crawl_pending: int = 0
    crawl_fetched: int = 0
    crawl_done: int = 0
    crawl_failed: int = 0
    #: D1/G: корпус был **политико-усечён** (``max_documents``/``max_depth``) — краул не
    #: исчерпал домен. Машинно-читаемый флаг на результате фазы (log_entry); связанные
    #: записи — ``truncation``. Политико-усечённый корпус не «полон» (см. статус сеанса).
    truncated: bool = False
    #: D1/G: детализация усечения по ``(product, seed-root)``: эффективные лимиты, число
    #: добытых страниц, причина (``max_documents``/``max_depth``/``round_budget``, а для
    #: операторского потолка ревалидации — ``revalidate_max_documents``), зачёркнутые
    #: блэклистом URL и аддитивный ``revalidation``-счётчик.
    truncation: list[dict[str, Any]] = field(default_factory=list)
    #: P5: видимые пропуски документов уровнем ``off`` (решение **никогда** не тихо):
    #: ``{product_id, url, reason: url_suffix|content_type, level}``. Питает канал
    #: усечения/аномалии (``source_scope: exhaustive`` поверх «прореженного» продукта).
    document_skips: list[dict[str, Any]] = field(default_factory=list)
    #: 09 §6.3: известные URL вне scope всех авторских корней (author-scope drift):
    #: ``"product_id:origin"`` → ``{"selected": N, "fetched": M}`` (+ ``deferred``/
    #: ``skipped`` при потолке). Каталоговый leftover — ``product_id:catalog:{url}`` с
    #: ``"via": "catalog"``. Наблюдение, не усечение и не ошибка.
    revalidation_orphans: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: 09 §6.3: счётчики документов по продукту (``total``/``new``/``reused``). Пусто —
    #: durable-субстрата нет (legacy), счётчики не фабрикуются.
    doc_counters: dict[str, dict[str, int]] = field(default_factory=dict)
    #: Наблюдаемость дедупа по ``content_hash`` **внутри продукта**: ``{"product_id": n}``
    #: — сколько документов схлопнулось. Не ошибка и не статус: дедуп легитимен (I17),
    #: но должен быть виден (иначе алиасы версий исчезают молча).
    duplicates: dict[str, int] = field(default_factory=dict)
    #: Сумма ``duplicates``. Аддитивное поле наблюдаемости; в ``index_build_id`` **не**
    #: входит (композиция зависит только от состава когорты и дайджестов партиций).
    duplicate_count: int = 0
    #: 09 §8.5: число продукт-партиций, переиспользованных без эмбеддинга.
    partitions_reused: int = 0
