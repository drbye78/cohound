"""Идентичность индексной сборки и эмбеддинг продукт-партиции (Ш5, Pass B).

Модуль вынесен из :mod:`core.pipeline.corpus` (полоса L3 плана декомпозиции) **механически**:
функции получили явные параметры вместо замыкания, состав параметров не менялся. Контракт
``ProductIndexInput → ProductIndexResult`` здесь **не** вводится намеренно: это изменение
формы, а не перенос, и смешивать их в одном шаге значило бы потерять возможность проверить
перенос отдельно (см. отчёт полосы). Порты (``EmbedderPort``/``IndexBuilderPort``) —
объявленная граница слоя, а не замыкание на локальные имена фазы.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import contextlib
import operator
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.domain import CorpusChunk, CorpusDoc
from core.pipeline.embed_cache import EmbedCache
from core.pipeline.liveness import Heartbeat
from core.ports.embedder import EmbedderPort
from core.ports.index_builder import IndexBuilderPort, IndexMeta
from core.ports.types import Chunk as PortChunk
from core.util.hashing import short_hash
from core.util.index_partition import build_root as _partition_root
from core.util.index_partition import is_committed as _partition_is_committed
from core.util.index_partition import read_manifest as _read_partition_manifest
from core.util.index_partition import (
    recompute_vector_digest as _recompute_vector_digest,
)

#: Разделитель полей material-строки ``index_build_id`` (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"

#: Схема формулы ``partition_build_id`` (09 §8.1). Входит в material, поэтому ревизия
#: формулы всегда даёт новые id: старые партиции не переиспользуются молча.
#:
#: ``partition-2`` (16 §5): в material добавлено устройство счёта (``embed_device``),
#: ``chunk_ids.jsonl`` несёт ``content_hash`` (отпечаток партиции стал пересчитываемым с
#: диска), ``chunk_id`` адресует версию документа (``doc_version_id``). Партиции формата
#: ``partition-1`` остаются на диске иммутабельно и **читаются**, но не переиспользуются:
#: их ``vector_digest`` недоказуем (в ``chunk_ids.jsonl`` нет ``content_hash``).
_PARTITION_SCHEMA = "partition-2"

#: Наблюдаемость эмбеддинга: число чанков в одном батче **на уровне фазы**
#: (Pass B, :func:`_embed_and_build`).
#:
#: Почему внешний батч, если :meth:`EmbedderPort.embed` режет вход сам: внутренняя нарезка
#: адаптера — его деталь, и между её батчами у фазы нет точки наблюдения. Один вызов на весь
#: продукт — это участок без признака жизни: 2026-10-25 прогон простоял 24 минуты на 505 % CPU
#: с неподвижным heartbeat, и «работает» было неотличимо от «повесился». Внешний батч даёт
#: границу, на которой фаза пишет heartbeat и счётчик прогресса.
#:
#: Величина — компромисс двух ограничений:
#:
#: * **Наблюдаемость.** Дефолтный ``chunk_size`` — 1500 символов; замер этого хоста
#:   (``docs/reports/measurements/2026-10-25-run-stall-embedding.md``) — 1173 мс/чанк. 64 чанка
#:   = окно ~75 с между точками жизни. Порог stale сеанса — 600 с, то есть запас восьмикратный,
#:   и оператор с ``--follow`` видит движение раз в батч, а не «никогда».
#: * **Память.** Тексты батча материализуются по мере обхода (список ссылок на
#:   ``chunk.text`` не строится на весь продукт), векторы батча добавляются к накопителю.
#:   Накопитель векторов всё равно полон: партиция собирается одним вызовом
#:   ``IndexBuilderPort.build(chunks, vectors, meta)``, который требует все векторы сразу —
#:   это контракт порта, а не выбор корпуса (пик памяти ограничивается там, не здесь).
#:
#: Кратность 32 (внутренний батч ``LocalEmbedder``) держит нарезку совпадающей с внутренней,
#: поэтому результат побитово тот же, что у одного вызова (детерминизм, A3).
EMBED_BATCH_CHUNKS = 64

#: Наблюдаемость эмбеддинга: вес нового замера в экспоненциальном сглаживании темпа эмбеддинга
#: (:class:`_EmbedThroughput`). 0.3 — темп реагирует на смену длины чанков за 3–4 батча, но
#: не скачет от одного медленного батча (первый батч включает ленивую загрузку модели).
_EMBED_RATE_ALPHA = 0.3

def _derive_partition_build_id(
    *,
    package_id: str,
    product_id: str,
    chunker_version: str,
    embed_model_id: str,
    embed_device: str = "",
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
    product_content_token: str,
) -> str:
    """Детерминированный ``partition_build_id`` одной продукт-партиции (09 §8.1).

    Входы (склеены ``\\x1f``, порядок фиксирован):

    0. ``_PARTITION_SCHEMA`` — версия формулы;
    1. ``package_id``;
    2. ``product_id`` — партиция продукт-скоупед (I17);
    3. ``chunker_version``;
    4. ``embed_model_id`` (пусто, если эмбеддер не подключён);
    5. ``embed_device`` — **место счёта** векторов (``cpu``/``cuda``, 16 §5.1). Не входит в
       ``embed_model_id``: тот описывает веса, а не хост, на котором они считались.
       Векторы CPU и CUDA одной модели численно разные, поэтому без устройства в ``pb``
       CPU-партиция молча переиспользовалась бы CUDA-прогоном;
    6. ``extractor_version`` — композит версий экстрактора **этого** продукта;
    7. ``lexical_engine_version`` — фактический лексический движок прогона
       (``tantivy-<ver>``; пусто — лексического плеча нет). Берётся у сборщика,
       которого настраивает composition root (``wiring``), а не у ``getattr``-дефолта:
       иначе ``pb`` не различал бы движки (§13:676);
    8. ``vector_store``;
    9. ``product_content_token`` — отпечаток содержимого партиции.

    **Чего здесь нет и почему.** ``params_token`` (каталог/фокус) и ``snapshot_date``
    намеренно исключены: каталог/фокус не меняют чанки/векторы, а снимок — провенанс,
    не содержимое. Это позволяет переиспользовать партицию при смене фокуса/каталога
    (09 §8.1). ``retrieval_*`` тоже не входят: ретривал-конфиг не меняет чанкинг/векторы.

    ``embed_device`` имеет значение по умолчанию ``""`` ради вызывающих, которые считают
    ``pb`` **без** объявленной оси (юнит-тесты формулы): фаза всегда передаёт ось явно
    (дефолт объявленной оси — ``cpu``), поэтому живой прогон не получает пустое значение.

    Ревизия формулы (``_PARTITION_SCHEMA``) всегда даёт новые id: старые партиции
    остаются на диске иммутабельно, но новой композицией не переиспользуются.
    """
    material = _FIELD_SEP.join(
        (
            _PARTITION_SCHEMA,
            package_id,
            product_id,
            chunker_version,
            embed_model_id,
            embed_device,
            extractor_version,
            lexical_engine_version,
            vector_store,
            product_content_token,
        )
    )
    return "pb_" + short_hash(material, 24)

def _product_content_token(chunks: Sequence[CorpusChunk]) -> str:
    """Отпечаток содержимого продукт-партиции: sorted ``(chunk_id, content_hash)``.

    Порядко-независим (сортировка пар) и чувствителен к любому изменению контента. Пары
    склеиваются ``_FIELD_SEP`` (``chunk_id`` не содержит разделителя) — однозначно.
    """
    material = _FIELD_SEP.join(
        f"{chunk_id}{_FIELD_SEP}{chunk_hash}"
        for chunk_id, chunk_hash in sorted(
            (c.chunk_id, c.content_hash or "") for c in chunks
        )
    )
    return short_hash(material, 24)

def _product_extractor_token(docs: Sequence[CorpusDoc]) -> str:
    """Композитная ``extractor_version`` **одного** продукта (09 §8.1).

    Собираются непустые ``CorpusDoc.extractor_version`` документов продукта,
    дедуплицируются и сортируются (порядко-независимо), склеиваются через запятую.
    При единогласии версий даёт одну строку; при расхождении (разные extract-маршруты) —
    детерминированный композит. Документы без версии игнорируются; если версий нет —
    пустая строка (вызывающий подставляет объявленную версию порта).
    """
    versions = sorted(
        {doc.extractor_version for doc in docs if doc.extractor_version}
    )
    return ",".join(versions)

def _chunk_id(product_id: str, doc_version_id: str, chunk_no: int) -> str:
    """Детерминированный глобально-уникальный run-level ``chunk_id`` (16 §6).

    Форма — ``f"{product_id}:{doc_version_id}:{chunk_no}"``. Второй сегмент — **версия
    документа** (``incremental.doc_version_id`` = ``"dv_" + short_hash(product,
    content_hash, extractor_version, level)``), а не голый ``content_hash``: хеш не
    различает уровень извлечения и версию экстрактора, поэтому ``layout``- и
    ``text``-документы одного текста давали **один** ``chunk_id`` при разном тексте.

    Грамматика ``source_id`` не меняется: ``doc_version_id`` не содержит двоеточий, а
    ``parse_source_id`` требует ровно 3 или 4 сегмента с целым третьим
    (``core/util/ids.py``). ``chunker_version`` в ``chunk_id`` **не** входит (16 §6.1):
    четвёртый сегмент ``source_id`` — это ``build_id``, и двух смыслов у одной позиции
    быть не может; смена чанкера и так меняет ``pb``.

    Идентификатор строится ядром (а не берётся у чанкера), чтобы гарантировать
    уникальность независимо от адаптера и воспроизводимость ``source_id``-резолва.
    """
    return f"{product_id}:{doc_version_id}:{chunk_no}"

@dataclass
class _EmbedCensus:
    """Счётчики работы эмбеддинга одного продукта: из кэша vs посчитано сейчас.

    Живёт в ``build_corpus`` (per продукт), наполняется :func:`_embed_batched` и
    читается наблюдаемостью (:func:`_make_embed_reporter`, итоговая строка продукта).
    Разделение принципиально для доказательности кэша: «сколько чанков эмбеддилось» и
    «сколько взято готовым» — разные величины, и только их пара отвечает на вопрос
    «сколько сэкономил кэш».

    ``processed = cached + embedded`` — сколько чанков продукта уже обслужено (это и есть
    прогресс ``embed_chunks``). ``hit_ratio_pct`` — целые проценты: наблюдаемость печатает
    целые, а не дробь, которую нечем сверить.
    """

    #: Чанки, вектор которых взят из кэша (эмбеддер не вызывался).
    cached: int = 0
    #: Чанки, вектор которых посчитан в этом прогоне (промахи кэша).
    embedded: int = 0

    @property
    def processed(self) -> int:
        """Сколько чанков уже обслужено (хиты + свежие)."""
        return self.cached + self.embedded

    @property
    def hit_ratio_pct(self) -> int:
        """Доля попаданий в процентах (0, если работы ещё не было)."""
        total = self.processed
        if total <= 0:
            return 0
        return round(100.0 * self.cached / total)

@dataclass
class _CorpusStageTimings:
    """Бакеты wall-времени **одного продукта** в фазе ``corpus`` (наблюдаемость, не контракт).

    Отвечают на вопрос «где уходит время», которого ``elapsed_seconds`` продукта не отвечает:
    он говорит «сколько», но не «на что». Ни одно поле отсюда не входит ни в
    ``EXECUTION_CONTRACT_FIELDS``, ни в ``pb``/``content_digest``/манифест: это измерение
    исполнения, а не условие измерения (иначе инструментирование меняло бы ``run_id``).

    **Что именно измерено — ожидание или работа.** Фетч и экстракция идут в пулах (в т.ч.
    процессов), поэтому главный поток видит **ожидание**, а не работу:

    * ``fetch_wait_s`` — участок фетча на главном потоке. Четыре точки, все суммируются
      сюда: ``entry.future.result()`` пула ``_local_fetch`` (локальный путь, **ожидание**);
      ``local_fetch`` на потоке однопоточного пула (браузерный путь — **работа**, потому
      что фетч идёт на выделенном потоке, а не на драйвере); ``_process_local``
      последовательного пути (``max_workers in (None, 1)`` — тоже **работа**; без этой точки
      фетч без пула уходил бы в ``overhead_s``) и ``_submit_collect`` (``split``: подача +
      опрос + сбор — **ожидание** исполнителя).
    * ``extract_wait_s`` — ожидание экстракции в потребителе конверсии (F5 плана ``17``:
      :func:`core.pipeline.extract_consume.consume`) и ``handle.result()`` в доводке члена
      контейнера. При окне
      ``extract_workers == 1`` это почти работа; при окне > 1 включает ожидание чужих
      заданий, поэтому окно публикуется рядом (``extract_workers``), иначе цифра читалась бы
      неверно.
    * ``chunk_s`` — **работа** на главном потоке: ``chunker.chunk(...)`` в двух точках
      (документ и член контейнера).
    * ``embed_s`` — ``_embed_batched`` внутри :func:`_embed_and_build` (работа главного
      потока: эмбеддер вызывается синхронно).
    * ``build_s`` — ``index_builder.build(...)`` (работа главного потока; время считается и
      при исключении — бакет не теряется).

    Остаток окна считается как ``elapsed - Σ бакетов`` и **не** зажимается в ``max(0, …)``:
    отрицательное значение — дефект инструментирования (бакет посчитан дважды или покрывает
    чужое окно), и он обязан быть виден, а не спрятан нулём.
    """

    #: Ожидание фетча на главном потоке (последовательный путь ``_process_local`` ‖
    #: ``_submit_collect`` split-пути).
    fetch_wait_s: float = 0.0
    #: Ожидание экстракции (``future.result`` страницы/артефакта и ``handle.result`` члена).
    extract_wait_s: float = 0.0
    #: Нарезка на главном потоке (документ и член контейнера).
    chunk_s: float = 0.0
    #: Эмбеддинг продукт-партиции (``_embed_batched``).
    embed_s: float = 0.0
    #: Физическая сборка продукт-партиции (``index_builder.build``).
    build_s: float = 0.0
    #: Фактическое окно экстракции (максимум по корням продукта); ``None`` — inline-путь.
    extract_workers: int | None = None
    #: Сколько **главный поток** провёл в закрытии продукта (конверсия очереди, Pass B,
    #: покрытие, строка продукта) — на это время выдача новых волн стоит (F9 плана 17).
    #: Это **не** бакет работы: интервал пересекается с ``embed_s``/``build_s``, поэтому
    #: в :meth:`stage_sum` он не входит, а публикуется рядом (``product_tail_s`` в
    #: ``resources.stage_timings``) — иначе остаток окна ушёл бы в минус.
    product_tail_s: float = 0.0

    def reset(self) -> None:
        """Обнуляет бакеты перед переиспользованием объекта.

        Носитель бакетов теперь **продуктовый** (``PhaseDrainScope.timings_for``): у
        каждого продукта свой объект, поэтому обнуление при смене продукта больше не
        нужно. Метод остаётся, потому что объектом пользуются и вне фазы (тесты,
        повторный замер), и потому что «начать заново» обязано обнулять все поля разом.
        """
        self.fetch_wait_s = 0.0
        self.extract_wait_s = 0.0
        self.chunk_s = 0.0
        self.embed_s = 0.0
        self.build_s = 0.0
        self.extract_workers = None
        self.product_tail_s = 0.0

    def note_extract_window(self, window: int | None) -> None:
        """Запоминает фактическое окно экстракции корня (максимум по корням продукта).

        Максимум, а не последнее значение: окно резолвится на корень, а бакет
        ``extract_wait_s`` — на продукт, и он включает ожидание чужих заданий по самому
        широкому окну.
        """
        if window is None:
            return
        candidate = int(window)
        if self.extract_workers is None or candidate > self.extract_workers:
            self.extract_workers = candidate

    def stage_sum(self) -> float:
        """Сумма пяти бакетов (сырая, без округления)."""
        return (
            self.fetch_wait_s
            + self.extract_wait_s
            + self.chunk_s
            + self.embed_s
            + self.build_s
        )

    def crawl_fields(self, elapsed_s: float) -> dict[str, Any]:
        """Бакеты окна краула (Pass A) + остаток **этого** окна.

        Эмбеддинг и сборка индекса (Pass B) идут после ``product_end``, поэтому в это окно
        не входят и нулями здесь не выдаются: их публикует ``product_index_end``.
        """
        pass_a = self.fetch_wait_s + self.extract_wait_s + self.chunk_s
        return {
            "fetch_wait_s": round(self.fetch_wait_s, 3),
            "extract_wait_s": round(self.extract_wait_s, 3),
            "chunk_s": round(self.chunk_s, 3),
            "extract_workers": self.extract_workers,
            # Не max(0, …): отрицательное — видимый дефект инструментирования.
            "overhead_s": round(elapsed_s - pass_a, 3),
        }

    def index_fields(self, elapsed_s: float) -> dict[str, Any]:
        """Бакеты Pass B + остаток **полного** окна продукта (все пять бакетов)."""
        return {
            "embed_s": round(self.embed_s, 3),
            "build_s": round(self.build_s, 3),
            "overhead_s": round(elapsed_s - self.stage_sum(), 3),
        }

@dataclass
class _EmbedThroughput:
    """Наблюдаемый темп батчевого эмбеддинга: EMA по батчам одного продукта.

    **Почему измерение, а не константа (A3).** Цена чанка зависит от длины текста, модели и
    CPU: замер этого хоста даёт 374 мс на 500 символов, 1173 мс на 1500 и 1916 мс на 3000.
    Предсказывать её константой значило бы объявить то, что измеряется; поэтому остаток
    оценивается по фактическому темпу **текущего** прогона.

    Сглаживание (``alpha`` = :data:`_EMBED_RATE_ALPHA`) нужно, чтобы первый батч — с ленивой
    загрузкой модели — не определял оценку на весь продукт, а один медленный батч не дёргал
    её на порядок.
    """

    #: Вес нового замера (0 < alpha ≤ 1).
    alpha: float = _EMBED_RATE_ALPHA
    #: Сглаженный темп, чанков/с (``None`` — замеров ещё не было).
    rate: float | None = None
    #: Сколько чанков уже эмбеддировано (по последнему замеру).
    done: int = 0
    #: Прошедшее время эмбеддинга на последнем замере, с.
    elapsed: float = 0.0

    def observe(self, done: int, elapsed: float) -> float | None:
        """Учитывает границу батча и возвращает сглаженный темп (чанков/с).

        Замер без прироста (нулевой интервал/повтор того же счётчика) темпа не меняет:
        делить на него нельзя, а подставлять ноль значило бы объявить остановку.
        """
        delta_n = done - self.done
        delta_t = elapsed - self.elapsed
        self.done = done
        self.elapsed = elapsed
        if delta_n <= 0 or delta_t <= 0:
            return self.rate
        sample = delta_n / delta_t
        self.rate = (
            sample
            if self.rate is None
            else self.alpha * sample + (1.0 - self.alpha) * self.rate
        )
        return self.rate

    def eta_seconds(self, done: int, total: int) -> int | None:
        """Оценка остатка, с (``None`` — темпа ещё нет; ``0`` — работа закончена)."""
        if done >= total:
            return 0
        if self.rate is None or self.rate <= 0:
            return None
        return round((total - done) / self.rate)

def format_embed_progress(
    done: int,
    total: int,
    rate: float | None,
    eta_s: int | None,
    *,
    cached: int | None = None,
) -> str:
    """Человекочитаемая строка эмбеддинга: ``N/M чанков, X чанк/с, осталось ~Y мин``.

    Остаток печатается только пока работа не закончена (``done < total``): «осталось ~0 мин»
    на завершённом продукте — шум, а не факт. Темп печатается с двумя знаками: при 1173 мс на
    чанк это 0.85 чанк/с, и округление до целого скрыло бы разницу между моделями и длинами
    текста — то есть ровно то, ради чего число публикуется.

    :param cached: сколько из ``done`` чанков взято из кэша векторов; ``None`` — кэша нет
        (строка остаётся прежней, байт-в-байт). При заданном числе публикуется и доля:
        без неё «сколько сэкономил кэш» не читается из строки.
    """
    parts = [f"{done}/{total} чанков"]
    if cached is not None:
        share = round(100.0 * cached / done) if done > 0 else 0
        parts.append(f"из кэша {cached} ({share}%)")
    if rate is not None and rate > 0:
        parts.append(f"{rate:.2f} чанк/с")
    if eta_s is not None and done < total:
        # Минуты — операторская единица для «десятков минут/часов»; меньше минуты честнее
        # назвать минутой, чем нулём.
        parts.append(f"осталось ~{max(1, round(eta_s / 60.0))} мин")
    return ", ".join(parts)

def _make_embed_reporter(
    *,
    heartbeat: Heartbeat,
    progress: Callable[[dict[str, int]], None] | None,
    store: Any,
    session: str | None,
    throughput: _EmbedThroughput,
    census: _EmbedCensus | None = None,
) -> Callable[[int, int, float], None]:
    """Колбэк границы батча эмбеддинга: признак жизни + прогресс с темпом и остатком.

    Эмбеддинг идёт **после** дренажа продукта, где :func:`core.pipeline.corpus_persist.emit_progress` уже не вызывается.
    До правки единственный вызов ``embed`` на весь продукт не оставлял в этом участке ни
    одной точки жизни: сеанс 2026-10-25 оставался ``running`` с heartbeat, замершим 1280 с,
    при 505 % CPU — работа выглядела как мёртвый сеанс.

    Каналы — существующие (``heartbeat`` и ``progress`` ``--follow``), новых не вводится.
    Ключи ``embed_*`` **дополняют** агрегат сеанса (``pending``/``fetched``/…), а не заменяют
    его: на границе эмбеддинга краул уже финализирован, и оба факта должны быть видны
    одновременно. Все значения — целые (контракт ``progress``): ``embed_chunks``/``embed_chunks_total``
    — «сколько из скольких», ``embed_elapsed_s`` — прошедшее время эмбеддинга,
    ``embed_chunks_per_min`` — фактический темп прогона, ``embed_eta_s`` — оценка остатка.
    При подключённом кэше векторов (:class:`~core.pipeline.embed_cache.EmbedCache`) к ним
    добавляются ``embed_cached`` — сколько чанков взято готовыми — и
    ``embed_hit_ratio_pct`` — их доля. Человекочитаемая форма тех же величин —
    :func:`format_embed_progress`.

    **``embed_chunks`` — обработанные чанки, ``embed_chunks_per_min`` — темп эмбеддинга.**
    Прогресс обязан считать хиты (иначе «из кэша 3000 из 3000» выглядело бы как стоящий на
    нуле продукт), а темп — только фактически посчитанные: это та же величина, что публикуют
    замеры пропускной способности, и она сравнивается между прогонами и моделями (A3). Оценка
    остатка берётся из неё и потому при большой доле хитов слегка завышена — в безопасную
    сторону («работает дольше, чем окажется»), а не в сторону мнимого зависания.

    Ошибки чтения стора/колбэка изолируются: прогресс информационен и не роняет фазу
    (наблюдаемость не измерение).
    """
    counts: dict[str, int] = {}

    def _report(done: int, total: int, elapsed: float) -> None:
        # Точка жизни — до любых ранних выходов: она не зависит от того, включён ли --follow.
        heartbeat.touch()
        # Наблюдение темпа — даже без колбэка: накопленный темп публикуется в итоговой
        # строке фазы (ценой «сколько это заняло»), а не только в живом прогрессе.
        # Без кэша (``census is None``) обработанные чанки и есть эмбеддированные.
        embedded = done if census is None else census.embedded
        rate = throughput.observe(embedded, elapsed)
        eta = throughput.eta_seconds(done, total)
        if progress is None:
            return
        if session is not None:
            # Прогресс информационен: агрегат сеанса дополняет живую строку, а его
            # недоступность не роняет фазу. Оператор видит остальные ключи строки
            # (``embed_*`` заполняются ниже безусловно) и итоговую строку продукта.
            with contextlib.suppress(Exception):
                counts.update(dict(store.crawl_session_counts(session)))
        counts["embed_chunks"] = done
        counts["embed_chunks_total"] = total
        counts["embed_elapsed_s"] = int(elapsed)
        if census is not None:
            counts["embed_cached"] = census.cached
            counts["embed_hit_ratio_pct"] = census.hit_ratio_pct
        if rate is not None and rate > 0:
            counts["embed_chunks_per_min"] = round(rate * 60.0)
        if eta is not None:
            counts["embed_eta_s"] = int(eta)
        # Сбой колбэка ``--follow`` не роняет эмбеддинг: результат прогона от него не
        # зависит, а полная картина всё равно публикуется итоговой строкой продукта.
        with contextlib.suppress(Exception):
            progress(dict(counts))

    return _report

def _embed_batched(
    embedder: EmbedderPort,
    chunks: Sequence[CorpusChunk],
    *,
    on_batch: Callable[[int, int, float], None] | None = None,
    batch_size: int = EMBED_BATCH_CHUNKS,
    cache: EmbedCache | None = None,
    embed_model_id: str = "",
    embed_device: str,
    census: _EmbedCensus | None = None,
) -> list[list[float]]:
    """Эмбеддинг чанков внешними батчами; порядок входа = порядок выхода (детерминизм, A3).

    Один батч — один вызов ``embed`` и одна точка наблюдения (:data:`EMBED_BATCH_CHUNKS`).
    Тексты собираются **по батчу**, а не списком на весь продукт: ``[c.text for c in chunks]``
    держал рядом с полным накопителем векторов ещё и полный список текстов.

    Возвращается полный список векторов: партицию собирает ``IndexBuilderPort.build(chunks,
    vectors, meta)`` целиком, поэтому пик «все векторы» задан контрактом порта, а не циклом.
    Батч лишь ограничивает **второй** материализуемый список (тексты) и даёт наблюдение.

    **Кэш векторов (опция).** При ``cache`` каждый чанк батча сначала спрашивается у кэша
    по ключу ``(content_hash текста, embed_model_id, embed_device)``; эмбеддер получает
    только промахи, свежие векторы сразу записываются в кэш, а хиты подставляются в **свои**
    позиции. Поэтому порядок входа = порядок выхода и при смешанном батче (инвариант
    держится структурой: слоты батча индексируются позицией чанка, а не порядком ответа
    эмбеддера). ``cache=None`` — прежнее поведение байт-в-байт: один вызов ``embed`` на
    весь батч.

    Кэш — оптимизация, а потому его сбой не должен стоить прогона: ``get``/``put`` устроены
    как промах/no-op на любой порче файла. Ошибка самого эмбеддера по-прежнему пробрасывается
    (её изолирует вызывающий): тихо подставить нули значило бы выдать выдуманный вектор за
    измеренный.

    :param embed_model_id: идентификатор модели — часть ключа кэша. Берётся у объявленного
        входа прогона (``EmbedderPort.model_id``), а не выдумывается здесь: кэш обязан
        смешивать векторы только одной модели.
    :param embed_device: устройство счёта — третья компонента ключа кэша (16 §5.3).
        Обязательный keyword: векторы CPU и CUDA численно разные, поэтому догадка о
        устройстве обслуженной записи была бы подменой значения провенансом. Значение —
        объявленная ось прогона, а не ответ адаптера (``EmbedderPort`` о ней не знает).
    :param census: счётчики per продукт (хиты/посчитанные); ``None`` — не считать.
    """
    total = len(chunks)
    vectors: list[list[float]] = []
    started = time.monotonic()
    # Размерность — часть проверки хитов: вектор другой размерности несовместим с моделью.
    # ``operator.index`` принимает и numpy-целые, иначе проверка молча выключилась бы.
    try:
        declared_dim = operator.index(getattr(embedder, "dim", None))
    except (TypeError, ValueError):
        declared_dim = 0
    expected_dim = declared_dim if declared_dim > 0 else None
    for start in range(0, total, batch_size):
        batch = chunks[start : start + batch_size]
        if cache is None:
            vectors.extend(embedder.embed([c.text for c in batch]))
            if census is not None:
                census.embedded += len(batch)
        else:
            slots: list[list[float] | None] = [None] * len(batch)
            pending: list[int] = []
            cached_now = 0
            for position, chunk in enumerate(batch):
                hit = cache.get(
                    chunk.content_hash or "",
                    embed_model_id,
                    embed_device=embed_device,
                    dim=expected_dim,
                )
                if hit is None:
                    pending.append(position)
                    continue
                slots[position] = hit
                cached_now += 1
            if pending:
                fresh = embedder.embed([batch[i].text for i in pending])
                if len(fresh) != len(pending):
                    raise ValueError(
                        "эмбеддер вернул "
                        f"{len(fresh)} векторов на {len(pending)} чанков"
                    )
                for offset, position in enumerate(pending):
                    vector = fresh[offset]
                    slots[position] = vector
                    # Пишем ровно то, что вернула модель: квантизация в fp16 — забота
                    # кэша, и она тождественна той, что делает сборщик индекса.
                    cache.put(
                        batch[position].content_hash or "",
                        embed_model_id,
                        vector,
                        embed_device=embed_device,
                        dim=expected_dim,
                    )
            if census is not None:
                census.cached += cached_now
                census.embedded += len(pending)
            for value in slots:
                if value is None:  # pragma: no cover — позиция заполняется всегда
                    raise ValueError("батч эмбеддинга оставил чанк без вектора")
                vectors.append(value)
        if on_batch is not None:
            on_batch(start + len(batch), total, time.monotonic() - started)
    return vectors

def _embed_and_build(
    embedder: EmbedderPort,
    index_builder: IndexBuilderPort,
    product_id: str,
    chunks: list[CorpusChunk],
    *,
    package_id: str,
    chunker_version: str,
    embed_model_id: str,
    embed_device: str,
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
    product_content_token: str,
    snapshot_date: str | None,
    partition_build_id: str,
    errors: list[str],
    on_batch: Callable[[int, int, float], None] | None = None,
    cache: EmbedCache | None = None,
    census: _EmbedCensus | None = None,
    timings: _CorpusStageTimings | None = None,
) -> bool:
    """Best-effort эмбеддинг и физическая сборка индекса продукт-партиции (K3, §B.1).

    ``pipeline`` не работает с файлами индекса напрямую: за ``.npy``-шарды/tantivy отвечает
    ``IndexBuilderPort``. Сбой изолируется в ``errors``.

    **Эмбеддинг — внешними батчами** (:data:`EMBED_BATCH_CHUNKS`), а не одним вызовом на весь
    продукт: между батчами фаза получает точку наблюдения (``on_batch`` → heartbeat +
    прогресс). Тексты батча материализуются по мере обхода; накопитель векторов полон по
    контракту ``IndexBuilderPort.build``. Порядок векторов совпадает с порядком чанков, а
    кратность внешнего батча внутреннему держит результат побитово равным одному вызову
    (детерминизм, A3).

    :param cache: кэш векторов (:class:`~core.pipeline.embed_cache.EmbedCache`); ``None`` —
        без кэша, поведение прежнее. Кэш не меняет ни ``pb``, ни манифест партиции: он
        сокращает **стоимость** эмбеддинга, а не условия измерения.
    :param embed_device: устройство счёта векторов (объявленная ось ``embed.device``).
        Идёт в ключ ``embed_cache`` и в манифест партиции (``extra["embed_device"]``):
        без него CUDA-прогон мог бы переиспользовать CPU-партицию/векторы (16 §5.1/§5.3).
        Значение приходит аргументом, а не спрашивается у адаптера.
    :param census: счётчики «из кэша / посчитано» этого продукта (наполняется эмбеддингом).
    :param timings: бакеты стадий продукта (наблюдаемость): сюда пишутся ``embed_s`` и
        ``build_s``. ``None`` — не измерять (юнит-путь). Оба бакета — **работа** главного
        потока: эмбеддер вызывается синхронно, ``build`` — тоже.
    :param on_batch: ``(done, total, elapsed_s)`` — граница батча; ``None`` — без наблюдения
        (юнит-путь). Ошибка колбэка не должна выдавать себя за сбой сборки: наблюдение
        изолируется вызывающим.
    :returns: ``True`` — если ``index_builder.build`` вернулся без исключения (партиция
        физически зафиксирована); ``False`` — при **любом** исключении (в т.ч.
        ``ImmutableBuildError`` при попытке изменить уже зафиксированную сборку, K2/§13).
        Вызывающий использует результат как гейт для записи ``corpus_chunks``/
        ``index_manifest`` («disk first, DB second»).
    """
    try:
        port_chunks = [
            PortChunk(
                chunk_id=c.chunk_id,
                chunk_no=c.chunk_no or 0,
                text=c.text,
                heading_path=c.heading_path or "",
                context_prefix=c.context_prefix or "",
                content_hash=c.content_hash or "",
            )
            for c in chunks
        ]
        _embed_started = time.perf_counter()
        try:
            vectors = _embed_batched(
                embedder,
                chunks,
                on_batch=on_batch,
                cache=cache,
                embed_model_id=embed_model_id,
                embed_device=embed_device,
                census=census,
            )
        finally:
            # Бакет не теряется и при исключении: время до отказа — измеренный факт.
            if timings is not None:
                timings.embed_s += time.perf_counter() - _embed_started
        meta = IndexMeta(
            package=package_id,
            chunker_version=chunker_version,
            embed_model_id=embed_model_id,
            snapshot_date=snapshot_date or "",
            product_id=product_id,
            extra={
                # 09 §8.2: физическая сборка адресуется **партицией**, не композицией.
                "partition_build_id": partition_build_id,
                # Контентный отпечаток партиции: сверяется вторым, физическим контролем
                # переиспользования (09 §8.5); id уже хеширует его, но диск проверяется
                # отдельно.
                "content_digest": product_content_token,
                # Версии производства/формата — для второго, физического контроля
                # переиспользования (09 §8.5): они уже свернуты в ``pb``, но манифест
                # на диске проверяется отдельно от id.
                "extractor_version": extractor_version,
                "lexical_engine_version": lexical_engine_version,
                "vector_store": vector_store,
                # 16 §5.1: место счёта векторов. Сборщик уже пишет его из ``extra``;
                # без записи CUDA-партиция была бы неотличима от CPU-партиции по манифесту.
                "embed_device": embed_device,
            },
        )
        _build_started = time.perf_counter()
        try:
            index_builder.build(port_chunks, vectors, meta)
        finally:
            # ``build_s`` — время **до** исключения: отказ сборки не отменяет того факта,
            # что этот участок столько стоил (бакет не должен «пропадать» при сбое).
            if timings is not None:
                timings.build_s += time.perf_counter() - _build_started
        return True
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        errors.append(f"{product_id}: index build: {type(exc).__name__}: {exc}")
        return False

def _partition_reuse_reason(
    *,
    index_root: str | Path | None,
    package_id: str,
    partition_build_id: str,
    product_id: str,
    product_content_token: str,
    chunker_version: str,
    embed_model_id: str,
    embed_device: str = "",
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
) -> str | None:
    """Причина, по которой партицию **нельзя** переиспользовать (``None`` — можно).

    Гейт переиспользования (09 §8.5) — два независимых уровня:

    1. **id-уровень.** ``partition_build_id`` уже хеширует все версии производства и
       ``product_content_token``; совпал id — совпали входы by construction.
    2. **физический уровень (эта проверка).** Партиция реально зафиксирована на диске
       (``is_committed``), а её ``manifest.json`` несёт тот же ``content_digest`` и те же
       ``chunker_version``/``embed_model_id``/``embed_device``/``extractor_version``/
       ``lexical_engine_version``/``vector_store``. Это защита от коллизии id, порчи и
       подмены артефакта: id — не доказательство целостности диска.
    3. **отпечаток содержимого (16 §5.2).** ``vector_digest`` пересчитывается **с диска**
       и сверяется с манифестом. Сборщик пишет его при сборке, а при переиспользовании
       сборка не строится — значит ``_guard_immutable`` не исполняется, и единственная
       проверка целостности ``chunk_ids``+``vectors.fp16.npy`` — этот пересчёт. Порядок
       важен: дешёвые сверки манифеста идут первыми, чтение матрицы — последним.

    Возвращает человекочитаемую причину отказа (для ``errors``) либо ``None``.
    Отсутствие каталога при первом прогоне — **не** ошибка (возвращается ``None`` только
    при пригодной партиции; вызывающий отличает «нет каталога» от «каталог неполон»).
    """
    if index_root is None:
        return "index_root не задан"
    if not _partition_is_committed(
        index_root, package_id, partition_build_id, product_id
    ):
        return "партиция не зафиксирована на диске"
    manifest = _read_partition_manifest(
        index_root, package_id, partition_build_id, product_id
    )
    expected = {
        "content_digest": product_content_token,
        "chunker_version": chunker_version,
        "embed_model_id": embed_model_id,
        # 16 §5.1: место счёта — часть условий сборки, а не провенанс. Партиция другого
        # устройства содержит другие числа под тем же содержимым.
        "embed_device": embed_device,
        "extractor_version": extractor_version,
        "lexical_engine_version": lexical_engine_version,
        "vector_store": vector_store,
    }
    for name, value in expected.items():
        actual = manifest.get(name)
        if str(actual if actual is not None else "") != str(value):
            return (
                f"манифест партиции расходится по {name}: "
                f"{actual!r} != {value!r}"
            )
    # 16 §5.2: отпечаток содержимого доказывается только пересчётом с диска. Партиция
    # формата ``partition-1`` его не несёт (в ``chunk_ids.jsonl`` нет ``content_hash``),
    # поэтому она читается, но не переиспользуется — и это названо, а не проглочено.
    on_disk = _recompute_vector_digest(
        _partition_root(index_root, package_id, partition_build_id, product_id)
    )
    if on_disk is None:
        return (
            "vector_digest недоказуем: в chunk_ids.jsonl нет content_hash "
            "(формат partition-1)"
        )
    declared = str(manifest.get("vector_digest") or "")
    if on_disk != declared:
        return (
            "vector_digest партиции расходится с манифестом: "
            f"{on_disk!r} != {declared!r}"
        )
    return None
