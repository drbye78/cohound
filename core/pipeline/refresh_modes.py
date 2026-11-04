"""coHound — **одна** таблица диспетчеризации политики наблюдения (план 19 §P3).

Зачем отдельный модуль. До этого слоя решения «ходит ли прогон в сеть», «участвует ли
discovery-diff», «переиспользуем ли версию или спрашиваем сервер» и «откуда берётся
корпус» были разбросаны по условиям ``policy is RefreshPolicy.OFFLINE`` в трёх модулях
(``corpus``, ``incremental``, ``corpus_ingest``). Каждое такое условие — точка, где новое
значение enum'а молча получает **старое** поведение: ``frozen`` без явной ветки означал бы
«сеть разрешена» (тихо в сеть) или «переиспользовать чанки» (тихо в кэш). Оба исхода —
ложь об условиях измерения, поэтому решение собрано в одном месте и без фолбэка:
неизвестное значение — :class:`AssertionError`, а не «по умолчанию как ``auto``».

Таблица — не удобство, а инвентарь: ``tests/pipeline/test_refresh_modes.py`` проверяет
статически, что **каждое** значение :class:`~core.domain.enums.RefreshPolicy` названо
строкой таблицы. Модуль не читает БД и не делает сетевых вызовов; он переводит
объявленную политику в исполнение.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from core.domain.enums import (
    CrawlCorpusSource,
    FrozenSource,
    IncrementalKind,
    RefreshPolicy,
)

__all__ = [
    "FROZEN_SOURCE_NOT_APPLICABLE",
    "FrozenSourceArg",
    "RefreshExecution",
    "canonical_frozen_source",
    "dispatch_refresh",
    "resolve_frozen_source",
]

#: Каноническое значение оси ``frozen_source``, когда политика **не** ``frozen``
#: (план 19 §6.3). Не ``None``: контракт пишет значение явно, и «ось не действует»
#: отличимо от «поле не передано» (последнее — ``None`` у чистых вызовов).
FROZEN_SOURCE_NOT_APPLICABLE = "n/a"

#: Тип входного значения оси: объявленная строка из CLI либо ``None`` (не объявлена).
FrozenSourceArg = str | FrozenSource | None


@dataclass(frozen=True)
class RefreshExecution:
    """Исполнение политики наблюдения — факты, которые читают все потребители.

    :param network: разрешены ли сетевые вызовы (фетч, условный GET, sitemap/feed,
        внешний поиск). ``False`` — **ни одного**: этим же флагом глушится SERP-контур.
    :param discovery: участвует ли discovery-diff вызывающего (``known_new_urls``):
        URL, которых нет в ``url_state``. Без сети он не имеет смысла — их неоткуда взять
        кроме сети, и план не должен обещать фетч.
    :param known_kind: что делается с **известными** URL (``url_state``):
        ``revalidate`` — условный GET, ``reuse`` — переиспользование durable-субстрата.
    :param validators: отправлять ли ``ETag``/``Last-Modified``. ``refresh`` обязан
        получить свежий ответ, поэтому валидаторы не отправляются даже при известном URL;
        ``offline``/``frozen`` не отправляют ничего, потому что сети нет.
    :param corpus_source: откуда берётся корпус. Для ``frozen`` это ось прогона
        (:class:`~core.domain.enums.FrozenSource`), поэтому значение подставляется
        :func:`dispatch_refresh` по объявленному источнику, а не угадывается.
    """

    network: bool
    discovery: bool
    known_kind: IncrementalKind
    validators: bool
    corpus_source: CrawlCorpusSource


#: **Единственная** таблица решений: строка на каждое значение ``RefreshPolicy``.
#:
#: Ни одно значение не получается «по умолчанию»: ``dict.get(policy, network*)`` вернул бы
#: сеть для опечатки, а ``dict.get(policy, chunks*)`` — тихое переиспользование. Для
#: ``frozen`` в таблице стоит источник-заглушка ``bytes`` — он же объявленный дефолт оси;
#: фактический источник подставляет :func:`dispatch_refresh` (``chunks`` возвращает
#: поведение ``offline``, план 19 §5).
_REFRESH_DISPATCH: Mapping[RefreshPolicy, RefreshExecution] = {
    RefreshPolicy.AUTO: RefreshExecution(
        network=True,
        discovery=True,
        known_kind=IncrementalKind.REVALIDATE,
        validators=True,
        corpus_source=CrawlCorpusSource.NETWORK,
    ),
    RefreshPolicy.REVALIDATE: RefreshExecution(
        network=True,
        discovery=True,
        known_kind=IncrementalKind.REVALIDATE,
        validators=True,
        corpus_source=CrawlCorpusSource.NETWORK,
    ),
    RefreshPolicy.REFRESH: RefreshExecution(
        network=True,
        discovery=True,
        known_kind=IncrementalKind.REVALIDATE,
        # Безусловный GET: сервер обязан ответить свежим представлением.
        validators=False,
        corpus_source=CrawlCorpusSource.NETWORK,
    ),
    RefreshPolicy.OFFLINE: RefreshExecution(
        network=False,
        discovery=False,
        known_kind=IncrementalKind.REUSE,
        validators=False,
        corpus_source=CrawlCorpusSource.CHUNKS,
    ),
    RefreshPolicy.FROZEN: RefreshExecution(
        network=False,
        discovery=False,
        known_kind=IncrementalKind.REUSE,
        validators=False,
        corpus_source=CrawlCorpusSource.BYTES,
    ),
}

#: Ось ``frozen_source`` → источник корпуса. Отдельная таблица, потому что это **другая**
#: ось (чем именно заморожен корпус), а не ещё одно значение политики. Ветка на каждое
#: значение + ``AssertionError`` на неизвестном: тихого «возьмём байты» нет.
_FROZEN_SOURCE_TO_CORPUS: Mapping[FrozenSource, CrawlCorpusSource] = {
    FrozenSource.BYTES: CrawlCorpusSource.BYTES,
    FrozenSource.CHUNKS: CrawlCorpusSource.CHUNKS,
}


def resolve_frozen_source(value: FrozenSourceArg) -> FrozenSource:
    """Объявленная ось ``frozen_source`` → enum; ``None`` — объявленный дефолт ``bytes``.

    Дефолт именно ``bytes``, а не ``chunks``: смысл ``frozen`` по решению владельца —
    «корпус из сохранённых байтов» (план 19 §2). ``chunks`` — явный запрос вернуть
    поведение ``offline``, и он должен быть назван оператором.

    :raises ValueError: значение вне ``bytes|chunks`` (а не «молча bytes»).
    """
    return FrozenSource.BYTES if value is None else FrozenSource(value)


def canonical_frozen_source(
    policy: RefreshPolicy | str, frozen_source: FrozenSourceArg
) -> str:
    """Значение поля контракта ``frozen_source`` с канонизацией (план 19 §6.3).

    Ось действует только при ``refresh_policy=frozen``; при любой другой политике
    канонизируется в ``n/a``. Иначе ``--frozen-source chunks``, поданный к ``auto``-прогону,
    давал бы ``execution_change`` у прогона, который эту ось не исполняет.

    При ``frozen`` в контракт идёт **эффективное** значение (объявленное либо дефолт
    ``bytes``): «флаг не подан» и «флаг подан как ``bytes``» — одно исполнение, значит и
    один ``run_id``.
    """
    normalized = RefreshPolicy(policy)
    if normalized is not RefreshPolicy.FROZEN:
        return FROZEN_SOURCE_NOT_APPLICABLE
    return resolve_frozen_source(frozen_source).value


def dispatch_refresh(
    policy: RefreshPolicy | str, *, frozen_source: FrozenSourceArg = None
) -> RefreshExecution:
    """Исполнение объявленной политики — из **одной** таблицы, без фолбэка.

    :param policy: политика прогона (значение ``RefreshPolicy`` либо его строка).
    :param frozen_source: ось ``frozen_source``. Для политик, отличных от ``frozen``,
        аргумент не читается: там ось канонически не действует (§6.3), и «забыли
        передать» не должно менять исполнение.
    :raises ValueError: политика вне ``RefreshPolicy`` (тихий ``auto`` — сеть по опечатке).
    :raises AssertionError: значение enum'а добавлено, а строка таблицы — нет.
    """
    normalized = RefreshPolicy(policy)
    try:
        execution = _REFRESH_DISPATCH[normalized]
    except KeyError:  # значение enum добавили, исполнение — нет
        raise AssertionError(
            f"refresh_modes: политика {normalized!r} без строки в таблице исполнения"
        ) from None
    if normalized is not RefreshPolicy.FROZEN:
        return execution
    try:
        source = _FROZEN_SOURCE_TO_CORPUS[resolve_frozen_source(frozen_source)]
    except KeyError:  # источник добавили, отображение — нет
        raise AssertionError(
            f"refresh_modes: источник {frozen_source!r} без отображения в corpus_source"
        ) from None
    return replace(execution, corpus_source=source)