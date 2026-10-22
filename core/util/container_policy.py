"""Политика спуска в контейнеры — чистая логика без I/O (спуск, B2).

Модуль отвечает на три вопроса **до** любого чтения байтов членов:

1. **Какой путь члена допустим?** :func:`normalize_member_path` — член никогда не
   выходит за пространство имён архива (нет абсолютных путей, ``..``, диска, NUL).
2. **Что из состава вообще станет документом корпуса?** :func:`is_ingestible_member` —
   суффикс против **реестра** форматов (``core.util.document_formats.DOCUMENT_SUFFIXES``
   плюс текстовые цели спуска), а не второй копии списка.
3. **Сколько спуск позволит прочитать?** :func:`plan_members` — принятые/пропущенные
   члены с **причиной на каждый пропуск**, детерминированный порядок, представитель.

Модуль вынесен из ``pipeline`` намеренно: политика тестируется без store, прогона и
бэкенда контейнеров. Импортировать ``ports`` он не может (матрица зависимостей 01 §1:
``util`` → ``domain``/``util``), поэтому форма контракта объявлена структурно
(:class:`MemberLike`/:class:`ListingLike`), а источник истины остаётся один —
``core.ports.container``.

**Bomb-guard — политика, а не библиотека.** Заявленный в индексе размер
(:attr:`core.ports.container.ContainerMember.size`) — **первый** гейт, и только он:
он врёт (zip может объявить одно, отдать другое; 7z-блок солиден). Поэтому потолки — оси
``archive_max_member_bytes`` / ``archive_max_uncompressed_bytes`` / ``archive_max_members``,
а **жёсткий байтовый потолок на самом чтении** даёт :func:`bounded_iter` — потребитель
обязан обернуть им поток ``open_member``. Без второго гейта «заявлено 1 KiB» превращается
в «распаковано 4 GiB».

Инвариант «листинг — не извлечение» соблюдается и здесь: всё, что делает модуль, —
арифметика над индексом.

Нейтрален (§1.3).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Protocol

from core.util.document_formats import DOCUMENT_SUFFIXES

__all__ = [
    "AXIS_MAX_MEMBERS",
    "AXIS_MAX_MEMBER_BYTES",
    "AXIS_MAX_UNCOMPRESSED",
    "CAPACITY_REASONS",
    "DESCENT_TEXT_SUFFIXES",
    "FORECAST_EXAMPLES_MAX",
    "INGEST_SUFFIXES",
    "MemberPlan",
    "MemberTooLarge",
    "SKIP_ENCRYPTED",
    "SKIP_IS_DIRECTORY",
    "SKIP_MEMBER_LIMIT",
    "SKIP_MEMBER_TOO_LARGE",
    "SKIP_NOT_INGESTIBLE",
    "SKIP_PATH_REJECTED",
    "SKIP_TOTAL_EXCEEDED",
    "archive_forecast",
    "bounded_iter",
    "is_ingestible_member",
    "normalize_member_path",
    "plan_members",
]

#: Оси бюджета спуска (``crawl.*``, 09 §6.6). Значения — имена конфиг-ключей: политика не
#: читает конфиг, но обязана называть оси, которые её ограничивают.
AXIS_MAX_MEMBER_BYTES = "archive_max_member_bytes"
AXIS_MAX_UNCOMPRESSED = "archive_max_uncompressed_bytes"
AXIS_MAX_MEMBERS = "archive_max_members"

#: Текстовые цели спуска сверх документного реестра: спуск достаёт из контейнера не
#: только PDF/Office, но и разметку, которую экстрактор HTML/markdown разбирает штатно.
DESCENT_TEXT_SUFFIXES: tuple[str, ...] = (".md", ".markdown", ".txt", ".html", ".htm")

#: Полный набор суффиксов, которые спуск превращает в документы корпуса.
#: Документная часть **не** дублируется: она берётся из реестра форматов.
INGEST_SUFFIXES: tuple[str, ...] = DOCUMENT_SUFFIXES + DESCENT_TEXT_SUFFIXES

#: Причины пропуска члена (словарь видимых пропусков; текст — часть провенанса).
SKIP_IS_DIRECTORY = "is_directory"
SKIP_PATH_REJECTED = "path_rejected"
SKIP_ENCRYPTED = "encrypted"
SKIP_NOT_INGESTIBLE = "not_ingestible_format"
SKIP_MEMBER_TOO_LARGE = "member_too_large"
SKIP_MEMBER_LIMIT = "member_limit_exceeded"
SKIP_TOTAL_EXCEEDED = "total_uncompressed_exceeded"

#: Причины, означающие «план урезан бюджетом» (а не «член не подходит по природе»):
#: именно они поднимают ``MemberPlan.truncated``.
CAPACITY_REASONS: frozenset[str] = frozenset(
    {SKIP_MEMBER_TOO_LARGE, SKIP_MEMBER_LIMIT, SKIP_TOTAL_EXCEEDED}
)

#: Сколько примеров пропусков на причину попадает в прогноз (как
#: ``artifacts_outside_prefix``: счётчик полный, примеры ограничены).
FORECAST_EXAMPLES_MAX = 10

#: Ведущая буква диска (``C:``/``c:``) — путь с ней не принадлежит пространству имён архива.
_DRIVE_RE = re.compile(r"^[A-Za-z]:")


class MemberLike(Protocol):
    """Форма члена контейнера, нужная политике (структурно, без импорта ``ports``).

    Слой ``util`` не имеет права импортировать ``ports`` (матрица зависимостей 01 §1:
    ``util`` → ``domain``/``util``). Контракт остаётся один — ``core.ports.container``
    (там же инварианты); здесь объявлена лишь **форма**, которой он удовлетворяет.
    Свойства read-only: так ``ContainerMember`` (frozen dataclass) совместим по типам.
    """

    @property
    def path(self) -> str: ...

    @property
    def size(self) -> int: ...

    @property
    def is_dir(self) -> bool: ...


class ListingLike(Protocol):
    """Форма индекса контейнера, нужная политике (см. :class:`MemberLike`)."""

    @property
    def format(self) -> str: ...

    @property
    def members(self) -> Sequence[MemberLike]: ...

    @property
    def solid(self) -> bool | None: ...

    @property
    def encrypted(self) -> bool: ...

    @property
    def truncated(self) -> bool: ...

    @property
    def unreadable(self) -> str | None: ...


def normalize_member_path(raw: str) -> str | None:
    """Нормализованный POSIX-путь члена либо ``None`` (путь недопустим).

    Допустимый путь **не выходит** за пространство имён архива: относительный, без
    ``..``-сегментов, без ведущего ``/``, без буквы диска, без NUL. ``.`` и пустые
    сегменты схлопываются (``a//./b`` → ``a/b``), обратные слэши приводятся к прямым —
    иначе ``..\\..\\x`` из zip, собранного Windows-утилитой, выглядел бы одним
    безобидным сегментом на POSIX и остался бы обходом на Windows.

    Пустая строка, строка из одних разделителей или из одних пробелов → ``None``: у члена
    обязано быть имя.
    """
    if not raw or not raw.strip() or "\x00" in raw:
        return None
    candidate = raw.replace("\\", "/")
    if candidate.startswith("/") or _DRIVE_RE.match(candidate):
        return None
    parts: list[str] = []
    for segment in candidate.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            return None
        parts.append(segment)
    if not parts:
        return None
    return "/".join(parts)


def _suffix_set(suffixes: Iterable[str]) -> frozenset[str]:
    """Суффиксы в нижнем регистре с ведущей точкой (терпимо к ``pdf``/``.PDF``)."""
    normalized: set[str] = set()
    for raw in suffixes:
        value = str(raw).strip().lower()
        if not value:
            continue
        normalized.add(value if value.startswith(".") else f".{value}")
    return frozenset(normalized)


def _member_suffix(path: str) -> str:
    """Суффикс пути члена в нижнем регистре (``""`` — суффикса нет)."""
    return PurePosixPath(path).suffix.lower()


def is_ingestible_member(
    path: str, ingest_suffixes: Iterable[str] = INGEST_SUFFIXES
) -> bool:
    """Станет ли член документом корпуса (суффикс против реестра форматов).

    ``path`` — уже нормализованный путь (политика вызывает это после
    :func:`normalize_member_path`). Член без суффикса не ingestible: спуск не угадывает
    тип по содержимому — это дело экстрактора, а не допуска.
    """
    suffix = _member_suffix(path)
    return bool(suffix) and suffix in _suffix_set(ingest_suffixes)


@dataclass(frozen=True)
class MemberPlan:
    """Результат планирования спуска по индексу контейнера.

    * ``accepted`` — принятые члены **в сыром виде** (``path`` — как объявлен
      контейнером: именно его принимает ``open_member``), отсортированные по
      нормализованному пути. Нормализованный путь — идентичность члена:
      ``normalize_member_path(member.path)``; она же пара к ``archive_content_hash``
      (``(archive_content_hash, normalized_member_path)``).
    * ``skipped`` — ``(путь, причина)`` в том же порядке; путь — нормализованный, а для
      отвергнутого пути (``path_rejected``) — сырой, потому что нормализовать его нельзя.
    * ``total_uncompressed`` — сумма **заявленных** размеров принятых членов.
    * ``truncated`` — план урезан бюджетом (``CAPACITY_REASONS``) либо индекс неполон
      (``ContainerListing.truncated``); «пропущено по природе» (каталог, не тот формат,
      шифрование) сюда **не** входит.
    * ``representative`` — **наименьший нормализованный путь** среди принятых (решение
      дизайна: один представитель нужен там, где на весь контейнер полагается одна
      карточка корпуса).
    """

    accepted: tuple[MemberLike, ...]
    skipped: tuple[tuple[str, str], ...]
    total_uncompressed: int
    truncated: bool
    representative: str | None


def plan_members(
    listing: ListingLike,
    *,
    max_members: int | None = None,
    max_member_bytes: int | None = None,
    max_total_uncompressed: int | None = None,
    ingest_suffixes: Iterable[str] = INGEST_SUFFIXES,
) -> MemberPlan:
    """Решить судьбу каждого члена по индексу (без чтения тел).

    Потолки ``None`` означают «оси не заданы» (без потолка). Порядок обхода —
    **детерминированный**: сортировка по нормализованному пути (сырой путь — тай-брейк),
    поэтому одинаковый индекс даёт одинаковый план при любом порядке записей в архиве.

    Прецедент причин (первая подходящая побеждает): ``is_directory`` →
    ``path_rejected`` → ``encrypted`` → ``not_ingestible_format`` → ``member_too_large`` →
    ``member_limit_exceeded`` → ``total_uncompressed_exceeded``. Структурные и
    содержательные отказы идут раньше бюджетных: иначе «не тот формат» маскировался бы
    под исчерпанный бюджет и прогноз врал бы о причине.

    Шифрование — свойство **контейнера** (в индексе нет флага на члена): при
    ``listing.encrypted`` ни один член не принимается, причина одна и та же —
    ``encrypted``. Спуск в зашифрованный архив без пароля — не «пустой архив».
    """
    entries: list[tuple[str | None, MemberLike]] = [
        (normalize_member_path(member.path), member) for member in listing.members
    ]
    entries.sort(key=lambda item: (item[0] or item[1].path, item[1].path))
    suffixes = _suffix_set(ingest_suffixes)

    accepted: list[MemberLike] = []
    skipped: list[tuple[str, str]] = []
    total = 0
    truncated = bool(listing.truncated)

    for normalized, member in entries:
        display = normalized if normalized is not None else member.path
        if member.is_dir:
            skipped.append((display, SKIP_IS_DIRECTORY))
            continue
        if normalized is None:
            skipped.append((member.path, SKIP_PATH_REJECTED))
            continue
        if listing.encrypted:
            skipped.append((display, SKIP_ENCRYPTED))
            continue
        if _member_suffix(normalized) not in suffixes:
            skipped.append((display, SKIP_NOT_INGESTIBLE))
            continue
        if max_member_bytes is not None and member.size > max_member_bytes:
            skipped.append((display, SKIP_MEMBER_TOO_LARGE))
            truncated = True
            continue
        if max_members is not None and len(accepted) >= max_members:
            skipped.append((display, SKIP_MEMBER_LIMIT))
            truncated = True
            continue
        if (
            max_total_uncompressed is not None
            and total + member.size > max_total_uncompressed
        ):
            skipped.append((display, SKIP_TOTAL_EXCEEDED))
            truncated = True
            continue
        accepted.append(member)
        total += member.size

    representative = (
        normalize_member_path(accepted[0].path) if accepted else None
    )
    return MemberPlan(
        accepted=tuple(accepted),
        skipped=tuple(skipped),
        total_uncompressed=total,
        truncated=truncated,
        representative=representative,
    )


def _suffix_histogram(members: Iterable[MemberLike]) -> dict[str, int]:
    """Гистограмма суффиксов принятых членов (ключи отсортированы — детерминизм)."""
    counts: dict[str, int] = {}
    for member in members:
        suffix = _member_suffix(member.path)
        counts[suffix] = counts.get(suffix, 0) + 1
    return {suffix: counts[suffix] for suffix in sorted(counts)}


def archive_forecast(listing: ListingLike, plan: MemberPlan) -> dict:
    """Обязательная запись прогноза **до** извлечения (форма будущих ``archive_observations``).

    Форма (стабильные ключи; значения сериализуемы в JSON):

    * ``format`` — ``"zip"``/``"7z"``/``"rar"``;
    * ``members_total`` — сколько записей в индексе;
    * ``members_accepted`` — сколько спуск возьмёт;
    * ``formats`` — гистограмма суффиксов **принятых** членов (``{".pdf": 3, ".txt": 1}``);
    * ``total_uncompressed`` — сумма заявленных размеров принятых;
    * ``solid`` — ``true``/``false``/``null`` («формат не сообщает», см. порт);
    * ``encrypted`` — требует ли контейнер пароля;
    * ``skipped`` — ``{причина: {"count": N, "examples": [...]}}``, причины с нулём
      отсутствуют, примеры ограничены :data:`FORECAST_EXAMPLES_MAX` (счётчик — полный);
    * ``truncated`` — план урезан бюджетом или индекс неполон;
    * ``representative`` — наименьший нормализованный путь среди принятых;
    * ``unreadable`` — причина, если листинг не удался (иначе ``null``).

    Прогноз фиксируется **до** извлечения намеренно: он объясняет, почему спуск взял
    меньше, чем «есть в архиве», и переживает отказ посреди извлечения.
    """
    buckets: dict[str, dict] = {}
    for path, reason in plan.skipped:
        bucket = buckets.setdefault(reason, {"count": 0, "examples": []})
        bucket["count"] += 1
        if len(bucket["examples"]) < FORECAST_EXAMPLES_MAX:
            bucket["examples"].append(path)
    return {
        "format": listing.format,
        "members_total": len(listing.members),
        "members_accepted": len(plan.accepted),
        "formats": _suffix_histogram(plan.accepted),
        "total_uncompressed": plan.total_uncompressed,
        "solid": listing.solid,
        "encrypted": listing.encrypted,
        "skipped": {reason: buckets[reason] for reason in sorted(buckets)},
        "truncated": plan.truncated,
        "representative": plan.representative,
        "unreadable": listing.unreadable,
    }


class MemberTooLarge(RuntimeError):
    """Поток члена пробил жёсткий байтовый потолок (второй гейт bomb-guard).

    Несёт ``limit`` и ``consumed``: «сколько прочитано к моменту отказа» — то, что нужно
    в провенансе, чтобы отличить «член правда больше» от «потолок выставлен слишком туго».
    """

    def __init__(self, limit: int, consumed: int) -> None:
        super().__init__(
            f"member_too_large: прочитано {consumed} Б при потолке {limit} Б "
            f"({AXIS_MAX_MEMBER_BYTES})"
        )
        self.limit = limit
        self.consumed = consumed


def bounded_iter(chunks: Iterable[bytes], limit: int) -> Iterator[bytes]:
    """Обернуть поток чанков жёстким потолком ``limit`` байт (второй гейт bomb-guard).

    Заявленный в индексе размер доверяется только как первый гейт: этот итератор
    **считает фактические** байты и поднимает :class:`MemberTooLarge` на первом чанке,
    который выводит сумму за ``limit``. Пробивший чанк **не** отдаётся: частичное тело
    хуже отказа — оно выглядело бы как успешный (усечённый) документ.
    """
    if limit < 0:
        raise ValueError("bounded_iter: потолок не может быть отрицательным")
    total = 0
    for chunk in chunks:
        total += len(chunk)
        if total > limit:
            raise MemberTooLarge(limit, total)
        yield chunk
