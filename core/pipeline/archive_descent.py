"""Спуск в контейнеры — исполнение плана членов (B3).

Срез B2 дал субстрат (порт, адаптеры, политика отбора); этот модуль **исполняет** спуск:
читает индекс контейнера, фиксирует прогноз **до** извлечения, стримит принятые члены в
один временный файл и передаёт каждый член на ингест вызывающему.

**Границы слоя.** Модуль не импортирует ``adapters`` и не знает про экстрактор/чанкер:
читатель контейнера и обработчик члена инъектируются (``container_for``/``ingest_member``)
— ровно так же, как краулеры и экстракторы приходят в фазу из composition root (A1).
Извлечение текста, дедуп и нарезка — дело фазы корпуса; здесь только механика контейнера.

**Вложенность без потолка глубины — но конечная.** Владелец решил: искусственного
потолка глубины нет. Конечность дают два независимых механизма:

* **цикл по хешу содержимого** — множество хешей архивов, уже пройденных в этом корне
  спуска: архив, чьи байты совпали с уже виденным, повторно не разворачивается
  (``archive_cycle``). Именно это делает «архив, содержащий копию себя» конечным;
* **кумулятивный бюджет корня** (``archive_max_total_uncompressed_bytes``) — оси
  ``archive_max_*`` ограничивают **один** контейнер, поэтому 10 000 вложенных архивов,
  каждый из которых «в бюджете», иначе исчерпали бы CPU/диск. Исчерпание бюджета —
  **видимая** запись ``archive_budget_exhausted``, а не тихое завершение.

**Прогноз — до извлечения.** :func:`core.util.container_policy.archive_forecast`
фиксируется в наблюдении раньше первого байта члена: он объясняет, почему спуск взял
меньше, чем «есть в архиве», и переживает отказ посреди спуска.

**Байты членов не ретейнятся.** Архив — сырьё (I19), член — производный вид (§6.7):
член стримится в **один** временный файл и удаляется сразу после обработки; в БД остаётся
идентичность ``(archive_content_hash, normalized_member_path)`` и путь архива, по которому
член ре-выводится тем же ``open_member``.

Нейтрален (§1.3).
"""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from core.domain.enums import ArchiveDescent, ArchiveDescentStatus
from core.domain.models import ArchiveObservation, CorpusMember
from core.ports.container import ContainerPort
from core.util.container_policy import (
    FORECAST_EXAMPLES_MAX,
    INGEST_SUFFIXES,
    MemberTooLarge,
    archive_forecast,
    bounded_iter,
    normalize_member_path,
    plan_members,
)
from core.util.document_formats import SUPPORTED_ARCHIVE_FORMATS
from core.util.hashing import short_hash

__all__ = [
    "DESCENT_SUFFIXES",
    "DescentBudget",
    "ArchiveDescentWalker",
    "MemberOutcome",
    "MemberTask",
    "SKIP_BUDGET",
    "SKIP_CONTAINER_READER",
    "SKIP_CYCLE",
    "SKIP_EXTRACT_FAILED",
    "SKIP_EXTRACT_UNAVAILABLE",
    "SKIP_NESTED",
    "SKIP_READ_FAILED",
    "SKIP_STREAM_TOO_LARGE",
    "member_url",
]

#: Сколько байт члена читается за один шаг стрима (1 MiB): пик RAM не зависит от члена.
CHUNK_SIZE = 1024 * 1024

#: Суффиксы, которые спуск рассматривает как **члены**: документы/разметка (реестр
#: форматов + текстовые цели) плюс контейнеры (вложенный архив — тоже член, он спускается).
DESCENT_SUFFIXES: tuple[str, ...] = INGEST_SUFFIXES + tuple(
    f".{fmt}" for fmt in SUPPORTED_ARCHIVE_FORMATS
)

#: Архив, чьи байты уже пройдены в этом корне спуска, повторно не разворачивается.
SKIP_CYCLE = "archive_cycle"
#: Кумулятивный бюджет корня исчерпан: спуск остановлен видимо.
SKIP_BUDGET = "archive_budget_exhausted"
#: Извлечение члена отказало (экстрактор вернул ошибку/бросил) — видимый пропуск.
SKIP_EXTRACT_FAILED = "extract_failed"
#: Экстрактор не умеет работать с локальными байтами (``split``: DMZ-путь по URL).
SKIP_EXTRACT_UNAVAILABLE = "extract_unavailable"
#: Поток члена пробил жёсткий байтовый потолок (второй гейт bomb-guard).
SKIP_STREAM_TOO_LARGE = "member_stream_too_large"
#: Член-контейнер спущен (его содержимое — отдельное наблюдение, не документ).
SKIP_NESTED = "nested_descended"
#: Тело члена не прочиталось (битый член, нет бэкенда на чтение).
SKIP_READ_FAILED = "member_read_failed"
#: Читатель контейнера не подключён composition root'ом (спуск невозможен).
SKIP_CONTAINER_READER = "container_reader_unavailable"


@dataclass(frozen=True)
class DescentBudget:
    """Границы спуска: три оси **контейнера** + одна кумулятивная ось **корня**.

    ``None`` — ось не задана (без потолка). ``max_total_uncompressed_bytes`` — сумма
    фактически прочитанных байт по **всему** корню спуска (все уровни вложенности).
    """

    max_member_bytes: int | None = None
    max_uncompressed_bytes: int | None = None
    max_members: int | None = None
    max_total_uncompressed_bytes: int | None = None


@dataclass(frozen=True)
class MemberTask:
    """Член, готовый к ингесту: временный файл + вся провенанс-идентичность.

    ``temp_path`` живёт только на время вызова :attr:`ArchiveDescentWalker` — обработчик
    обязан прочитать его **до** возврата (байты члена не ретейнятся, §6.7).
    ``content_hash`` — хеш байтов члена (для цикла и для дедупа), ``declared_size`` —
    заявленный размер из индекса.
    """

    product_id: str
    root_url: str
    archive_url: str
    archive_content_hash: str
    archive_path: str
    member_path: str
    member_url: str
    temp_path: str
    suffix: str
    declared_size: int
    content_hash: str
    depth: int


@dataclass(frozen=True)
class MemberOutcome:
    """Результат ингеста одного члена: документ корпуса **или** причина пропуска.

    ``content_hash`` — хеш **извлечённого** текста (тот же, что у ``CorpusDoc``); он
    попадает в ``corpus_members.extracted_content_hash``, а не хеш байтов члена.
    """

    doc_id: str | None = None
    skip_reason: str | None = None
    detail: str | None = None
    content_hash: str | None = None


def member_url(archive_url: str, member_path: str) -> str:
    """Адрес члена как URL провенанса: ``{archive_url}!/{member_path}``.

    Это **не** сетевой адрес (по нему нельзя фетчить) — конвенция провенанса: видно, из
    какого архива пришёл документ. Схема ``!/`` выбрана потому, что не встречается в
    обычных URL и однозначно отделяет архив от пути внутри него.
    """
    return f"{archive_url}!/{member_path}"


def _bounded_histogram(entries: list[tuple[str, str]]) -> dict[str, dict[str, Any]]:
    """``{причина: {"count": N, "examples": [...]}}`` — счёт полный, примеры ограничены."""
    buckets: dict[str, dict[str, Any]] = {}
    for path, reason in entries:
        bucket = buckets.setdefault(reason, {"count": 0, "examples": []})
        bucket["count"] += 1
        if len(bucket["examples"]) < FORECAST_EXAMPLES_MAX:
            bucket["examples"].append(path)
    return {reason: buckets[reason] for reason in sorted(buckets)}


class ArchiveDescentWalker:
    """Спуск в контейнеры одного корня: индекс → прогноз → члены → вложенность.

    Один экземпляр — один **корень спуска** (архив, допущенный в корпус по URL): в нём
    живут кумулятивный счётчик бюджета и множество пройденных хешей, поэтому вложенные
    архивы не могут ни зациклиться, ни выйти за кумулятивный потолок.

    Наблюдения (``observations``) и строки членов (``members``) накапливаются в списках
    вызывающего — фаза корпуса персистит их и отдаёт в ``PhaseResult``.
    """

    def __init__(
        self,
        *,
        product_id: str,
        root_url: str,
        descent: ArchiveDescent,
        budget: DescentBudget,
        container_for: Callable[[str | Path], ContainerPort | None] | None,
        ingest_member: Callable[[MemberTask], MemberOutcome],
        observations: list[ArchiveObservation],
        members: list[CorpusMember],
        errors: list[str],
        run_id: str | None = None,
        created_at: str | None = None,
    ) -> None:
        self.product_id = product_id
        self.root_url = root_url
        self.descent = ArchiveDescent(descent)
        self.budget = budget
        self._container_for = container_for
        self._ingest_member = ingest_member
        self.observations = observations
        self.members = members
        self.errors = errors
        self.run_id = run_id
        self.created_at = created_at
        #: Хеши архивов, уже пройденных в этом корне (цикл по содержимому).
        self._seen: set[str] = set()
        #: Кумулятивно прочитанные байты членов по всему корню.
        self._total = 0
        #: Один временный файл на **члена**: архив целиком в файл не пишется никогда, а
        #: вложенный спуск не затирает файл родителя (у каждого уровня свои имена).
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self._tmp_counter = 0

    # -- публичный вход ----------------------------------------------------- #

    def descend(
        self, *, archive_path: str | Path, archive_url: str, archive_content_hash: str
    ) -> ArchiveObservation:
        """Спуститься в архив и вернуть наблюдение **корня** (вложенные — в списке).

        ``archive_content_hash`` — хеш ретейленных байтов (I19): он же идентичность
        архива в ``corpus_members`` и первый элемент множества пройденных хешей.
        """
        self._seen = {archive_content_hash}
        self._total = 0
        try:
            return self._walk(
                archive_path=Path(archive_path),
                archive_url=archive_url,
                archive_content_hash=archive_content_hash,
                depth=0,
            )
        finally:
            if self._tmpdir is not None:
                self._tmpdir.cleanup()
                self._tmpdir = None

    # -- внутреннее --------------------------------------------------------- #

    def _reader(self, path: Path) -> ContainerPort | None:
        """Читатель контейнера для файла (``None`` — не подключён/не распознан)."""
        if self._container_for is None:
            return None
        try:
            return self._container_for(path)
        except Exception:  # noqa: BLE001 — выбор читателя не должен ронять фазу
            return None

    def _temp_path(self) -> Path:
        """Новый временный файл члена (создаётся лениво, каталог чистится в ``descend``)."""
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix="cohound-descent-")
        self._tmp_counter += 1
        return Path(self._tmpdir.name) / f"member-{self._tmp_counter}.bin"

    def _remaining_total(self) -> int | None:
        """Остаток кумулятивного бюджета корня (``None`` — ось не задана)."""
        limit = self.budget.max_total_uncompressed_bytes
        if limit is None:
            return None
        return max(0, int(limit) - self._total)

    def _observation_id(self, archive_content_hash: str, depth: int) -> str:
        """Детерминированный id наблюдения (повторный спуск не плодит строки)."""
        material = "|".join(
            (
                str(self.run_id or ""),
                self.product_id,
                self.root_url,
                archive_content_hash,
                str(depth),
            )
        )
        return f"ao_{short_hash(material)}"

    def _record(
        self,
        *,
        archive_path: Path,
        archive_url: str,
        archive_content_hash: str,
        depth: int,
        status: ArchiveDescentStatus,
        forecast: dict[str, Any],
        outcome: dict[str, Any],
        reason: str | None,
    ) -> ArchiveObservation:
        """Собирает и складывает наблюдение (в список фазы) — единая точка записи."""
        observation = ArchiveObservation(
            observation_id=self._observation_id(archive_content_hash, depth),
            run_id=self.run_id,
            product_id=self.product_id,
            root_url=self.root_url,
            archive_url=archive_url,
            archive_content_hash=archive_content_hash,
            archive_path=str(archive_path),
            depth=depth,
            descent=self.descent,
            status=status,
            forecast=forecast,
            outcome=outcome,
            reason=reason,
            created_at=self.created_at,
        )
        self.observations.append(observation)
        return observation

    def _outcome(
        self,
        *,
        members_total: int,
        members_accepted: int,
        ingested: int,
        failed: int,
        skipped: list[tuple[str, str]],
        bytes_written: int,
        stopped_reason: str | None,
        cycle_stops: int,
    ) -> dict[str, Any]:
        """Исход спуска: что сделано и почему остановились (видимо, не «молча»)."""
        return {
            "members_total": members_total,
            "members_accepted": members_accepted,
            "members_ingested": ingested,
            "members_failed": failed,
            "members_skipped": len(skipped),
            "skipped": _bounded_histogram(skipped),
            "bytes_written": bytes_written,
            "stopped_reason": stopped_reason,
            "cycle_stops": cycle_stops,
            "budget_limit_bytes": self.budget.max_total_uncompressed_bytes,
            "budget_total_bytes": self._total,
        }

    def _walk(
        self,
        *,
        archive_path: Path,
        archive_url: str,
        archive_content_hash: str,
        depth: int,
    ) -> ArchiveObservation:
        """Спуск в один архив: прогноз, затем члены (``full``) или только план (``plan``)."""
        reader = self._reader(archive_path)
        if reader is None:
            reason = (
                "container_reader_unavailable: ContainerPort не инжектирован "
                "(composition root не передал читателя контейнеров)"
                if self._container_for is None
                else "container_reader_unavailable: формат контейнера не распознан читателем"
            )
            self.errors.append(f"{self.product_id}/{archive_url}: archive_unreadable: {reason}")
            return self._record(
                archive_path=archive_path,
                archive_url=archive_url,
                archive_content_hash=archive_content_hash,
                depth=depth,
                status=ArchiveDescentStatus.UNREADABLE,
                forecast={},
                outcome=self._outcome(
                    members_total=0,
                    members_accepted=0,
                    ingested=0,
                    failed=0,
                    skipped=[],
                    bytes_written=0,
                    stopped_reason=SKIP_CONTAINER_READER,
                    cycle_stops=0,
                ),
                reason=reason,
            )

        listing = reader.list_members(str(archive_path))
        plan = plan_members(
            listing,
            max_members=self.budget.max_members,
            max_member_bytes=self.budget.max_member_bytes,
            max_total_uncompressed=self.budget.max_uncompressed_bytes,
            ingest_suffixes=DESCENT_SUFFIXES,
        )
        # Прогноз фиксируется **до** любого чтения байтов члена (решение владельца).
        forecast = archive_forecast(listing, plan)

        if listing.unreadable:
            reason = str(listing.unreadable)
            self.errors.append(
                f"{self.product_id}/{archive_url}: archive_unreadable: {reason}"
            )
            return self._record(
                archive_path=archive_path,
                archive_url=archive_url,
                archive_content_hash=archive_content_hash,
                depth=depth,
                status=ArchiveDescentStatus.UNREADABLE,
                forecast=forecast,
                outcome=self._outcome(
                    members_total=len(listing.members),
                    members_accepted=0,
                    ingested=0,
                    failed=0,
                    skipped=[],
                    bytes_written=0,
                    stopped_reason=reason,
                    cycle_stops=0,
                ),
                reason=reason,
            )

        if self.descent is ArchiveDescent.PLAN:
            # ``plan`` — только планирование: ни одного байта члена не читаем.
            return self._record(
                archive_path=archive_path,
                archive_url=archive_url,
                archive_content_hash=archive_content_hash,
                depth=depth,
                status=ArchiveDescentStatus.PLANNED,
                forecast=forecast,
                outcome=self._outcome(
                    members_total=len(listing.members),
                    members_accepted=len(plan.accepted),
                    ingested=0,
                    failed=0,
                    skipped=[],
                    bytes_written=0,
                    stopped_reason=None,
                    cycle_stops=0,
                ),
                reason=None,
            )

        ingested = failed = cycle_stops = bytes_written = 0
        skipped: list[tuple[str, str]] = []
        stopped_reason: str | None = None

        for member in plan.accepted:
            normalized = normalize_member_path(member.path) or member.path
            suffix = PurePosixPath(normalized).suffix.lower()
            remaining = self._remaining_total()
            if remaining is not None and remaining <= 0:
                stopped_reason = SKIP_BUDGET
                skipped.append((normalized, SKIP_BUDGET))
                self.errors.append(
                    f"{self.product_id}/{archive_url}: archive_budget_exhausted: "
                    f"кумулятивный потолок корня {self.budget.max_total_uncompressed_bytes} Б "
                    f"исчерпан на члене {normalized!r} — спуск остановлен"
                )
                continue
            limits = [value for value in (self.budget.max_member_bytes, remaining) if value]
            member_limit = min(limits) if limits else None
            temp_path = self._temp_path()
            hasher = hashlib.sha256()
            written = 0
            try:
                with temp_path.open("wb") as handle:
                    chunks = reader.open_member(str(archive_path), member.path)
                    stream = bounded_iter(chunks, member_limit) if member_limit else chunks
                    for chunk in stream:
                        handle.write(chunk)
                        hasher.update(chunk)
                        written += len(chunk)
            except MemberTooLarge:
                temp_path.unlink(missing_ok=True)
                if remaining is not None and member_limit == remaining:
                    stopped_reason = SKIP_BUDGET
                    skipped.append((normalized, SKIP_BUDGET))
                    self.errors.append(
                        f"{self.product_id}/{archive_url}: archive_budget_exhausted: член "
                        f"{normalized!r} пробил остаток кумулятивного бюджета корня "
                        f"({remaining} Б) — спуск остановлен"
                    )
                else:
                    skipped.append((normalized, SKIP_STREAM_TOO_LARGE))
                    self.errors.append(
                        f"{self.product_id}/{archive_url}: {SKIP_STREAM_TOO_LARGE}: "
                        f"член {normalized!r} больше потолка {member_limit} Б при чтении"
                    )
                continue
            except Exception as exc:  # noqa: BLE001 — отказ чтения члена изолируется
                temp_path.unlink(missing_ok=True)
                skipped.append((normalized, SKIP_READ_FAILED))
                self.errors.append(
                    f"{self.product_id}/{archive_url}: {SKIP_READ_FAILED}: "
                    f"член {normalized!r}: {type(exc).__name__}: {exc}"
                )
                continue
            self._total += written
            bytes_written += written
            member_hash = hasher.hexdigest()
            url = member_url(archive_url, normalized)

            if suffix in {f".{fmt}" for fmt in SUPPORTED_ARCHIVE_FORMATS}:
                # Вложенный архив: спуск без потолка глубины, конечность — цикл + бюджет.
                if member_hash in self._seen:
                    cycle_stops += 1
                    skipped.append((normalized, SKIP_CYCLE))
                    self.members.append(
                        self._member_row(
                            archive_url=archive_url,
                            archive_content_hash=archive_content_hash,
                            archive_path=archive_path,
                            member_path=normalized,
                            member_url_value=url,
                            suffix=suffix,
                            declared_size=member.size,
                            content_hash=None,
                            doc_id=None,
                            skip_reason=SKIP_CYCLE,
                            depth=depth,
                        )
                    )
                    temp_path.unlink(missing_ok=True)
                    continue
                self._seen.add(member_hash)
                self.members.append(
                    self._member_row(
                        archive_url=archive_url,
                        archive_content_hash=archive_content_hash,
                        archive_path=archive_path,
                        member_path=normalized,
                        member_url_value=url,
                        suffix=suffix,
                        declared_size=member.size,
                        content_hash=None,
                        doc_id=None,
                        skip_reason=SKIP_NESTED,
                        depth=depth,
                    )
                )
                self._walk(
                    archive_path=temp_path,
                    archive_url=url,
                    archive_content_hash=member_hash,
                    depth=depth + 1,
                )
                temp_path.unlink(missing_ok=True)
                continue

            try:
                outcome = self._ingest_member(
                    MemberTask(
                        product_id=self.product_id,
                        root_url=self.root_url,
                        archive_url=archive_url,
                        archive_content_hash=archive_content_hash,
                        archive_path=str(archive_path),
                        member_path=normalized,
                        member_url=url,
                        temp_path=str(temp_path),
                        suffix=suffix,
                        declared_size=member.size,
                        content_hash=member_hash,
                        depth=depth,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — отказ члена не роняет спуск
                outcome = MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            finally:
                temp_path.unlink(missing_ok=True)

            if outcome.doc_id is not None:
                ingested += 1
            else:
                failed += 1
                reason = outcome.skip_reason or SKIP_EXTRACT_FAILED
                skipped.append((normalized, reason))
                detail = f": {outcome.detail}" if outcome.detail else ""
                self.errors.append(
                    f"{self.product_id}/{url}: {reason}{detail}"
                )
            self.members.append(
                self._member_row(
                    archive_url=archive_url,
                    archive_content_hash=archive_content_hash,
                    archive_path=archive_path,
                    member_path=normalized,
                    member_url_value=url,
                    suffix=suffix,
                    declared_size=member.size,
                    content_hash=(
                        outcome.content_hash if outcome.doc_id is not None else None
                    ),
                    doc_id=outcome.doc_id,
                    skip_reason=(
                        None
                        if outcome.doc_id is not None
                        else (outcome.skip_reason or SKIP_EXTRACT_FAILED)
                    ),
                    depth=depth,
                )
            )

        for path, reason in plan.skipped:
            skipped.append((path, reason))

        status = (
            ArchiveDescentStatus.BUDGET_EXHAUSTED
            if stopped_reason == SKIP_BUDGET
            else ArchiveDescentStatus.DESCENDED
        )
        return self._record(
            archive_path=archive_path,
            archive_url=archive_url,
            archive_content_hash=archive_content_hash,
            depth=depth,
            status=status,
            forecast=forecast,
            outcome=self._outcome(
                members_total=len(listing.members),
                members_accepted=len(plan.accepted),
                ingested=ingested,
                failed=failed,
                skipped=skipped,
                bytes_written=bytes_written,
                stopped_reason=stopped_reason,
                cycle_stops=cycle_stops,
            ),
            reason=stopped_reason,
        )

    def _member_row(
        self,
        *,
        archive_url: str,
        archive_content_hash: str,
        archive_path: Path,
        member_path: str,
        member_url_value: str,
        suffix: str,
        declared_size: int,
        content_hash: str | None,
        doc_id: str | None,
        skip_reason: str | None,
        depth: int,
    ) -> CorpusMember:
        """Строка ``corpus_members``: идентичность пары (архив, нормализованный путь)."""
        return CorpusMember(
            archive_content_hash=archive_content_hash,
            member_path=member_path,
            product_id=self.product_id,
            run_id=self.run_id,
            root_url=self.root_url,
            archive_url=archive_url,
            archive_path=str(archive_path),
            member_url=member_url_value,
            member_format=suffix.lstrip(".") or None,
            declared_size=int(declared_size),
            extracted_content_hash=content_hash,
            doc_id=doc_id,
            skip_reason=skip_reason,
            depth=depth,
            created_at=self.created_at,
        )
