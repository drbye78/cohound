"""coHound — механический эшелон валидации цитат Ш6 (T9.4, §7.1/§10).

Эшелон §10 превращает сырой выход оценщика (``LLMCellOutput`` с ``source_id``+``quote``)
в типизированный ``CellEvidence`` **только если** цитата механически подтверждается:

1. ``source_id`` существует среди переданных чанков (I10/I15);
2. цитата — подстрока **чистого** ``text`` чанка с нормализацией пробелов (strict);
3. из подтверждённых цитат строится ``CellEvidence``: детерминированный ``evidence_id``
   (S8), ``content_hash`` чанка (**I19**), ``source_url`` через ``resolver`` (I10/I15);
4. аномалии ревью (§10.4): ✅ при retrieval-скоре ниже порога и ✅ на единственной
   цитате — **заметки**, не сбои. Автопонижение статуса запрещено (§2.2: равновесные
   трактовки → меньший статус решает **ревью**, а не эшелон).

**Нормализация цитаты (точный контракт).** Сравнение выполняется не побайтово, а после
схлопывания любых пробельных последовательностей (``\\s`` в Unicode, включая переводы
строк) в один пробел и обрезки краёв — **одинаково** для текста чанка и для цитаты.
Это компенсирует пере-обёртку строк LLM, но не допускает перефразирования: регистр и
порядок символов обязаны совпадать. ``context_prefix`` в сравнение **не входит** (§4.4).
Хранится исходная цитата модели (не нормализованная) — аудит видит то, что вернула LLM.

**Провал vs кандидат.** При ``strict=True`` любой провал даёт
``failure_kind=validation_failed``; при ``strict=False`` (preliminary) провал возвращается
как ``ok=False`` без ``failure_kind`` и с пометкой «candidate» — решение о
preliminary-ячейке принимает вызывающий (I13).

Модуль категорийно нейтрален (§1.3), без сети, без новых зависимостей.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from core.domain import (
    CellEvidence,
    EvidenceType,
    FailureKind,
    LLMCellOutput,
    SourceKind,
    SourceScope,
    Status,
    derived_evidence_id,
)
from core.domain.anchors import SourceResolver
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk

__all__ = [
    "DEFAULT_SOURCE_KIND",
    "ChunkLike",
    "ValidationFailureKind",
    "ValidationResult",
    "normalize_quote_text",
    "validate_output",
]

#: Любой допустимый тип чанка на входе валидации (у обоих есть ``chunk_id``/``text``).
ChunkLike = Chunk | RetrievedChunk

#: Источник корпусного evidence по умолчанию: чанк прошёл corpus → hash → index (I19).
#: Порт ретривала не несёт ``source_kind`` (это свойство происхождения документа, не
#: чанка), поэтому эшелон консервативно маркирует прямую корпусную цитату как
#: ``documentation``. Web/distribution-происхождение уточняется на уровне корпуса (§5).
DEFAULT_SOURCE_KIND = SourceKind.DOCUMENTATION

#: Регулярка нормализации пробелов (§10 шаг 2): любые пробельные Unicode-последовательности.
_WS_RE = re.compile(r"\s+")


class ValidationFailureKind(StrEnum):
    """Типы сбоев эшелона (§7.1). Значение совпадает с доменным ``FailureKind``.

    Эшелон валидации использует единственный вид — ``validation_failed``; константа
    вынесена, чтобы вызывающий код не хардкодил строку и чтобы она сверялась с
    источником истины (``core.domain.enums.FailureKind``).
    """

    VALIDATION_FAILED = FailureKind.VALIDATION_FAILED.value


# Проверка синхронности с доменом (источник истины — ``FailureKind``): рассогласование —
# ошибка сборки, а не данных.
assert ValidationFailureKind.VALIDATION_FAILED.value == FailureKind.VALIDATION_FAILED.value


@dataclass
class ValidationResult:
    """Результат механической валидации одной ячейки (§10, 02 §C.2).

    ``ok`` — прошла ли цитатная валидация целиком. ``failure_kind`` — вид сбоя
    (``validation_failed``) в strict-режиме, иначе ``None`` (preliminary-кандидат).
    ``notes`` — аномалии/пояснения (§10.4), не влияющие на ``ok``.
    ``resolved_evidence`` — типизированные доказательства из **подтверждённых** цитат
    (при частичном провале содержит валидные элементы — полезно preliminary-ветке).
    """

    ok: bool
    failure_kind: str | None
    notes: list[str] = field(default_factory=list)
    resolved_evidence: list[CellEvidence] = field(default_factory=list)


def normalize_quote_text(text: str) -> str:
    """Нормализует пробелы для сверки цитаты (§10 шаг 2).

    Любая пробельная последовательность → один пробел, края обрезаются. Применяется
    **одинаково** к тексту чанка и к цитате — см. докстринг модуля.
    """
    return _WS_RE.sub(" ", text).strip()


def _chunk_id(chunk: ChunkLike) -> str:
    """``chunk_id`` чанка (общий атрибут ``Chunk`` и ``RetrievedChunk``)."""
    return chunk.chunk_id


def _chunk_text(chunk: ChunkLike) -> str:
    """Чистый ``text`` чанка (без ``context_prefix``) — база цитатной сверки (§4.4)."""
    return chunk.text


def _chunk_hash(chunk: ChunkLike) -> str | None:
    """``content_hash`` чанка (I19); пустое значение трактуется как отсутствие."""
    return getattr(chunk, "content_hash", None) or None


def _chunk_build_id(chunk: ChunkLike) -> str | None:
    """``build_id`` индексной сборки (есть только у ``RetrievedChunk``, S5)."""
    return getattr(chunk, "build_id", None) or None


def _partition_build_id(
    chunk_build_id: str | None, composition_id: str | None
) -> str | None:
    """``partition_build_id`` evidence — **только** физическая партиция (09 §8.1).

    ``RetrievedChunk.build_id`` — партиция (``pb_``), но у legacy-строки
    ``corpus_chunks.build_id`` может лежать старый ``ib_``/``cb_`` (сборка/композиция).
    Выдавать композицию за партицию нельзя: evidence назвал бы не тот артефакт, а
    backfill/новая колонка — вне этой правки. Поэтому:

    * пусто → ``None`` (источник не из персистентного корпуса — не выдумываем);
    * значение равно ``composition_id`` → ``None``: партиция **никогда** не равна
      композиции (``pb_`` и ``cb_`` — разные формулы, 09 §8.1), значит это не партиция;
    * иначе — значение как есть.

    Сравнение с композицией, а не разбор префикса: префикс не несёт семантики
    (legacy ``ib_`` был и сборкой, и партицией), а равенство id — надёжный признак.
    """
    if not chunk_build_id:
        return None
    if composition_id is not None and chunk_build_id == composition_id:
        return None
    return chunk_build_id


def _chunk_score(chunk: ChunkLike) -> float | None:
    """Retrieval-скор чанка (есть только у ``RetrievedChunk``); иначе ``None``."""
    score = getattr(chunk, "score", None)
    return float(score) if score is not None else None


def _note(notes: list[str], message: str) -> None:
    """Добавляет заметку (идемпотентно — без дублей, детерминизм выходного отчёта)."""
    if message not in notes:
        notes.append(message)


def _resolve_url(
    source_id: str,
    chunk: ChunkLike,
    resolver: SourceResolver | None,
) -> tuple[str | None, str | None, str | None]:
    """Резолвит ``source_id`` в ``(source_url, build_id, ошибка)`` (I10/I15/I19).

    Без ``resolver`` URL не резолвится (``None``) — это не ошибка на уровне эшелона:
    резолв опционален и включается composition root'ом. При наличии ``resolver``:

    * отсутствие ``source_id`` в резолвере → ошибка (I10/I15);
    * расхождение ``content_hash`` резолва и чанка (в т.ч. пустой) → ошибка (I19);
      это зеркало :func:`core.domain.anchors.verify_evidence_hash` в порядке, когда
      URL ещё не построен.
    """
    if resolver is None:
        return None, _chunk_build_id(chunk), None

    resolved = resolver.resolve(source_id)
    if resolved is None:
        return None, None, f"I10/I15: source_id {source_id!r} не резолвится"
    chunk_hash = _chunk_hash(chunk)
    if chunk_hash and resolved.content_hash != chunk_hash:
        return None, None, (
            f"I19: content_hash чанка {chunk_hash[:12]}… расходится с резолвом"
        )
    build_id = resolved.build_id or _chunk_build_id(chunk)
    return resolved.source_url, build_id, None


def _build_evidence(
    *,
    source_id: str,
    quote: str,
    chunk: ChunkLike,
    seq: int,
    product_id: str,
    param_id: str,
    run_id: str,
    source_url: str | None,
    build_id: str | None,
    composition_id: str | None = None,
) -> CellEvidence:
    """Собирает ``CellEvidence`` из подтверждённой цитаты (S8/I19).

    ``evidence_id`` детерминирован (:func:`core.domain.derived_evidence_id`): та же
    ячейка при том же наборе цитат даёт те же id (идемпотентность resume/повторного Ш6).
    ``content_hash`` берётся из чанка — I19: внешний контент стал evidence только после
    ``corpus_raw → hash → index``. ``source_kind`` — :data:`DEFAULT_SOURCE_KIND`.

    ``partition_build_id`` — физическая партиция чанка-цитаты (``pb_``), **не**
    композиция: ``composition_id`` (из ``RetrievalResult.build_id``) нужен только чтобы
    отличить legacy-строку с id сборки/композиции от настоящей партиции и в этом случае
    оставить ``NULL`` (см. :func:`_partition_build_id`).
    """
    return CellEvidence(
        evidence_id=derived_evidence_id(
            run_id,
            product_id,
            param_id,
            source_id,
            DEFAULT_SOURCE_KIND.value,
            seq,
        ),
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        source_kind=DEFAULT_SOURCE_KIND,
        evidence_type=EvidenceType.DIRECT,
        source_id=source_id,
        source_url=source_url,
        citation_quote=quote,
        content_hash=_chunk_hash(chunk),
        index_build_id=build_id,
        # 09 §8.1: физическая партиция чанка-цитаты (``RetrievedChunk.build_id``).
        # Композиция/id сборки в этой колонке — не партиция, поэтому ``NULL``.
        partition_build_id=_partition_build_id(
            _chunk_build_id(chunk), composition_id
        ),
    )


def validate_output(
    *,
    output: LLMCellOutput,
    chunks: Mapping[str, ChunkLike],
    product_id: str,
    param_id: str,
    run_id: str,
    resolver: SourceResolver | None = None,
    strict: bool = True,
    min_retrieval_score: float | None = None,
    composition_id: str | None = None,
) -> ValidationResult:
    """Механически валидирует выход оценщика и строит evidence (§10, 02 §C.2).

    :param output: разобранный выход оценщика (``status``/``justification``/``evidence``).
    :param chunks: чанки, переданные оценщику, по ``chunk_id`` (I17: только продукт-скоуп).
    :param product_id / param_id / run_id: ключ ячейки (для ``evidence_id``/``CellEvidence``).
    :param resolver: детерминированный резолвер ``source_id`` → URL (I10/I15); опционален.
    :param strict: strict-эшелон (``failure_kind`` при провале) vs preliminary-кандидат.
    :param min_retrieval_score: порог retrieval-скора для аномалии §10.4 (заметка, не сбой).
    :param composition_id: ``RetrievalResult.build_id`` — композиция прогона (09 §8.1).
        Нужна, чтобы не записать композицию в ``partition_build_id`` legacy-строки:
        совпадение с ``chunk.build_id`` означает «это не партиция» → ``NULL``.

    Возвращает :class:`ValidationResult`. **Никогда не бросает** на невалидных данных:
    провал — это значение (``ok=False``), а не исключение — конвейер продолжает работу
    и принимает решение о retry/failed/preliminary на уровне ``evaluate.py``.
    """
    notes: list[str] = []
    resolved_evidence: list[CellEvidence] = []
    hard_fail = False

    # I7: N/A запрещён на выходе LLM (``LLMCellOutput`` это уже отвергает). Defensive:
    # model_construct может обойти валидатор — не даём N/A молча стать ячейкой.
    if output.status is Status.NOT_APPLICABLE:
        return ValidationResult(
            ok=False,
            failure_kind=ValidationFailureKind.VALIDATION_FAILED.value,
            notes=["I7: N/A недопустим на выходе оценщика"],
            resolved_evidence=[],
        )

    # I1 (defensive): ✅/⚠️ без evidence недопустимы даже при обходе модели.
    if output.status in {Status.OK, Status.PARTIAL} and not output.evidence:
        _note(notes, "I1: ✅/⚠️ требуют evidence ≥ 1")

    # (I22 defensive) Прямая претензия обязана нести непустую цитату.
    if not output.evidence and output.status in {Status.OK, Status.PARTIAL}:
        hard_fail = True

    cited_chunks: list[ChunkLike] = []
    for seq, ev in enumerate(output.evidence):
        source_id = ev.source_id
        # (1) source_id существует среди переданных чанков (I10/I15).
        chunk = chunks.get(source_id)
        if chunk is None:
            _note(notes, f"I10/I15: source_id {source_id!r} отсутствует среди чанков")
            hard_fail = True
            continue

        # (2) цитата — подстрока чистого текста чанка (strict, нормализация пробелов).
        quote = ev.quote
        if not normalize_quote_text(quote):
            _note(notes, f"I22: пустая цитата для source_id {source_id!r}")
            hard_fail = True
            continue
        if normalize_quote_text(quote) not in normalize_quote_text(_chunk_text(chunk)):
            _note(notes, f"I22: цитата не является подстрокой текста {source_id!r}")
            hard_fail = True
            continue

        # I19: корпусный чанк обязан нести content_hash, иначе evidence не строится.
        if _chunk_hash(chunk) is None:
            _note(notes, f"I19: чанк {source_id!r} без content_hash")
            hard_fail = True
            continue

        # (1b) резолв URL (I10/I15) + сверка hash (I19) — только при заданном resolver.
        source_url, build_id, resolve_error = _resolve_url(source_id, chunk, resolver)
        if resolve_error is not None:
            _note(notes, resolve_error)
            hard_fail = True
            continue

        resolved_evidence.append(
            _build_evidence(
                source_id=source_id,
                quote=quote,
                chunk=chunk,
                seq=seq,
                product_id=product_id,
                param_id=param_id,
                run_id=run_id,
                source_url=source_url,
                build_id=build_id,
                composition_id=composition_id,
            )
        )
        cited_chunks.append(chunk)

    # (4) §10.4 аномалии: не сбои, только заметки для ревью.
    if output.status is Status.OK:
        if len(output.evidence) == 1:
            _note(
                notes,
                "§2.2/§10.4: ✅ опирается на единственную цитату — на ревью "
                "(автопонижение статуса не выполняется)",
            )
        if min_retrieval_score is not None:
            scores = [s for c in cited_chunks if (s := _chunk_score(c)) is not None]
            if scores and max(scores) < min_retrieval_score:
                _note(
                    notes,
                    "§10.4: аномалия — ✅ при retrieval-скоре "
                    f"{max(scores):.4g} ниже порога {min_retrieval_score:.4g}",
                )

    if hard_fail and strict:
        return ValidationResult(
            ok=False,
            failure_kind=ValidationFailureKind.VALIDATION_FAILED.value,
            notes=notes,
            resolved_evidence=resolved_evidence,
        )
    if hard_fail:
        # preliminary: провал не несёт failure_kind — это кандидат на ревью (I13).
        _note(notes, "candidate: preliminary-кандидат (провал валидации, не failed)")
        return ValidationResult(
            ok=False,
            failure_kind=None,
            notes=notes,
            resolved_evidence=resolved_evidence,
        )

    return ValidationResult(
        ok=True,
        failure_kind=None,
        notes=notes,
        resolved_evidence=resolved_evidence,
    )
