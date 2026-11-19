"""coHound — факты о документации предка как **проекция** вердиктов оценщика.

Один вопрос — один прибор. Вопрос «говорит ли документация предка версии V, что параметр
X есть» решала пара приборов с разными правилами: ``hound baseline recon --classifier
phrase`` (BM25-кандидаты + зачёт по фразе, без LLM) и оценщик (тот же корпус, промпт
``evaluator_v6``). Замер 19.11.2026 на reference-корпусе версии 18 (9 763 чанка,
181 параметр каталога):

===================  =========  =========  =========
факт ``phrase``      карта ✅/⚠️  карта ❓   карта ❌
===================  =========  =========  =========
``present`` (37)     **11**     **26**     0
``absent`` (144)     **15**     128        1
===================  =========  =========  =========

κ между ними — **0.0515**. Это не шум одного прибора: это два разных правила под одним
именем, и выбирать между ними приходится по тому, чьё правило методология признаёт.
Поэтому третий классификатор не появляется: **вердикт оценщика — измерение, факт — его
проекция**.

Проекция механическая (никакого второго суждения о том же тексте):

========================  ==========  ==========================================
вердикт ячейки            факт        ``note``
========================  ==========  ==========================================
``✅``                     ``present``  —
``⚠️``                     ``present``  ``llm_partial``
``❌``                     ``absent``   ``llm_absent; as_of=<absence_as_of>``
``❓`` (или иной)          ``unclear``  ``llm_unknown``
ячейки нет в наборе        ``unclear``  ``llm_missing_cell``
========================  ==========  ==========================================

Отказы видны, а не тихи: ``present`` без непустой цитаты, без адреса (I15: цитата без URL
не резолвится) или без хеша текста — ``unclear`` с пометкой, а не «факт без
доказательства». Провенанс наблюдения приходит от вызывающего (``observed_by``:
``llm:{model_id}:{candidate_policy}`` либо ``phrase-1``); подпись человека прибор не ставит
никогда (``review=pending``).

Нейтрален к предметной категории (§1.3): ни одного названия продукта или вендора.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from core.domain.enums import CellState, FactReview, FactStatus, Status
from core.domain.models import BaselineFact, CatalogRow, Cell

__all__ = [
    "FACT_PROJECTION_SCHEMA",
    "NOTE_ABSENT",
    "NOTE_FAILED",
    "NOTE_HASH_MISSING",
    "NOTE_MISSING_CELL",
    "NOTE_PARTIAL",
    "NOTE_QUOTE_MISSING",
    "NOTE_QUOTE_WITHOUT_URL",
    "NOTE_UNKNOWN",
    "facts_from_cells",
]

#: Версия формата проекции. Меняется, когда меняется правило «вердикт → факт»: две
#: проекции по разным правилам — разные наборы фактов, и это должно быть видно из
#: провенанса (``observed_by``), а не выясняться сравнением чисел.
FACT_PROJECTION_SCHEMA = "fact-projection/1"

#: Пометки факта. Строки, а не enum: ``note`` — свободное поле формата CSV (провенанс
#: наблюдения), и заводить перечисление ради четырёх констант значило бы сделать
#: человеческое поле машинным. Префикс ``llm_`` называет источник: вердикт оценщика.
NOTE_PARTIAL = "llm_partial"
NOTE_ABSENT = "llm_absent"
NOTE_UNKNOWN = "llm_unknown"
#: Оценщик не дал вердикта: ячейка ``state=failed`` (status=None). Отдельная пометка, а не
#: ``llm_unknown``: «прибор отказал» и «прибор посмотрел и не нашёл» — разные наблюдения, и
#: сливать их значило бы выдать отказ инструмента за свойство документации (I4: ``failed``
#: не статус). Факт при этом ``unclear`` — «не смотрели» тоже не отсутствие.
NOTE_FAILED = "llm_failed"
NOTE_MISSING_CELL = "llm_missing_cell"
NOTE_QUOTE_MISSING = "llm_quote_missing"
NOTE_QUOTE_WITHOUT_URL = "llm_quote_without_url"
NOTE_HASH_MISSING = "llm_hash_missing"


def _fact(
    *,
    baseline_id: str,
    param_id: str,
    version: str,
    status: FactStatus,
    observed_at: str,
    corpus_build_id: str,
    observed_by: str,
    note: str | None = None,
    url: str | None = None,
    quote: str | None = None,
    content_hash: str | None = None,
) -> BaselineFact:
    """Факт проекции: подпись человека пуста всегда (прибор не подписывает за человека)."""
    return BaselineFact(
        baseline_id=baseline_id,
        param_id=param_id,
        version=version,
        status=status,
        url=url,
        quote=quote,
        content_hash=content_hash,
        observed_at=observed_at,
        corpus_build_id=corpus_build_id,
        review=FactReview.PENDING,
        reviewed_by=None,
        note=note,
        observed_by=observed_by,
    )


def _first_quote(evidence: Any) -> tuple[str | None, str | None, str | None]:
    """Первая запись ``evidence`` с непустой цитатой: ``(quote, url, source_id)``.

    «Первая» — по порядку ячейки, а не «лучшая»: выбор цитаты — дело оценщика и его
    правил, и второй выбор здесь был бы вторым суждением о том же тексте. Записи без
    цитаты пропускаются: пустая цитата не доказательство (I22).
    """
    if not isinstance(evidence, list | tuple):
        return None, None, None
    for item in evidence:
        quote = str(getattr(item, "quote", "") or "").strip()
        if not quote:
            continue
        # ``url`` у свидетельства необязателен (``LLMEvidence`` его не несёт вовсе):
        # адрес добирается у чанка, на который указывает ``source_id``.
        url = str(getattr(item, "url", "") or "").strip() or None
        source_id = str(getattr(item, "source_id", "") or "").strip() or None
        return quote, url, source_id
    return None, None, None


def _present_fact(
    *,
    cell: Cell,
    param_id: str,
    baseline_id: str,
    version: str,
    observed_at: str,
    corpus_build_id: str,
    observed_by: str,
    note: str | None,
    chunk_lookup: Callable[[str], tuple[str | None, str | None]],
) -> BaselineFact:
    """``present`` из вердикта оценщика — либо ``unclear``, если доказательство не адресуемо.

    Три отказа по отдельности, а не один «нет доказательства»: «цитаты нет», «цитаты нет
    адреса» и «нет хеша текста» — разные дефекты и чинятся в разных местах. Молчаливый
    ``present`` без адреса — ровно тот класс, который запрещает I15.
    """
    common = {
        "baseline_id": baseline_id,
        "param_id": param_id,
        "version": version,
        "observed_at": observed_at,
        "corpus_build_id": corpus_build_id,
        "observed_by": observed_by,
    }
    quote, url, source_id = _first_quote(cell.evidence)
    if quote is None:
        return _fact(status=FactStatus.UNCLEAR, note=NOTE_QUOTE_MISSING, **common)
    # Адрес и хеш — у чанка, на который указывает свидетельство: оценщик называет
    # ``source_id`` и цитату, а адрес документа и хеш текста знает корпус.
    chunk_url, chunk_hash = chunk_lookup(source_id) if source_id else (None, None)
    resolved_url = url or chunk_url
    if not resolved_url:
        return _fact(status=FactStatus.UNCLEAR, note=NOTE_QUOTE_WITHOUT_URL, **common)
    if not (chunk_hash or "").strip():
        return _fact(status=FactStatus.UNCLEAR, note=NOTE_HASH_MISSING, **common)
    return _fact(
        status=FactStatus.PRESENT,
        note=note,
        url=resolved_url,
        quote=quote,
        content_hash=chunk_hash,
        **common,
    )


def facts_from_cells(
    *,
    cells: Mapping[str, Cell],
    params: Sequence[CatalogRow],
    baseline_id: str,
    version: str,
    corpus_build_id: str,
    observed_at: str,
    observed_by: str,
    chunk_lookup: Callable[[str], tuple[str | None, str | None]],
) -> list[BaselineFact]:
    """Проекция вердиктов оценщика в факты: вердикт → статус факта, механически.

    :param cells: ``param_id`` → :class:`~core.domain.models.Cell` (оценщик уже ответил;
        проекция вопрос не переспрашивает).
    :param params: каталог профиля **в его порядке**: факты идут в том же порядке, чтобы
        повторная проекция тех же вердиктов давала тот же файл (R7).
    :param observed_at: момент проекции. Не дата вердикта: дата оценки — провенанс
        измерения, а наблюдение факта сделано сейчас, по уже полученным вердиктам.
    :param observed_by: провенанс наблюдения — каким прибором и с каким правилом отбора
        кандидатов получено (``llm:{model_id}:{candidate_policy}`` либо ``phrase-1``).
        В ``baseline_facts_digest`` не входит (см. ``BaselineFact.observed_by``).
    :param chunk_lookup: ``chunk_id`` → ``(url, content_hash)``; ``(None, None)`` — чанка
        нет. Хеш берётся у **текста чанка**: цитата привязана к тексту, и это
        единственное, что делает её проверяемой.
    :returns: факты в порядке ``params``; ``review=pending`` у всех.

    ``present`` требует цитату, адрес и хеш (I22/I15). Любой из трёх отсутствует — факт
    становится ``unclear`` с названной причиной: оценщик сказал «есть», но доказательство
    не резолвится, и наследовать его нельзя. Это отказ **видимый**: причина остаётся в
    ``note``, а не превращается в ``absent`` (отсутствие доказательства — не отсутствие
    функции, I12).

    Ячейки нет (оценщик этот параметр не оценивал) — ``unclear`` с ``llm_missing_cell``:
    «вердикта нет» — не «документация молчит».
    """
    facts: list[BaselineFact] = []
    for param in params:
        param_id = str(param.param_id)
        cell = cells.get(param_id)
        if cell is None:
            facts.append(
                _fact(
                    baseline_id=baseline_id,
                    param_id=param_id,
                    version=version,
                    status=FactStatus.UNCLEAR,
                    observed_at=observed_at,
                    corpus_build_id=corpus_build_id,
                    observed_by=observed_by,
                    note=NOTE_MISSING_CELL,
                )
            )
            continue
        status = cell.status
        if status in {Status.OK, Status.PARTIAL}:
            facts.append(
                _present_fact(
                    cell=cell,
                    param_id=param_id,
                    baseline_id=baseline_id,
                    version=version,
                    observed_at=observed_at,
                    corpus_build_id=corpus_build_id,
                    observed_by=observed_by,
                    note=NOTE_PARTIAL if status is Status.PARTIAL else None,
                    chunk_lookup=chunk_lookup,
                )
            )
            continue
        if status is Status.ABSENT:
            # Дата отсутствия (I2d) публикуется в пометке: ``❌`` — утверждение о
            # датированном событии, и факт без даты читался бы как «нет и сегодня».
            as_of = str(cell.absence_as_of or "").strip() or "unknown"
            facts.append(
                _fact(
                    baseline_id=baseline_id,
                    param_id=param_id,
                    version=version,
                    status=FactStatus.ABSENT,
                    observed_at=observed_at,
                    corpus_build_id=corpus_build_id,
                    observed_by=observed_by,
                    note=f"{NOTE_ABSENT}; as_of={as_of}",
                )
            )
            continue
        # ``state=failed`` (status=None) — отказ прибора, а не «не нашли»: у ячейки нет
        # вердикта вовсе (I4). Слить его с ``llm_unknown`` значило бы опубликовать отказ
        # инструмента как наблюдение о документации; причина остаётся в ``note``.
        note = NOTE_FAILED if cell.state is CellState.FAILED else NOTE_UNKNOWN
        facts.append(
            _fact(
                baseline_id=baseline_id,
                param_id=param_id,
                version=version,
                status=FactStatus.UNCLEAR,
                observed_at=observed_at,
                corpus_build_id=corpus_build_id,
                observed_by=observed_by,
                note=note,
            )
        )
    return facts
