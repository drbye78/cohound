"""coHound — кандидаты recon для оценщика: объединение словесного и фразового наборов.

Разведка фактов о документации предка отвечает на вопрос «описан ли параметр P в версии V»,
и до этого модуля ответ давали два прибора с разными правилами: фразовый гейт
(``derive_facts``) и оценщик карты. Замер 19.11.2026 на reference-корпусе версии 18
(9 763 чанка, 181 параметр) показал, что расхождение — **не** разница правил оценки:
из 53 параметров, где **какой-то** прибор нашёл цитату (37 фразовых фактов + 26 ячеек
карты), наборы кандидатов покрывали:

=========================  =========
набор кандидатов            покрыл
=========================  =========
словесный (``terms``)        49 / 53  (0.925)
фразовый (``phrases``)       28 / 53  (0.528)
объединение                  **53 / 53** (1.000)
=========================  =========

Уникально у слов — 25 параметров, уникально у фраз — 4: ни один набор не является
надмножеством другого. Отдельно: из 26 ячеек, где фразовый гейт говорил ``present``, а
карта — ``❓``, **25** вообще не попали в ретривал карты ни при ``cutoff_k=10``, ни при
``20``. То есть ``❓`` карты — это не «оценщик не признал», а «ретривал не донёс».

Поэтому кандидаты recon — объединение обоих наборов, а решение о наличии принимает
оценщик (тот же промпт и та же схема, что у карты: второй формулы оценки нет).

**Среза до ``terms_top_k`` нет, и это тоже замер.** Объединение — и есть набор кандидатов;
срез до 10 терял 25 из 53 параметров, которые уже были найдены каким-то прибором. Цена
среза — потерянные утверждения, цена отсутствия среза — лишние кандидаты в промпте
(``terms_top_k + phrases_top_k`` сверху, дедуп снижает).

Нейтрален к предметной категории (§1.3): ни одного названия продукта или вендора.
"""

from __future__ import annotations

from core.domain.models import CatalogRow
from core.pipeline.prompt import render_queries
from core.pipeline.reference_corpus import parameter_terms
from core.pipeline.retrieve import MAX_QUERIES
from core.ports.lexical_index import LexicalSearcherPort

__all__ = [
    "CANDIDATE_POLICY",
    "PHRASES_TOP_K",
    "TERMS_TOP_K",
    "phrase_candidates",
    "recon_candidates",
    "term_candidates",
]

#: Имя правила отбора кандидатов. Входит в ``observed_by`` факта
#: (``llm:{model_id}:{CANDIDATE_POLICY}``): две разведки по разным наборам кандидатов —
#: разные наблюдения, даже если модель одна.
CANDIDATE_POLICY = "terms+phrases"

#: Сколько кандидатов берётся на **один** запрос фразового набора (как ``cutoff_k`` карты).
PHRASES_TOP_K = 10
#: Сколько кандидатов берётся по словесному запросу (как ``RECON_TOP_K`` фразового гейта).
TERMS_TOP_K = 20


def term_candidates(
    param: CatalogRow,
    *,
    index: LexicalSearcherPort,
    product_id: str,
    top_k: int = TERMS_TOP_K,
    query_lang: str = "all",
) -> list[tuple[str, float]]:
    """**Словесный** набор кандидатов: ``parameter_terms`` одним запросом.

    Это набор recall: BM25 всё равно токенизирует, поэтому слова находят страницу, даже
    если фразы в тексте нет дословно. Пустые термины дают пустой набор, а не запрос из
    пустой строки: ``search_scores("")`` вернул бы произвольные чанки.
    """
    terms = parameter_terms(param, query_lang=query_lang)
    if not terms:
        return []
    return index.search_scores(" ".join(terms), product_ids=[product_id], top_k=top_k)


def phrase_candidates(
    param: CatalogRow,
    *,
    index: LexicalSearcherPort,
    product_id: str,
    top_k: int = PHRASES_TOP_K,
    query_lang: str = "all",
) -> list[tuple[str, float]]:
    """**Фразовый** набор: запросы Ш6 (``render_queries``), слияние по max, затем срез.

    Те же запросы, что у ячейки Ш6 и у карты (``reference_map._retrieve``), — иначе
    расхождение приборов мерило бы разницу формулировок, а не разницу путей. Срез
    делается **после** слияния запросов: три запроса одного параметра дают пул, из
    которого в кандидаты идёт ``top_k``.
    """
    merged: dict[str, float] = {}
    for query in render_queries(param, max_queries=MAX_QUERIES, query_lang=query_lang):
        for chunk_id, score in index.search_scores(
            query, product_ids=[product_id], top_k=top_k
        ):
            previous = merged.get(chunk_id)
            if previous is None or score > previous:
                merged[chunk_id] = score
    ordered = sorted(merged.items(), key=lambda item: (-item[1], item[0]))
    return ordered[:top_k]


def recon_candidates(
    param: CatalogRow,
    *,
    index: LexicalSearcherPort,
    product_id: str,
    terms_top_k: int = TERMS_TOP_K,
    phrases_top_k: int = PHRASES_TOP_K,
    query_lang: str = "all",
) -> list[tuple[str, float]]:
    """Кандидаты параметра: объединение словесного и фразового наборов, дедуп по чанку.

    Два набора отвечают на разные вопросы, и потому ни один не заменяет другой:

    * **словесный** (:func:`term_candidates`) — recall;
    * **фразовый** (:func:`phrase_candidates`) — точность формулировки.

    Объединение — потому что замер 19.11.2026 дал 49/53 и 28/53 по отдельности против
    53/53 вместе: каждый набор уникален на своей части параметров (25 против 4).

    :param index: собранный индекс корпуса (порт ``LexicalSearcherPort``); движок выбирает
        composition root — ``pipeline`` не импортирует адаптер.
    :param terms_top_k / phrases_top_k: глубина каждого набора. Срез применяется к
        **каждому набору отдельно**; к объединению срез не применяется (см. докстринг
        модуля).
    :param query_lang: объявленный язык терминов (``ru``|``en``|``all``) — доходит до
        обоих наборов: ``parameter_terms`` и ``render_queries`` резолвят его сами.
    :returns: ``(chunk_id, score)`` по убыванию счёта, tie-break по ``chunk_id``. Порядок
        детерминирован (R7): повторный вызов на тех же данных даёт тот же список — он же
        порядок входа оценщика, и от него зависит, какие чанки попадут в промпт.
    """
    best: dict[str, float] = {}
    hits = term_candidates(
        param, index=index, product_id=product_id, top_k=terms_top_k, query_lang=query_lang
    ) + phrase_candidates(
        param, index=index, product_id=product_id, top_k=phrases_top_k, query_lang=query_lang
    )
    for chunk_id, score in hits:
        previous = best.get(chunk_id)
        if previous is None or score > previous:
            best[chunk_id] = score
    return sorted(best.items(), key=lambda item: (-item[1], item[0]))
