"""Retrieval-golden: метрики качества ретривала (hit@K, MRR, recall@K, nDCG@K).

Соответствует §11.2/§11.3 регламента (R9/R10, retrieval-golden) и I20
(cut-off метрики = pipeline ``retrieval_cutoff_K``). Используется для выбора
профиля R0/R1/R2 и как регрессионная сеть при смене эмбеддинга/чанкера.

**nDCG — отдельная функция, не поле отчёта.** :func:`ndcg_at_k` считает метрику по
бинарной релевантности и живёт рядом с остальными; в :class:`RetrievalReport` она не
агрегируется, потому что публикуемая поверхность метрик — явный словарь
:meth:`core.pipeline.experiments.RetrievalExperimentReport.to_manifest_json`, и добавлять
туда поле значило бы менять опубликованный формат отчёта, а не только считать метрику.

**Неразрешённые id эталона (O4).** Голден привязан к конкретным ``chunk_id``. Когда
сборка пересобрана, а эталон — нет, ни один ожидаемый id не резолвится, и ``recall@K``
даёт честный ноль — неотличимый от провала поиска. Поэтому отчёт несёт ещё и число
неразрешённых ожидаемых id (``unresolved_expected_ids`` + примеры): «эталон устарел» —
видимый факт, а не догадка читателя.

Происхождение: метрики (hit@1/3/5, MRR) и eval-паттерн адаптированы из проекта gtm
(`scripts/kb_poc.py`, собственная разработка).
Изменения: обобщено до K из конфигурации (I20), добавлен recall@K, типизация,
нейтральность (§1.3); O4 — счётчик неразрешённых ожидаемых id.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass, field

#: Сколько примеров неразрешённых id публиковать. Голден может быть большим (сотни
#: пар), а оператору нужен адрес, чтобы найти рассинхрон, а не весь список.
UNRESOLVED_EXAMPLE_LIMIT = 5


@dataclass(frozen=True)
class GoldenQuery:
    """Один пример retrieval-golden: запрос → ожидаемые релевантные идентификаторы."""

    query: str
    expected_ids: frozenset[str]
    lang: str = "unknown"


@dataclass
class RetrievalReport:
    k: int
    queries: int
    hit_at_k: float
    recall_at_k: float
    mrr: float
    per_query_rank: list[int | None] = field(default_factory=list)
    #: Уникальные ``expected_ids``, которые **не разрешились** (O4). См.
    #: :func:`evaluate_retrieval`: без ``known_ids`` это «ни разу не пришёл в выдаче»,
    #: с ``known_ids`` — «нет в известном множестве сборки».
    unresolved_expected_ids: int = 0
    #: Сколько всего уникальных ``expected_ids`` в голдене — знаменатель «N из M».
    expected_ids_total: int = 0
    #: До :data:`UNRESOLVED_EXAMPLE_LIMIT` примеров неразрешённых id (отсортированы,
    #: детерминированы). Полный список — в голдене, здесь только адрес проблемы.
    unresolved_expected_examples: tuple[str, ...] = ()
    #: Идентификаторы, реально попавшие в выдачу ранжировщика (не больше
    #: ``queries × k``). Носитель различия «поиск не нашёл» ↔ «id нет в сборке»:
    #: вызывающий объединяет наблюдённое по профилям и сравнивает с ожидаемым.
    observed_ids: frozenset[str] = frozenset()

    def passes(self, min_recall: float) -> bool:
        return self.recall_at_k >= min_recall


# ``ranker(query, k)`` возвращает ранжированный список chunk_id (лучший первым).
Ranker = Callable[[str, int], Sequence[str]]


def evaluate_retrieval(
    queries: Sequence[GoldenQuery],
    ranker: Ranker,
    k: int,
    *,
    known_ids: Collection[str] | None = None,
) -> RetrievalReport:
    """Считает hit@K, recall@K и MRR по retrieval-golden при заданном cut-off K (I20).

    Кроме метрик отчёт публикует **число неразрешённых ожидаемых id** (O4): эталон,
    привязанный к ``chunk_id`` исчезнувшей сборки, даёт ``recall@K = 0``, неотличимый
    от провала поиска. Здесь это различие называется числом и примерами.

    «Не разрешён» = ожидаемый id, которого нет в множестве известных id. Множество —
    ``known_ids`` (передаёт вызывающий, если может спросить сборку/хранилище) **плюс**
    фактически наблюдённые id: то, что ранжировщик вернул, существует по определению.

    :param known_ids: идентификаторы, о которых известно, что они существуют в
        оцениваемой сборке (например, найденные в БД прогона). **Если не передан,
        функция не может отличить «поиск не нашёл» от «такого id в сборке нет»** —
        тогда неразрешённым считается всякий ожидаемый id, не встретившийся ни в одной
        выдаче. Читать это надо буквально: «этот id ни разу не пришёл», а не «этого id
        в сборке нет».
    """
    if k <= 0:
        raise ValueError("k должен быть > 0")
    if not queries:
        return RetrievalReport(k=k, queries=0, hit_at_k=0.0, recall_at_k=0.0, mrr=0.0)

    hit = 0
    recall_sum = 0.0
    rr_sum = 0.0
    ranks: list[int | None] = []
    observed: set[str] = set()

    for item in queries:
        ranked = list(ranker(item.query, k))[:k]
        observed.update(ranked)
        rank = next((i for i, cid in enumerate(ranked, 1) if cid in item.expected_ids), None)
        ranks.append(rank)
        if rank is not None:
            hit += 1
            rr_sum += 1.0 / rank
        if item.expected_ids:
            found = sum(1 for cid in ranked if cid in item.expected_ids)
            recall_sum += found / len(item.expected_ids)

    expected_all = frozenset(cid for item in queries for cid in item.expected_ids)
    # Наблюдённое — доказательство существования: ранжировщик его вернул. Поэтому
    # ``known_ids`` дополняется, а не заменяется: неполное множество известных id не
    # должно превращать найденный чанк в «несуществующий».
    present: set[str] = set(known_ids) if known_ids is not None else set()
    present.update(observed)
    unresolved = sorted(expected_all - present)

    n = len(queries)
    return RetrievalReport(
        k=k,
        queries=n,
        hit_at_k=hit / n,
        recall_at_k=recall_sum / n,
        mrr=rr_sum / n,
        per_query_rank=ranks,
        unresolved_expected_ids=len(unresolved),
        expected_ids_total=len(expected_all),
        unresolved_expected_examples=tuple(unresolved[:UNRESOLVED_EXAMPLE_LIMIT]),
        observed_ids=frozenset(observed),
    )


def ndcg_at_k(expected_ids: Collection[str], ranked: Sequence[str], k: int) -> float:
    """nDCG@K по **бинарной** релевантности: ``DCG/IDCG`` на срезе ``ranked[:k]``.

    ``DCG = Σ rel_i / log2(i+1)`` по местам 1..k (``rel_i = 1``, если id ожидаемый, иначе
    0); ``IDCG`` — тот же DCG идеального порядка, то есть ``min(len(expected), k)``
    релевантных на первых местах. Метрика учитывает **позиции** всех найденных ожидаемых
    id, а не только первого (в отличие от MRR) и не только их число (в отличие от
    recall@K), поэтому чувствительна к тому, насколько высоко стоит каждый из них.

    Знаменатель — идеальный порядок для этого же K, а не для всего эталона: при
    ``k < len(expected_ids)`` nDCG=1.0 достижим (все K первых мест релевантны), иначе
    метрика была бы недостижима по построению.

    :param expected_ids: ожидаемые релевантные идентификаторы (порядок не важен).
    :param ranked: выдача ранжировщика, лучший первым.
    :param k: cut-off (I20); ``<= 0`` — отказ, а не пустая метрика.
    :returns: значение в ``[0, 1]``; ``0.0`` при пустом ``expected_ids`` и при
        ``IDCG == 0`` (недостижимо при ``k > 0`` и непустом эталоне — оставлено как
        честная защита от деления на ноль, а не как «ноль качества»).
    """
    if k <= 0:
        raise ValueError("k должен быть > 0")
    expected = set(expected_ids)
    if not expected:
        return 0.0
    top = list(ranked)[:k]
    dcg = sum(
        1.0 / math.log2(position + 1)
        for position, chunk_id in enumerate(top, 1)
        if chunk_id in expected
    )
    ideal_hits = min(len(expected), k)
    idcg = sum(1.0 / math.log2(position + 1) for position in range(1, ideal_hits + 1))
    if idcg == 0:
        return 0.0
    return dcg / idcg

