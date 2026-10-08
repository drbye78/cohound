"""coHound — эксперимент ретривала R0/R1/R2 (T8.6, §B.3, R9/R10, I20).

Прогон фиксированного retrieval-golden по профилям R0 (только BM25), R1 (только dense)
и R2 (RRF-гибрид), метрика — ``recall@K`` с cut-off ``retrieval_cutoff_K`` (I20: один и тот
же K для golden и pipeline). Профиль выбирается **измерением** (A3), а не предвидением:
результат фиксируется в манифесте (``retrieval_params_json``, ``profile`` + ``k`` + ``rrf_k``).

Метрики не переопределяются здесь — переиспользуется :func:`core.util.retrieval_eval.evaluate_retrieval`.
Модуль категорийно нейтрален (§1.3) и детерминирован; единственный I/O — загрузчик golden.

Dense-плечо (R1/R2) требует вектор запроса. Он получается через инъецированный
``embed: Callable[[str], Sequence[float]]``. Если ``embed`` (или нужный порт) не передан,
профиль **пропускается** с записанной причиной (``ExperimentReport.skipped``) — эксперимент
не падает и не «выдумывает» вектор.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Sequence

from core.pipeline.retrieve import DEFAULT_RRF_K, RetrievalProfile, retrieve
from core.ports.lexical_index import LexicalIndexPort
from core.ports.vector_index import VectorIndexPort
from core.util.retrieval_eval import GoldenQuery, RetrievalReport, evaluate_retrieval

__all__ = [
    "DEFAULT_PROFILES",
    "ProfileExperiment",
    "ExperimentReport",
    "load_retrieval_golden",
    "run_retrieval_experiment",
]


#: Профили эксперимента по умолчанию: baseline R0, dense R1, гибрид R2.
DEFAULT_PROFILES: tuple[RetrievalProfile, ...] = (
    RetrievalProfile.R0,
    RetrievalProfile.R1,
    RetrievalProfile.R2,
)

#: Инъекция эмбеддинга одного запроса (dense-плечо R1/R2). Без неё R1/R2 пропускаются.
EmbedFn = Callable[[str], Sequence[float]]

#: Приоритет профиля при равных recall@K и MRR (A3: предпочтителен лучший гибрид R2 > R1 > R0).
_PROFILE_PREFERENCE: dict[str, int] = {
    RetrievalProfile.R2.value: 2,
    RetrievalProfile.R1.value: 1,
    RetrievalProfile.R0.value: 0,
}


@dataclass(frozen=True)
class ProfileExperiment:
    """Результат одного профиля: отчёт метрик и применённый cut-off K (I20)."""

    profile: str
    report: RetrievalReport
    cutoff_k: int


@dataclass
class ExperimentReport:
    """Итог эксперимента R0/R1/R2: метрики по профилям и выбранный измерением профиль (R9/R10).

    :param k: cut-off ``retrieval_cutoff_K``, единый для golden и pipeline (I20).
    :param by_profile: метрики по фактически оценённым профилям (``profile -> RetrievalReport``).
    :param selected_profile: выбранный профиль (измерением, A3).
    :param selection_basis: человекочитаемое обоснование выбора (для манифеста/логов).
    :param per_query: ранг первого релевантного чанка по каждому профилю (диагностика).
    :param rrf_k: RRF-константа, использованная в эксперименте (часть ``retrieval_params_json``).
    :param skipped: пропущенные профили с причиной (например, нет ``embed`` для dense).
    """

    k: int
    by_profile: dict[str, RetrievalReport]
    selected_profile: str
    selection_basis: str
    per_query: dict[str, list[int | None]] = field(default_factory=dict)
    rrf_k: int = DEFAULT_RRF_K
    skipped: dict[str, str] = field(default_factory=dict)

    def to_manifest_json(self) -> str:
        """JSON для ``retrieval_params_json``/манифеста: профиль, K, ``rrf_k`` и метрики.

        Детерминирован (``sort_keys``); не включает ``per_query`` (диагностика, не параметр).
        """
        payload = {
            "selected_profile": self.selected_profile,
            "k": self.k,
            "rrf_k": self.rrf_k,
            "selection_basis": self.selection_basis,
            "metrics": {
                name: {
                    "queries": report.queries,
                    "hit_at_k": report.hit_at_k,
                    "recall_at_k": report.recall_at_k,
                    "mrr": report.mrr,
                }
                for name, report in self.by_profile.items()
            },
            "skipped": dict(self.skipped),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _coerce_profile(profile: RetrievalProfile | str) -> RetrievalProfile:
    """Нормализует профиль к :class:`RetrievalProfile` (принимает ``"R0"``/``"r0"``)."""
    if isinstance(profile, RetrievalProfile):
        return profile
    try:
        return RetrievalProfile(str(profile).upper())
    except ValueError as exc:
        raise ValueError(
            f"неизвестный профиль ретривала: {profile!r} (ожидается R0|R1|R2)"
        ) from exc


def load_retrieval_golden(path: str | Path) -> list[GoldenQuery]:
    """Загружает нейтральный retrieval-golden из JSON (форма: список пар запрос→ожидаемые id).

    Ожидаемая форма::

        [{"query": "...", "expected_ids": ["c1"], "lang": "ru"}]

    :raises ValueError: файл не JSON-список, элемент не объект, ``query`` не непустая строка,
        ``expected_ids`` не непустой список непустых строк, ``lang`` не строка.
    """
    raw = Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"retrieval-golden: некорректный JSON ({path}): {exc}") from exc
    if not isinstance(data, list):
        raise ValueError(f"retrieval-golden: ожидается JSON-список, получено {type(data).__name__}")

    golden: list[GoldenQuery] = []
    for index, item in enumerate(data):
        where = f"retrieval-golden[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{where}: ожидается объект, получено {type(item).__name__}")
        query = item.get("query")
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"{where}.query: ожидается непустая строка")
        expected = item.get("expected_ids")
        if not isinstance(expected, list) or not expected:
            raise ValueError(f"{where}.expected_ids: ожидается непустой список идентификаторов")
        ids: list[str] = []
        for cid in expected:
            if not isinstance(cid, str) or not cid.strip():
                raise ValueError(f"{where}.expected_ids: идентификаторы должны быть непустыми строками")
            ids.append(cid)
        lang = item.get("lang", "unknown")
        if not isinstance(lang, str):
            raise ValueError(f"{where}.lang: ожидается строка")
        golden.append(GoldenQuery(query=query, expected_ids=frozenset(ids), lang=lang))
    return golden


def _missing_dependencies(
    profile: RetrievalProfile,
    *,
    lexical_index: LexicalIndexPort | None,
    vector_index: VectorIndexPort | None,
    embed: EmbedFn | None,
) -> str | None:
    """Возвращает причину пропуска профиля или ``None``, если зависимостей достаточно."""
    if profile in (RetrievalProfile.R0, RetrievalProfile.R2) and lexical_index is None:
        return "нет lexical_index"
    if profile in (RetrievalProfile.R1, RetrievalProfile.R2):
        if vector_index is None:
            return "нет vector_index"
        if embed is None:
            return "нет embed (dense-вектор запроса недоступен)"
    return None


def _run_profile(
    profile: RetrievalProfile,
    golden: Sequence[GoldenQuery],
    *,
    product_ids: Sequence[str],
    cutoff_k: int,
    lexical_index: LexicalIndexPort | None,
    vector_index: VectorIndexPort | None,
    embed: EmbedFn | None,
    rrf_k: int,
) -> ProfileExperiment:
    """Прогоняет один профиль по golden и возвращает :class:`ProfileExperiment` (I20)."""

    def ranker(query: str, k: int) -> Sequence[str]:
        query_vector: Sequence[float] | None = None
        if profile in (RetrievalProfile.R1, RetrievalProfile.R2):
            assert embed is not None  # обеспечено проверкой зависимостей выше
            query_vector = embed(query)
        result = retrieve(
            query_text=query,
            query_vector=query_vector,
            product_ids=product_ids,
            profile=profile,
            cutoff_k=cutoff_k,
            lexical_index=lexical_index,
            vector_index=vector_index,
            rrf_k=rrf_k,
        )
        return [chunk.chunk_id for chunk in result.chunks]

    report = evaluate_retrieval(golden, ranker, k=cutoff_k)
    return ProfileExperiment(profile=profile.value, report=report, cutoff_k=cutoff_k)


def _select_profile(
    experiments: Sequence[ProfileExperiment],
    *,
    min_delta: float,
) -> tuple[str, str]:
    """Детерминированно выбирает профиль и возвращает ``(profile, selection_basis)`` (A3).

    Правило: наивысший ``recall_at_k``; при равенстве — больший ``mrr``; при полном
    равенстве — приоритет R2 > R1 > R0 (лучший гибрид). Если лучший профиль превосходит
    baseline R0 по ``recall@K`` менее чем на ``min_delta``, сохраняется консервативный R0.
    """
    by_name = {experiment.profile: experiment for experiment in experiments}
    best = max(
        experiments,
        key=lambda experiment: (
            experiment.report.recall_at_k,
            experiment.report.mrr,
            _PROFILE_PREFERENCE.get(experiment.profile, -1),
        ),
    )
    baseline = by_name.get(RetrievalProfile.R0.value)

    if baseline is None or best.profile == baseline.profile:
        basis = (
            f"выбран профиль {best.profile}: лучший по recall@K="
            f"{best.report.recall_at_k:.4f}/MRR={best.report.mrr:.4f}"
        )
        return best.profile, basis

    improvement = best.report.recall_at_k - baseline.report.recall_at_k
    if improvement < min_delta:
        basis = (
            f"лучший профиль {best.profile} (recall@K={best.report.recall_at_k:.4f}) "
            f"превосходит baseline R0 (recall@K={baseline.report.recall_at_k:.4f}) "
            f"лишь на Δ={improvement:.4f} < min_delta={min_delta:.4f}; "
            f"conservative-дефолт R0 (A3)"
        )
        return baseline.profile, basis

    basis = (
        f"профиль {best.profile} превосходит baseline R0 по recall@K: "
        f"{best.report.recall_at_k:.4f} vs {baseline.report.recall_at_k:.4f} "
        f"(Δ={improvement:.4f} ≥ min_delta={min_delta:.4f})"
    )
    return best.profile, basis


def run_retrieval_experiment(
    golden: Sequence[GoldenQuery],
    *,
    product_ids: Sequence[str],
    cutoff_k: int,
    profiles: Iterable[RetrievalProfile | str] = DEFAULT_PROFILES,
    lexical_index: LexicalIndexPort | None = None,
    vector_index: VectorIndexPort | None = None,
    embed: EmbedFn | None = None,
    rrf_k: int = DEFAULT_RRF_K,
    min_delta: float = 0.0,
) -> ExperimentReport:
    """Прогоняет R0/R1/R2 на retrieval-golden и выбирает профиль измерением (T8.6, R9/R10).

    :param golden: пары запрос→ожидаемые ``chunk_id`` (:class:`GoldenQuery`).
    :param product_ids: продукт-партиции запроса (I17).
    :param cutoff_k: ``retrieval_cutoff_K``; один и тот же K для golden и ретривала (I20).
    :param profiles: профили для прогона (по умолчанию :data:`DEFAULT_PROFILES`).
    :param lexical_index / vector_index: инъецированные порты (ручной DI, A1).
    :param embed: функция запрос→вектор для dense-плеча; без неё R1/R2 пропускаются.
    :param rrf_k: RRF-константа (версионируется в ``retrieval_params_json``, S1).
    :param min_delta: порог превосходства над baseline R0, ниже которого сохраняется R0 (A3).

    :raises ValueError: ``cutoff_k <= 0``, ``min_delta < 0``, неизвестный профиль или
        ни один профиль не может быть оценён (все пропущены).
    """
    if cutoff_k <= 0:
        raise ValueError("cutoff_k (retrieval_cutoff_K) должен быть > 0 (I20)")
    if min_delta < 0:
        raise ValueError("min_delta должен быть >= 0")

    # Дедуп с сохранением порядка для детерминизма и уникальности ключей by_profile.
    normalized: list[RetrievalProfile] = []
    for raw in profiles:
        profile = _coerce_profile(raw)
        if profile not in normalized:
            normalized.append(profile)

    evaluated: list[ProfileExperiment] = []
    skipped: dict[str, str] = {}
    for profile in normalized:
        reason = _missing_dependencies(
            profile,
            lexical_index=lexical_index,
            vector_index=vector_index,
            embed=embed,
        )
        if reason is not None:
            skipped[profile.value] = reason
            continue
        evaluated.append(
            _run_profile(
                profile,
                golden,
                product_ids=product_ids,
                cutoff_k=cutoff_k,
                lexical_index=lexical_index,
                vector_index=vector_index,
                embed=embed,
                rrf_k=rrf_k,
            )
        )

    if not evaluated:
        details = "; ".join(f"{name}: {reason}" for name, reason in skipped.items()) or "нет профилей"
        raise ValueError(f"ни один профиль не может быть оценён ({details})")

    by_profile = {experiment.profile: experiment.report for experiment in evaluated}
    per_query = {experiment.profile: list(experiment.report.per_query_rank) for experiment in evaluated}
    selected_profile, selection_basis = _select_profile(evaluated, min_delta=min_delta)

    return ExperimentReport(
        k=cutoff_k,
        by_profile=by_profile,
        selected_profile=selected_profile,
        selection_basis=selection_basis,
        per_query=per_query,
        rrf_k=rrf_k,
        skipped=skipped,
    )
