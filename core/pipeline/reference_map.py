"""coHound — карта reference-продукта (план 27 §9): Capability/Coverage по каталогу профиля.

Карта отвечает на вопрос «какую долю **наших** параметров документация предка покрывает и
насколько уверенно». Она считается **тем же кодом**, что бандл когорты
(:func:`core.pipeline.compute._compute_from_cells`), по ячейкам, полученным оценщиком на
reference-корпусе одного продукта одной версии.

Три границы, закреплённые здесь кодом:

* **не входит в сравнение когорты**: артефакт лежит в ``reference/…/maps/{map_id}/``, а не
  в ``runs/{run_id}/outputs/``; отчёт когорты его не читает;
* **не смешивается** с Capability продуктов: своя идентичность (``map_id``), а не
  ``run_id`` — у карты нет когорты, сидов и покрытия продуктов, и ``run_id`` описывал бы
  не тот предмет;
* **не выносит вердикт о продукте когорты**: ``❌`` карты — про документацию предка.

Идентичность ``map_id`` — content-addressed от **объявленных** входов: сборка корпуса,
отпечаток фактов (карта и факты обязаны быть про одно), каталог, модель+декодер,
шаблон промпта и политика оценки. ``observed_at`` в неё **не** входит: дата снятия —
провенанс, а не условие измерения.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.domain.enums import AnchorRelation, FactStatus, Status
from core.domain.models import CatalogRow, Cell, Cohort, CohortEntry
from core.pipeline.compute import ScoreBundle, _compute_from_cells
from core.pipeline.evaluate import evaluate_cell, run_queries_for_cell
from core.pipeline.inheritance import major_version
from core.ports.lexical_index import LexicalDocLike, LexicalSearcherPort
from core.ports.vector_index import RetrievalResult, RetrievedChunk
from core.util.agreement import KappaResult, _cohen_kappa
from core.util.hashing import short_hash

__all__ = [
    "MAP_SCHEMA",
    "ReferenceMapIncomplete",
    "ReferenceMapLayout",
    "ReferenceMapManifest",
    "ReferenceMapResult",
    "build_reference_map",
    "cell_row",
    "fact_map_agreement",
    "read_map_cells",
    "read_map_manifest",
    "reference_map_id",
    "write_map",
]

#: Версия формата артефактов карты (``manifest.json``/``cells.jsonl``/``score.json``).
MAP_SCHEMA = "reference-map/1"

#: Профиль ретривала карты: лексический, без векторов (§8.4).
MAP_RETRIEVAL_PROFILE = "R0"

#: Значение ``subject`` в ``score.json``: тот же формат, что у бандла когорты, но предмет
#: другой. Поле стоит рядом с ``capability``/``coverage``, чтобы читатель не спутал карту
#: предка с картой продукта (один формат на два предмета — §9.4).
MAP_SUBJECT = "reference"


class ReferenceMapIncomplete(RuntimeError):
    """Карта не собрана: часть ячеек упала, и частичный артефакт не пишется.

    Частичная карта опаснее отсутствующей: ``params`` в манифесте описывал бы каталог, а
    ``cells.jsonl`` — его подмножество, и Capability считалась бы по неполному набору без
    всякого признака неполноты. Поэтому сбой ячейки не «пропускается», а называется:
    ``failed`` несёт ``param_id: тип: сообщение`` по каждой упавшей ячейке.
    """

    def __init__(self, failed: Sequence[str]) -> None:
        self.failed: tuple[str, ...] = tuple(failed)
        super().__init__(
            f"карта не собрана: упало ячеек {len(self.failed)}; "
            + "; ".join(self.failed[:5])
            + (" …" if len(self.failed) > 5 else "")
        )


def reference_map_id(
    *,
    reference_build_id: str,
    baseline_facts_digest: str | None,
    catalog_version: str | None,
    llm_scenarios: Any,
    prompt_template: str | None,
    evaluation_policy: Any,
) -> str:
    """``map_id`` — content-addressed идентичность карты (§7.3).

    Материал — ровно объявленные условия: какие чанки прочитаны, какие факты были на
    столе, какой каталог, какая модель и политика. ``observed_at``/пути/имена файлов в
    материал не входят: дата снятия и раскладка хоста — провенанс, а не измерение.

    :returns: ``map_{16 hex}`` — короткий, но content-addressed (как ``partition_build_id``).
    """
    payload = {
        "reference_build_id": str(reference_build_id),
        "baseline_facts_digest": baseline_facts_digest,
        "catalog_version": catalog_version,
        "llm_scenarios": _canonical(llm_scenarios),
        "prompt_template": prompt_template,
        "evaluation_policy": _canonical(evaluation_policy),
    }
    material = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return f"map_{short_hash(material, 16)}"


def _canonical(value: Any) -> Any:
    """Приводит значение к JSON-стабильной форме (словари — по ключам, списки — по месту).

    Модели Pydantic сериализуются через ``model_dump``, остальное — ``str``: ``map_id``
    обязан быть функцией **значения**, а не repr объекта с адресом в памяти.
    """
    if value is None or isinstance(value, str | int | float | bool):
        return value
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return _canonical(dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_canonical(item) for item in value]
    return str(value)


# ---------------------------------------------------------------------------
# Раскладка и манифест
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReferenceMapLayout:
    """``reference/{package}/{product}/{version}/maps/{map_id}/`` (§9.4).

    Путь приходит от вызывающего (composition root), а не выводится из окружения:
    раскладка хоста — не измерение.
    """

    root: Path
    map_id: str

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.json"

    @property
    def cells_path(self) -> Path:
        return self.root / "cells.jsonl"

    @property
    def score_path(self) -> Path:
        return self.root / "score.json"

    def ensure(self) -> ReferenceMapLayout:
        """Создаёт каталог карты (идемпотентно)."""
        self.root.mkdir(parents=True, exist_ok=True)
        return self


@dataclass
class ReferenceMapManifest:
    """``manifest.json`` карты: чем и по какому каталогу она посчитана (§9.4).

    :param counts: распределение вердиктов (``present``/``absent``/``unknown``/``failed``)
        — число, по которому видно, что карта не пуста, без открытия ``cells.jsonl``.
    """

    schema: str
    map_id: str
    reference_build_id: str
    product_id: str
    version: str
    params: int
    counts: dict[str, int] = field(default_factory=dict)
    baseline_facts_digest: str | None = None
    catalog_version: str | None = None
    llm_scenarios: Any = None
    prompt_template: str | None = None
    evaluation_policy: Any = None
    cutoff_k: int | None = None
    retrieval_profile: str = MAP_RETRIEVAL_PROFILE
    observed_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "map_id": self.map_id,
            "reference_build_id": self.reference_build_id,
            "product_id": self.product_id,
            "version": self.version,
            "params": self.params,
            "counts": dict(sorted(self.counts.items())),
            "baseline_facts_digest": self.baseline_facts_digest,
            "catalog_version": self.catalog_version,
            "llm_scenarios": _canonical(self.llm_scenarios),
            "prompt_template": self.prompt_template,
            "evaluation_policy": _canonical(self.evaluation_policy),
            "cutoff_k": self.cutoff_k,
            "retrieval_profile": self.retrieval_profile,
            "observed_at": self.observed_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReferenceMapManifest:
        return cls(
            schema=str(data.get("schema", MAP_SCHEMA)),
            map_id=str(data["map_id"]),
            reference_build_id=str(data.get("reference_build_id", "")),
            product_id=str(data.get("product_id", "")),
            version=str(data.get("version", "")),
            params=int(data.get("params", 0)),
            counts={str(k): int(v) for k, v in (data.get("counts") or {}).items()},
            baseline_facts_digest=data.get("baseline_facts_digest"),
            catalog_version=data.get("catalog_version"),
            llm_scenarios=data.get("llm_scenarios"),
            prompt_template=data.get("prompt_template"),
            evaluation_policy=data.get("evaluation_policy"),
            cutoff_k=data.get("cutoff_k"),
            retrieval_profile=str(data.get("retrieval_profile", MAP_RETRIEVAL_PROFILE)),
            observed_at=data.get("observed_at"),
        )


def read_map_manifest(path: str | Path) -> ReferenceMapManifest | None:
    """Читает ``manifest.json`` карты; отсутствие — ``None`` (карты ещё нет)."""
    file = Path(path)
    if not file.is_file():
        return None
    return ReferenceMapManifest.from_dict(
        json.loads(file.read_text(encoding="utf-8"))
    )


# ---------------------------------------------------------------------------
# Ретривал: BM25 по reference-корпусу (без векторов, §8.4)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _IndexDoc:
    """Документ для индекса: ровно то, что нужно ``LexicalSearcherPort`` (структурно).

    Отдельный тип, а не ``MapChunk``: индекс не должен видеть адрес документа и его хеш —
    иначе «что индексируется» и «что цитируется» станут одним объектом, и правка одного
    молча изменит другое.
    """

    chunk_id: str
    product_id: str
    text: str


@dataclass(frozen=True)
class MapChunk:
    """Чанк reference-корпуса, каким его видит карта (минимум для ретривала)."""

    chunk_id: str
    doc_id: str
    url: str | None
    text: str
    content_hash: str


def _retrieve(
    param: CatalogRow,
    *,
    index: LexicalSearcherPort,
    chunks: Mapping[str, MapChunk],
    product_id: str,
    build_id: str,
    cutoff_k: int,
) -> RetrievalResult:
    """Top-K чанков параметра: запросы Ш6, объединение по лучшему счёту (R7).

    Запросы берутся **тем же** :func:`run_queries_for_cell`, что у ячейки когорты: карта
    должна отвечать на тот же вопрос, что оценщик, иначе расхождение факт↔карта мерило бы
    разницу формулировок, а не разницу путей.
    """
    best: dict[str, float] = {}
    for query in run_queries_for_cell(param):
        for chunk_id, score in index.search_scores(
            query, product_ids=[product_id], top_k=cutoff_k
        ):
            previous = best.get(chunk_id)
            if previous is None or score > previous:
                best[chunk_id] = score
    ordered = sorted(best.items(), key=lambda item: (-item[1], item[0]))[:cutoff_k]
    retrieved = [
        RetrievedChunk(
            chunk_id=chunk_id,
            product_id=product_id,
            text=chunks[chunk_id].text,
            content_hash=chunks[chunk_id].content_hash,
            heading_path="",
            build_id=build_id,
            score=score,
        )
        for chunk_id, score in ordered
        if chunk_id in chunks
    ]
    return RetrievalResult(
        chunks=retrieved,
        profile=MAP_RETRIEVAL_PROFILE,
        cutoff_K=cutoff_k,
        build_id=build_id,
    )


# ---------------------------------------------------------------------------
# Сборка карты
# ---------------------------------------------------------------------------


@dataclass
class ReferenceMapResult:
    """Результат сборки: ячейки, бандл, манифест (всё, что пишется на диск)."""

    manifest: ReferenceMapManifest
    cells: list[Cell]
    bundle: ScoreBundle

    @property
    def counts(self) -> dict[str, int]:
        return dict(self.manifest.counts)


def _count_verdicts(cells: Sequence[Cell]) -> dict[str, int]:
    """Распределение вердиктов карты (``failed`` — отдельно: это не вердикт, I4)."""
    counts: dict[str, int] = {}
    for cell in cells:
        if cell.state.value != "evaluated" or cell.status is None:
            key = "failed" if cell.state.value == "failed" else "unresolved"
        elif cell.status in {Status.OK, Status.PARTIAL}:
            key = "present"
        elif cell.status is Status.ABSENT:
            key = "absent"
        else:
            key = "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _single_product_cohort(product_id: str) -> Cohort:
    """Однопродуктовая когорта для Comparability: у карты нет когорты профиля.

    Без когорты Comparability была бы ``INSUFFICIENT`` с причиной «когорты нет», что для
    карты предка — верное утверждение, но бесполезное: карта не сравнивается ни с кем.
    Однопродуктовая когорта делает аннотацию честной (один продукт, он же якорь).
    """
    return Cohort(
        case_id="reference",
        frozen_at=_today(),
        entries=[
            CohortEntry(
                product_id=product_id,
                role_overlap=1.0,
                selection_reason="reference map",
                anchor_relation=AnchorRelation.ANCHOR,
            )
        ],
    )


def _today() -> Any:
    import datetime as _dt

    return _dt.date.today()


def build_reference_map(
    *,
    layout: ReferenceMapLayout,
    product_id: str,
    version: str,
    reference_build_id: str,
    chunks: Iterable[MapChunk],
    params: Sequence[CatalogRow],
    evaluator: Any,
    lexical_index: LexicalSearcherPort,
    catalog: Mapping[str, CatalogRow] | None = None,
    module_weights: Mapping[str, float] | None = None,
    baseline_facts_digest: str | None = None,
    catalog_version: str | None = None,
    llm_scenarios: Any = None,
    prompt_template: str | None = None,
    evaluation_policy: Any = None,
    cutoff_k: int = 10,
    model_id: str | None = None,
    observed_at: str | None = None,
    artifact_dates: Any = None,
    coverage: Any = None,
    sleep: Callable[[float], Any] | None = None,
    llm_concurrency: int = 1,
    progress: Callable[[int, int, str], None] | None = None,
) -> ReferenceMapResult:
    """Собирает карту: ретривал по reference-корпусу → оценка → бандл (§9.1).

    Ячейка оценивается **тем же** :func:`core.pipeline.evaluate.evaluate_cell`, что у
    когорты, с тем же промптом и эшелоном валидации (I22 и прочее): второй формулы
    оценки нет, иначе расхождение факт↔карта мерило бы разницу реализаций.

    ``run_id`` ячейки — ``map_id``: у карты нет прогона, и подставлять чужой идентификатор
    значило бы выдать её ячейки за ячейки когорты.

    **Число вызовов оценщика в полёте (``llm_concurrency``).** Карта упирается в
    латентность модели, а не в локальную работу: замер живого прогона — p50 17.6 с на
    вызов, локальная часть (ретривал, валидация, сборка ячейки) — доли секунды. При
    ``> 1`` ячейки считаются пулом потоков тем же :func:`evaluate_cell`, что и в Ш6, где
    параллельный путь уже исполняется (``core/pipeline/evaluate.py``,
    ``llm_concurrency``). Ось **не** входит в ``map_id``: число вызовов в полёте —
    условие исполнения, а не измерения, ровно как у ``--llm-concurrency`` прогона (он
    тоже не в ``EXECUTION_CONTRACT_FIELDS``). Порядок ячеек — порядок ``params`` при
    любом значении: ``cells.jsonl`` не должен зависеть от того, кто ответил первым.

    **Отсутствие доказывается датированным носителем и здесь.** ``artifact_dates`` и
    ``coverage`` передаются в :func:`evaluate_cell`, поэтому I2c/I2d действуют на карте
    так же, как в Ш6. Замер 18.11.2026: без них карта опубликовала ``❌`` класса
    ``explicit`` по цитате, у которой нет записи наблюдения (``corpus_artifact`` в
    reference-БД пуста, 0 строк), — то есть утверждение «этого нет» без даты. Теперь
    такой ``❌`` понижается до ``❓`` с токеном ``absence_undated``, а даты берутся из самой
    сборки (:func:`core.pipeline.reference_corpus.read_artifact_dates`,
    :func:`core.pipeline.reference_corpus.coverage_from_manifest`).

    **``sleep`` по умолчанию — настоящий.** ``evaluate_cell`` ждёт между попытками
    (``_retry_pause``, ``Retry-After`` провайдера), и ``None`` здесь означал бы «внедрённой
    паузы нет», а не «не спать»: первая же повторная попытка падала бы
    ``TypeError: 'NoneType' object is not callable`` и убивала карту на 181-й ячейке из 181
    (замер 18.11.2026 — так и вышло). Поэтому ``None`` нормализуется в ``time.sleep``, а
    тесты внедряют свою функцию явно.

    **Изоляция сбоя.** Исключение на одной ячейке не теряет остальные: ошибки
    собираются и называются в конце, а карта при их наличии **не** возвращается —
    частичная карта выдала бы себя за полную (``params`` в манифесте разошёлся бы с
    числом ячеек). ``progress(index, total, param_id)`` вызывается после каждой готовой
    ячейки: прогон длится десятки минут, и «молчащий» прибор неотличим от зависшего.

    :param lexical_index: собранный лексический индекс (``LexicalSearcherPort``); движок
        выбирает composition root — карта спрашивает лексику через контракт, а не через
        адаптер.
    :param catalog: ``{param_id: CatalogRow}`` для расчёта; ``None`` — из ``params``.
    :param module_weights: объявленные веса модулей пакета; ``None`` — все по 0.0, и тогда
        Capability не определена (честнее, чем выдумать равные веса).
    """
    pause = sleep if sleep is not None else time.sleep
    if isinstance(llm_concurrency, bool) or not isinstance(llm_concurrency, int):
        raise ValueError(f"llm_concurrency обязан быть целым, получено {llm_concurrency!r}")
    if llm_concurrency < 1:
        raise ValueError(f"llm_concurrency обязан быть ≥ 1, получено {llm_concurrency!r}")
    material = list(chunks)
    catalog_map: dict[str, CatalogRow] = dict(catalog or {})
    for param in params:
        catalog_map.setdefault(str(param.param_id), param)

    # Движок лексики — адаптер, его выбирает composition root (матрица 01 §1: pipeline не
    # импортирует adapters). Здесь только контракт: собрать и спросить.
    docs: list[LexicalDocLike] = [
        _IndexDoc(chunk_id=chunk.chunk_id, product_id=product_id, text=chunk.text)
        for chunk in material
    ]
    lexical_index.index(docs)
    by_id = {chunk.chunk_id: chunk for chunk in material}

    map_id = reference_map_id(
        reference_build_id=reference_build_id,
        baseline_facts_digest=baseline_facts_digest,
        catalog_version=catalog_version,
        llm_scenarios=llm_scenarios,
        prompt_template=prompt_template,
        evaluation_policy=evaluation_policy,
    )

    cells: list[Cell | None] = [None] * len(params)

    def _cell_for(param: CatalogRow) -> Cell:
        """Одна ячейка карты: ретривал по ссылочному индексу и оценка тем же оценщиком."""
        retrieval = _retrieve(
            param,
            index=lexical_index,
            chunks=by_id,
            product_id=product_id,
            build_id=reference_build_id,
            cutoff_k=cutoff_k,
        )
        outcome = evaluate_cell(
            param=param,
            product_id=product_id,
            run_id=map_id,
            evaluator=evaluator,
            chunks=list(retrieval.chunks),
            retrievals=retrieval,
            strict=True,
            model_id=model_id,
            artifact_dates=artifact_dates,
            coverage=coverage,
            sleep=pause,
        )
        return outcome.cell

    failed: list[str] = []

    def _record(index: int, param: CatalogRow, cell: Cell) -> None:
        cells[index] = cell
        if progress is not None:
            progress(index + 1, len(params), str(param.param_id))

    if llm_concurrency == 1:
        # Последовательный путь — прежний байт-в-байт: тот же порядок вызовов и тот же
        # порядок исключений, что до появления оси.
        for index, param in enumerate(params):
            try:
                _record(index, param, _cell_for(param))
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                failed.append(f"{param.param_id}: {type(exc).__name__}: {exc}")
    else:
        with ThreadPoolExecutor(
            max_workers=llm_concurrency, thread_name_prefix="map-llm"
        ) as pool:
            futures = {
                pool.submit(_cell_for, param): (index, param)
                for index, param in enumerate(params)
            }
            for future in as_completed(futures):
                index, param = futures[future]
                try:
                    _record(index, param, future.result())
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    failed.append(f"{param.param_id}: {type(exc).__name__}: {exc}")

    if failed:
        # Частичная карта НЕ собирается: ``params`` в манифесте разошёлся бы с числом
        # ячеек, и артефакт выдал бы себя за полный. Ошибки названы все сразу — повтор
        # после починки не требует угадывать, что именно упало.
        raise ReferenceMapIncomplete(tuple(failed))

    material_cells: list[Cell] = []
    for cell in cells:
        if cell is None:  # pragma: no cover — исключено проверкой ``failed`` выше
            raise ReferenceMapIncomplete(("неизвестно",))
        material_cells.append(cell)
    weights = dict(module_weights or {})
    bundle = _compute_from_cells(
        material_cells,
        catalog=catalog_map,
        module_weights=weights,
        cohort=_single_product_cohort(product_id),
        alignment=None,
        requested_ranking="full",
        universe=[(product_id, str(param.param_id)) for param in params],
    )

    manifest = ReferenceMapManifest(
        schema=MAP_SCHEMA,
        map_id=map_id,
        reference_build_id=reference_build_id,
        product_id=product_id,
        version=version,
        params=len(params),
        counts=_count_verdicts(material_cells),
        baseline_facts_digest=baseline_facts_digest,
        catalog_version=catalog_version,
        llm_scenarios=_canonical(llm_scenarios),
        prompt_template=prompt_template,
        evaluation_policy=_canonical(evaluation_policy),
        cutoff_k=cutoff_k,
        retrieval_profile=MAP_RETRIEVAL_PROFILE,
        observed_at=observed_at,
    )
    return ReferenceMapResult(manifest=manifest, cells=material_cells, bundle=bundle)


# ---------------------------------------------------------------------------
# Запись
# ---------------------------------------------------------------------------


def cell_row(cell: Cell) -> dict[str, Any]:
    """Строка ``cells.jsonl``: вердикт + доказательство + свидетельство о дефектах."""
    return {
        "param_id": cell.param_id,
        "product_id": cell.product_id,
        "state": cell.state.value,
        "status": cell.status.value if cell.status is not None else None,
        "source_kind": cell.source_kind.value if cell.source_kind is not None else None,
        "evidence": [
            {
                "source_id": item.source_id,
                "quote": item.quote,
                "url": getattr(item, "url", None),
            }
            for item in cell.evidence
        ],
        "absence_evidence": list(cell.absence_evidence),
        "review_reason": cell.review_reason,
        "validation_notes": list(cell.validation_notes),
        "retrieval_profile": cell.retrieval_profile,
        "index_build_id": cell.index_build_id,
        # Дата отсутствия (I2d) публикуется вместе с вердиктом: `❌` — утверждение о
        # событии, и без даты носителя оно недоказуемо. У карты нет таблицы `cells`,
        # поэтому `cells.jsonl` — единственное место, где эта дата может жить.
        "absence_as_of": cell.absence_as_of,
    }


def write_map(
    layout: ReferenceMapLayout,
    result: ReferenceMapResult,
    *,
    extra_score: Mapping[str, Any] | None = None,
) -> dict[str, Path]:
    """Пишет артефакты карты: ``manifest.json``, ``cells.jsonl``, ``score.json`` (§9.4).

    ``score.json`` — **та же форма**, что бандл когорты, плюс ``subject: reference``:
    один формат на два предмета, чтобы читатель не учил второй. ``extra_score`` —
    поля, которые нужны только карте (согласие факт↔карта, разложение причин).
    """
    layout.ensure()
    score = result.bundle.to_dict()
    score["subject"] = MAP_SUBJECT
    score["map_id"] = result.manifest.map_id
    score["reference_build_id"] = result.manifest.reference_build_id
    score["product_id"] = result.manifest.product_id
    score["version"] = result.manifest.version
    if extra_score:
        score.update(dict(extra_score))

    manifest_text = json.dumps(
        result.manifest.as_dict(), ensure_ascii=False, sort_keys=True, indent=2
    )
    layout.manifest_path.write_text(manifest_text + "\n", encoding="utf-8")

    rows = sorted(result.cells, key=lambda cell: cell.param_id)
    cells_text = "".join(
        json.dumps(cell_row(cell), ensure_ascii=False, sort_keys=True) + "\n"
        for cell in rows
    )
    layout.cells_path.write_text(cells_text, encoding="utf-8")

    score_text = json.dumps(score, ensure_ascii=False, sort_keys=True, indent=2)
    layout.score_path.write_text(score_text + "\n", encoding="utf-8")
    return {
        "manifest": layout.manifest_path,
        "cells": layout.cells_path,
        "score": layout.score_path,
    }


def read_map_cells(path: str | Path) -> list[dict[str, Any]]:
    """Читает ``cells.jsonl`` (по строке на параметр); отсутствие — пустой список."""
    file = Path(path)
    if not file.is_file():
        return []
    return [
        json.loads(line)
        for line in file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# ---------------------------------------------------------------------------
# Согласие «факты ↔ карта» (§9.5)
# ---------------------------------------------------------------------------


def _map_verdict(row: Mapping[str, Any]) -> str | None:
    """Вердикт карты в бинарной форме для согласия: ``present``/``absent``/``None``.

    ``❓``/``failed``/``pending`` не превращаются в «нет»: неизвестность — не отрицание
    (I12), и пара «факт present × карта ❓» не является расхождением о наличии.
    """
    if row.get("state") != "evaluated":
        return None
    status = row.get("status")
    if status in {Status.OK.value, Status.PARTIAL.value}:
        return "present"
    if status == Status.ABSENT.value:
        return "absent"
    return None


def _fact_verdict(fact: Any) -> str | None:
    """Вердикт факта в бинарной форме: ``present``/``absent``/``None`` (``unclear``)."""
    if fact.status is FactStatus.PRESENT:
        return "present"
    if fact.status is FactStatus.ABSENT:
        return "absent"
    return None


def fact_map_agreement(
    facts: Sequence[Any],
    cells: Sequence[Mapping[str, Any]],
    *,
    baseline_id: str | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    """Согласие «факт ↔ карта» по одним и тем же параметрам (§9.5).

    Это **измерение прибора**, а не проверка правильности: факт ищет место в тексте
    детерминированно, карта решает, доказан ли параметр, читая те же чанки. Разница между
    ними — цена дешёвого пути, и она публикуется числом (κ, ``n``, список расхождений),
    а не мнением.

    ``n = 0`` даёт ``kappa = None`` с причиной — не ``0.0``: выдуманное согласие хуже
    отсутствующего (A3).

    :param baseline_id: ограничить факты одной линией (по умолчанию — все).
    :param version: версия **карты**. Факты живут по версиям (наблюдение @11 — не
        наблюдение @18), и смешивать их значило бы сравнивать разные корпуса: на
        пере-разведённом слое (8 версий × 181) пара «факт @11 × карта @18» попадала в
        ``n`` и размывала κ (замер 19.11.2026: n=216 вместо 181). ``None`` — не
        фильтровать (совместимо с вызовом без версии), сравнение идёт по мажору.
    """
    map_by_param: dict[str, Mapping[str, Any]] = {
        str(row.get("param_id")): row for row in cells
    }
    wanted_version = major_version(version) if version is not None else None
    pairs: list[tuple[str, str]] = []
    disagreements: list[dict[str, Any]] = []
    compared: list[str] = []
    skipped_other_versions = 0
    for fact in facts:
        if baseline_id is not None and fact.baseline_id != baseline_id:
            continue
        if wanted_version is not None:
            fact_major = major_version(getattr(fact, "version", None))
            if fact_major is not None and fact_major != wanted_version:
                skipped_other_versions += 1
                continue
        row = map_by_param.get(str(fact.param_id))
        if row is None:
            continue
        fact_side = _fact_verdict(fact)
        map_side = _map_verdict(row)
        if fact_side is None or map_side is None:
            continue
        compared.append(str(fact.param_id))
        pairs.append((fact_side, map_side))
        if fact_side != map_side:
            disagreements.append(
                {
                    "param_id": str(fact.param_id),
                    "fact": fact_side,
                    "map": map_side,
                }
            )
    kappa: KappaResult = _cohen_kappa(pairs)
    return {
        "n": kappa.n,
        "kappa": kappa.kappa,
        "undefined_reason": kappa.undefined_reason,
        "observed_agreement": kappa.observed_agreement,
        "expected_agreement": kappa.expected_agreement,
        "compared": len(compared),
        "disagreements": disagreements,
        "fact_present_map_absent": sum(
            1 for item in disagreements if item["fact"] == "present"
        ),
        "fact_absent_map_present": sum(
            1 for item in disagreements if item["fact"] == "absent"
        ),
        # Сколько фактов других версий отброшено: смешение версий размывает κ, и его
        # нельзя прятать — «сравнили не то» должно быть видно числом.
        "skipped_other_versions": skipped_other_versions,
        "version": version,
    }
