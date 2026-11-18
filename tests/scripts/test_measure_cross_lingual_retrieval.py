"""Тесты прибора кросс-языкового замера поиска (`scripts/measure_cross_lingual_retrieval.py`).

Прибор отвечает числом на вопрос «находит ли поиск ZH-документ по **русскому** запросу и
меняет ли это dense-плечо», и ломается он тихо: числа получаются правдоподобными при
любом из дефектов ниже. Поэтому каждый проверяется отдельно и **без сети, без LLM** — на
внедрённых швах (`retrieve`, `_retrieve`, `_lexical_index`, `evaluate_cell` подменяются).

* **метрика не насыщается** — ``summarize_profile`` обязана считать ранг, а не факт
  «что-то вернулось»: на ячейках с одинаковыми ``counts`` разные ``rank`` дают разные
  ``hit_at``/``mrr``. Обратное (числа совпали) означало бы, что прибор хвалит dense за
  непустую выдачу — замер 18.11.2026 давал так R1 30/30.
* **``hit@3`` режет по порогу** — ``_anchor_cells(cutoff=3)``: ``rank=3`` попадание,
  ``rank=4`` нет. Без этого возвращается сравнение «на всю глубину top_k», где срез
  насыщен и плечи неразличимы.
* **``_ru_only_queries`` не пропускает латиницу** ни через ``title``, ни через
  ``concept_terms['ru']`` (в каталоге первый элемент — копия заголовка, поэтому латинский
  мост приходил именно оттуда), схлопывает дубликаты, сохраняет порядок и берёт ≤ 3.
* **``_retrieve`` сливает по ``max``** и не выдумывает вектор: для ``R0`` запрос уходит
  без вектора, для ``R1`` — с вектором от переданного эмбеддера.
* **``--ru-query`` доходит до поиска** — ``measure_param(ru_only=True)`` обязан передать в
  ``_retrieve`` **русские** запросы: первая редакция звала ``retrieve_for_cell``, который
  строит запросы сам, и метка ``latin_in_query`` была ложью (поймано 18.11.2026).
* **``DenseMatrix`` считает косинус**, а не сырое произведение: векторы разной длины с
  одинаковым направлением дают ≈ 1.0; ``top_k`` больше числа чанков не падает.
* **судья не подменяет вердикт** — ``judge_cell`` отдаёт ``status=None`` при
  ``failure_kind``, и агрегация считает такую ячейку ``unscored``, а не ``❓``.
* **``--profiles`` отвергает неизвестный профиль** и нормализует порядок.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain.enums import AbsenceRuleType, Importance, ParamType
from core.domain.models import CatalogRow

_ROOT = Path(__file__).resolve().parents[2]


def _load_module() -> Any:
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_cross_lingual_retrieval.py"
    spec = importlib.util.spec_from_file_location("measure_cross_lingual_retrieval", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_cross_lingual_retrieval"] = module
    spec.loader.exec_module(module)
    return module


measure = _load_module()

#: Сторона пары живёт в соседнем приборе (он загружен импортом выше): берём настоящий
#: класс оттуда, чтобы пара была той же формы, что в замере.
SideText: Any = sys.modules["measure_cross_lingual_eval"].SideText


def _param(
    title: str,
    terms: dict[str, list[str]],
    *,
    param_id: str = "p1",
) -> Any:
    """Строка каталога (остальные поля — минимальные валидные)."""
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="core",
        group="g",
        importance=Importance.IMPORTANT,
        param_type=ParamType.FACT,
        rule="Правило.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="core_engine",
        concept_terms=terms,
        search_langs=["ru", "en"],
    )


def _chunk(chunk_id: str, text: str) -> Any:
    """Чанк стороны пары (поля — как у чанкера)."""
    return measure.Chunk(
        chunk_id=chunk_id,
        chunk_no=0,
        text=text,
        heading_path="h",
        context_prefix="",
        content_hash="0" * 16,
    )


def _pair(param: Any, *, path: str = "sync.html", anchor: str = "wal") -> Any:
    """Пара страниц: обе стороны с одним чанком, без слайсов (слайсы прибору не нужны)."""
    return measure.Pair(
        param=param,
        anchor=anchor,
        path=path,
        en=SideText(
            url=f"https://example.org/en/{path}",
            text="en",
            chunks=(_chunk("en#0", "текст без якоря"),),
            reader_degraded=False,
        ),
        zh=SideText(
            url=f"https://example.org/zh/{path}",
            text="zh",
            chunks=(_chunk("zh#0", "文本"),),
            reader_degraded=False,
        ),
        en_slice=(),
        zh_slice=(),
    )


def _retrieved(chunk_id: str, score: float, *, text: str = "фрагмент") -> Any:
    """Чанк выдачи ретривала: адрес, оценка и текст (якоря в тексте по умолчанию нет)."""
    return measure.RetrievedChunk(
        chunk_id=chunk_id,
        product_id=measure.PRODUCT_ID,
        text=text,
        content_hash="0" * 16,
        heading_path="h",
        build_id="xling-en-1",
        score=score,
    )


def _summary_cell(
    param_id: str,
    *,
    found: bool = True,
    count: int = 10,
    anchor: bool = False,
    rank: int | None = None,
    profile: str = "R0",
) -> dict[str, Any]:
    """Ячейка в форме, которую строит ``measure_param`` (для сводки).

    ``anchor`` — всегда ``bool`` (как в приборе: ``first is not None``), а не ``None``:
    отсутствие якоря у контрольного параметра выражается рангом, а не дырой в поле.
    """
    return {
        "param_id": param_id,
        "en_found": {profile: found},
        "en_counts": {profile: count},
        "en_anchor": {profile: anchor},
        "en_rank": {profile: rank},
    }


# --------------------------------------------------------------------------- #
# Метрика не насыщается: ранг, а не «что-то вернулось»
# --------------------------------------------------------------------------- #


def test_summary_separates_ranks_with_identical_counts() -> None:
    """``counts`` одинаковы (все по 10) — метрика обязана различать ячейки по ``rank``.

    Антипод: реализация, возвращающая ``found_share`` вместо рангов, даёт одинаковые
    числа на обоих наборах, и тест обязан упасть. Замер 18.11.2026: именно так R1 давал
    «нашёл» на всех ячейках — dense ранжирует любой текст, поэтому непустая выдача
    ничего не доказывает.
    """
    good = [_summary_cell(f"p{i}", rank=1) for i in range(4)]
    bad = [_summary_cell(f"p{i}", rank=None) for i in range(4)]

    good_summary = measure.summarize_profile(good, side="en", profile="R0")
    bad_summary = measure.summarize_profile(bad, side="en", profile="R0")

    # Общая часть у наборов одна — иначе различие можно было бы объяснить ею.
    assert good_summary["found_share"] == bad_summary["found_share"] == 1.0
    assert good_summary["chunks_mean"] == bad_summary["chunks_mean"] == 10.0

    assert good_summary["hit_at"]["3"] == 4
    assert bad_summary["hit_at"]["3"] == 0
    assert good_summary["mrr"] == pytest.approx(1.0)
    assert bad_summary["mrr"] == pytest.approx(0.0)


def test_summary_counts_anchorless_cell_as_zero_in_mrr_denominator() -> None:
    """Ячейка без якоря даёт ``0.0`` в среднем, а не выбрасывается из знаменателя.

    Иначе «плечо ничего не нашло» исчезает из MRR, и прибор хвалит себя за пустоту.
    """
    cells = [
        _summary_cell("p1", rank=1),
        _summary_cell("p2", rank=None),
        _summary_cell("p3", rank=None),
    ]
    summary = measure.summarize_profile(cells, side="en", profile="R0")
    assert summary["mrr"] == pytest.approx(1.0 / 3.0)
    assert summary["hit_at"]["1"] == 1
    assert summary["hit_at"]["3"] == 1


def test_summary_hit_at_thresholds_use_rank_not_presence() -> None:
    """``hit@1`` и ``hit@3`` — разные пороги: ранг 2 входит во второй и не входит в первый."""
    cells = [
        _summary_cell("p1", rank=2),
        _summary_cell("p2", rank=3),
        _summary_cell("p3", rank=4),
    ]
    summary = measure.summarize_profile(cells, side="en", profile="R0")
    assert summary["hit_at"]["1"] == 0
    assert summary["hit_at"]["3"] == 2
    # MRR считает обратный ранг, а не «вошёл ли в срез».
    assert summary["mrr"] == pytest.approx((1 / 2 + 1 / 3 + 1 / 4) / 3)


def test_summary_without_anchor_reports_no_mrr_and_no_hit_at() -> None:
    """Контроль без якоря (``include_anchor=False``) не выдумывает MRR из пустого среза."""
    cells = [_summary_cell("p1", rank=None)]
    summary = measure.summarize_profile(cells, side="en", profile="R0", include_anchor=False)
    assert summary["mrr"] is None
    assert summary["hit_at"] == {}
    assert summary["anchor"] is None


# --------------------------------------------------------------------------- #
# hit@3: срез по порогу, а не на всю глубину top_k
# --------------------------------------------------------------------------- #


def test_anchor_cells_cut_at_three() -> None:
    """``rank=3`` — попадание, ``rank=4`` — нет: иначе срез насыщается и плечи неразличимы."""
    cells = [
        {"param_id": "p1", "en_rank": {"R0": 3}},
        {"param_id": "p2", "en_rank": {"R0": 4}},
        {"param_id": "p3", "en_rank": {"R0": None}},
        {"param_id": "p4", "en_rank": {"R0": 1}},
    ]
    cut = measure._anchor_cells(cells, side="en", profile="R0", cutoff=3)
    assert {cell["cell"]: cell["rank"] for cell in cut} == {
        "p1:en": 0,
        "p2:en": None,
        "p3:en": None,
        "p4:en": 0,
    }


def test_anchor_cells_default_cutoff_is_the_whole_depth() -> None:
    """Дефолт — ``top_k`` замера (10): ``rank=4`` попадает, когда срез не сужен."""
    cells = [{"param_id": "p1", "en_rank": {"R0": 4}}]
    assert measure._anchor_cells(cells, side="en", profile="R0")[0]["rank"] == 0


# --------------------------------------------------------------------------- #
# _ru_only_queries: латиница не проходит ни через один вход
# --------------------------------------------------------------------------- #


def test_ru_only_queries_drops_latin_title_and_latin_terms() -> None:
    """Заголовок с ``LDAP`` не идёт целиком, термин ``MS AD`` — не идёт вовсе.

    Заголовок в каталоге продублирован первым элементом ``concept_terms['ru']``, поэтому
    латинский мост приходил именно оттуда — проверяются оба входа.
    """
    param = _param(
        "Синхронизация ролей и групп из LDAP",
        {
            "ru": [
                "Синхронизация ролей и групп из LDAP",  # копия заголовка (как в каталоге)
                "Аутентификация пользователей",
                "MS AD",
                "Синхронизация с LDAP",  # кириллица + латиница: тоже мост
            ],
            "en": ["ldap"],
        },
    )
    queries = measure._ru_only_queries(param)
    assert queries == ["Аутентификация пользователей"]
    assert not any(measure._LATIN.search(query) for query in queries)


def test_ru_only_queries_keeps_order_collapses_duplicates_and_caps_at_three() -> None:
    """Порядок — как в каталоге, дубликаты схлопнуты, не больше трёх запросов (S13)."""
    param = _param(
        "Резервное копирование",
        {
            "ru": [
                "Резервное копирование",  # дубликат заголовка
                "Архивация",
                "Восстановление",
                "Контрольная точка",
                "Репликация",
            ],
        },
    )
    assert measure._ru_only_queries(param) == [
        "Резервное копирование",
        "Архивация",
        "Восстановление",
    ]


def test_ru_only_queries_does_not_substitute_english() -> None:
    """Русских терминов нет — пустой список, а не английский запрос «вместо»."""
    param = _param("LDAP authentication", {"en": ["ldap", "authentication"], "ru": ["MS AD"]})
    assert measure._ru_only_queries(param) == []


# --------------------------------------------------------------------------- #
# _retrieve: слияние по max, порядок, вектор
# --------------------------------------------------------------------------- #


def _retrieve_recorder(
    results: dict[str, list[Any]],
) -> tuple[Any, list[dict[str, Any]]]:
    """Подмена ``retrieve``: записывает аргументы и отдаёт заранее заданную выдачу."""
    calls: list[dict[str, Any]] = []

    def fake(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return SimpleNamespace(chunks=results.get(str(kwargs["query_text"]), []))

    return fake, calls


def _embedder(vector: list[float]) -> tuple[Any, list[list[str]]]:
    """Подмена эмбеддера: считает, какие тексты у него спросили."""
    calls: list[list[str]] = []

    def embed(texts: list[str]) -> list[list[float]]:
        calls.append(list(texts))
        return [list(vector) for _ in texts]

    return SimpleNamespace(embed=embed), calls


def test_retrieve_merges_same_chunk_by_max_score(monkeypatch: pytest.MonkeyPatch) -> None:
    """Один ``chunk_id`` из двух запросов — остаётся большая оценка (дефолт фазы: ``max``)."""
    fake, calls = _retrieve_recorder(
        {
            "a": [_retrieved("c1", 0.2), _retrieved("c2", 0.7)],
            "b": [_retrieved("c1", 0.9)],
        }
    )
    monkeypatch.setattr(measure, "retrieve", fake)

    result = measure._retrieve(
        ["a", "b"],
        profile="R0",
        top_k=10,
        lexical=object(),
        dense=None,
        embedder=None,
        query_vectors=None,
    )

    assert [(chunk.chunk_id, chunk.score) for chunk in result] == [("c1", 0.9), ("c2", 0.7)]
    assert len(calls) == 2
    assert calls[0]["product_ids"] == [measure.PRODUCT_ID]
    assert calls[0]["cutoff_k"] == 10


def test_retrieve_sorts_by_score_desc_then_chunk_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Порядок детерминирован: по убыванию оценки, при равенстве — по адресу."""
    fake, _ = _retrieve_recorder({"a": [_retrieved("c2", 0.5), _retrieved("c1", 0.5)]})
    monkeypatch.setattr(measure, "retrieve", fake)

    result = measure._retrieve(
        ["a"],
        profile="R0",
        top_k=10,
        lexical=object(),
        dense=None,
        embedder=None,
        query_vectors=None,
    )
    assert [chunk.chunk_id for chunk in result] == ["c1", "c2"]


def test_retrieve_truncates_to_top_k(monkeypatch: pytest.MonkeyPatch) -> None:
    """Срез — объявленный ``cutoff_k``, а не «сколько вернули запросы» (I20)."""
    fake, _ = _retrieve_recorder(
        {"a": [_retrieved("c1", 0.9), _retrieved("c2", 0.8)], "b": [_retrieved("c3", 0.7)]}
    )
    monkeypatch.setattr(measure, "retrieve", fake)

    result = measure._retrieve(
        ["a", "b"],
        profile="R0",
        top_k=2,
        lexical=object(),
        dense=None,
        embedder=None,
        query_vectors=None,
    )
    assert [chunk.chunk_id for chunk in result] == ["c1", "c2"]


def test_retrieve_does_not_invent_vector_for_lexical_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``R0`` — вектор не считается и не передаётся, даже если эмбеддер под рукой."""
    fake, calls = _retrieve_recorder({"a": []})
    monkeypatch.setattr(measure, "retrieve", fake)
    embedder, embed_calls = _embedder([1.0, 0.0])

    measure._retrieve(
        ["a"],
        profile="R0",
        top_k=5,
        lexical=object(),
        dense=None,
        embedder=embedder,
        query_vectors=None,
    )

    assert calls[0]["query_vector"] is None
    assert embed_calls == []


def test_retrieve_computes_vector_for_dense_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """``R1`` без пре-компьюта: вектор запроса считается переданным эмбеддером."""
    fake, calls = _retrieve_recorder({"a": []})
    monkeypatch.setattr(measure, "retrieve", fake)
    embedder, embed_calls = _embedder([3.0, 0.0])

    measure._retrieve(
        ["a"],
        profile="R1",
        top_k=5,
        lexical=object(),
        dense=object(),
        embedder=embedder,
        query_vectors=None,
    )

    assert calls[0]["query_vector"] == [3.0, 0.0]
    assert embed_calls == [["a"]]


def test_retrieve_takes_precomputed_vector_without_calling_embedder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Готовый вектор из карты пре-компьюта идёт как есть: эмбеддер не зовётся."""
    fake, calls = _retrieve_recorder({"a": []})
    monkeypatch.setattr(measure, "retrieve", fake)
    embedder, embed_calls = _embedder([9.0, 9.0])

    measure._retrieve(
        ["a"],
        profile="R1",
        top_k=5,
        lexical=object(),
        dense=object(),
        embedder=embedder,
        query_vectors={"a": [1.0, 2.0]},
    )

    assert calls[0]["query_vector"] == [1.0, 2.0]
    assert embed_calls == []


# --------------------------------------------------------------------------- #
# --ru-query доходит до поиска
# --------------------------------------------------------------------------- #


def _sent_queries(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Подмена ``_retrieve`` (шов «что уходит в поиск») + пустой лексический индекс."""
    sent: list[list[str]] = []

    def fake_retrieve(queries: Any, **_kwargs: Any) -> list[Any]:
        sent.append(list(queries))
        return []

    monkeypatch.setattr(measure, "_retrieve", fake_retrieve)
    monkeypatch.setattr(
        measure, "_lexical_index", lambda chunks, *, build_id: SimpleNamespace(close=lambda: None)
    )
    return sent


def test_ru_query_flag_reaches_search_with_russian_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ru_only=True`` — в поиск уходят русские запросы: метка ``latin_in_query`` не лжёт.

    Дефект, который ловит тест: путь через ``retrieve_for_cell`` строит запросы сам, и
    флаг ``--ru-query`` до поиска не доходил (замер 18.11.2026: прибор «находил» 5, а
    прямой русский запрос — 0).
    """
    sent = _sent_queries(monkeypatch)
    param = _param(
        "Синхронизация ролей и групп из LDAP",
        {"ru": ["Аутентификация пользователей"], "en": ["ldap"]},
    )
    pair = _pair(param)

    record = measure.measure_param(
        pair,
        param,
        anchor=None,
        profiles=("R0",),
        top_k=3,
        embedder=None,
        query_vectors=None,
        indexes={},
        ru_only=True,
    )

    assert sent, "поиск не вызывался"
    assert sent[0] == ["Аутентификация пользователей"]
    assert not any(measure._LATIN.search(query) for query in sent[0])
    assert record["queries"] == sent[0]
    assert record["latin_in_query"] is False


def test_without_ru_query_search_gets_render_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ru_only=False`` — запросы Ш6 (``render_queries``), где латинский заголовок есть."""
    sent = _sent_queries(monkeypatch)
    param = _param(
        "Синхронизация ролей и групп из LDAP",
        {"ru": ["Аутентификация пользователей"], "en": ["ldap"]},
    )

    record = measure.measure_param(
        _pair(param),
        param,
        anchor=None,
        profiles=("R0",),
        top_k=3,
        embedder=None,
        query_vectors=None,
        indexes={},
        ru_only=False,
    )

    assert sent[0] == ["Синхронизация ролей и групп из LDAP", "Аутентификация пользователей", "ldap"]
    assert any(measure._LATIN.search(query) for query in sent[0])
    assert record["latin_in_query"] is True


# --------------------------------------------------------------------------- #
# DenseMatrix: косинус, а не сырое произведение
# --------------------------------------------------------------------------- #


def test_dense_matrix_scores_by_cosine() -> None:
    """Векторы разной длины с одинаковым направлением дают ≈ 1.0, а не сырое скалярное."""
    chunks = [_chunk("c0", "первый"), _chunk("c1", "второй")]
    matrix = measure.DenseMatrix(chunks, [[2.0, 0.0, 0.0], [0.0, 1.0, 0.0]], build_id="xling-en-2")

    hits = matrix.search(
        measure.VectorQuery(product_ids=[measure.PRODUCT_ID], top_k=5, vector=[3.0, 0.0, 0.0])
    )

    # Сырое произведение дало бы 6.0 (2·3) — косинус обязан быть 1.0.
    assert hits[0].chunk_id == "c0"
    assert hits[0].score == pytest.approx(1.0, abs=1e-6)
    assert hits[1].score == pytest.approx(0.0, abs=1e-6)
    assert matrix.dim == 3
    assert matrix.build_id == "xling-en-2"


def test_dense_matrix_top_k_larger_than_material_returns_everything() -> None:
    """``top_k`` больше числа чанков не падает и не выдумывает пустые места."""
    chunks = [_chunk("c0", "первый"), _chunk("c1", "второй")]
    matrix = measure.DenseMatrix(chunks, [[1.0, 0.0], [0.0, 1.0]], build_id="xling-en-2")

    hits = matrix.search(
        measure.VectorQuery(product_ids=[measure.PRODUCT_ID], top_k=50, vector=[1.0, 1.0])
    )
    assert len(hits) == 2
    assert {hit.chunk_id for hit in hits} == {"c0", "c1"}


def test_dense_matrix_refuses_text_only_query() -> None:
    """Без вектора dense-плечо отказывает, а не считает текст «похожим»."""
    matrix = measure.DenseMatrix([_chunk("c0", "первый")], [[1.0, 0.0]], build_id="b")
    with pytest.raises(ValueError):
        matrix.search(
            measure.VectorQuery(
                product_ids=[measure.PRODUCT_ID], top_k=1, text="запрос", vector=None
            )
        )


# --------------------------------------------------------------------------- #
# Судья: форма ответа и «unscored», а не ❓
# --------------------------------------------------------------------------- #


def test_judge_cell_reports_none_status_and_caps_quotes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Сбой судьи — ``status=None`` (не ``❓``), цитат ≤ 2, каждая обрезана до 100."""
    outcome = SimpleNamespace(
        cell=SimpleNamespace(
            status=None,
            evidence=[
                SimpleNamespace(quote="ж" * 150),
                SimpleNamespace(quote=""),
                SimpleNamespace(quote="вторая цитата"),
                SimpleNamespace(quote="третья цитата"),
            ],
        ),
        failure_kind="evaluation_failed",
    )
    monkeypatch.setattr(measure, "evaluate_cell", lambda **kwargs: outcome)

    judged = measure.judge_cell(
        _param("Резервное копирование", {"ru": ["Резервное копирование"]}),
        [_retrieved("c0", 1.0)],
        evaluator=object(),
        run_id="xling-judge-r0",
    )

    assert judged["status"] is None
    assert judged["failure_kind"] == "evaluation_failed"
    assert judged["quotes"] == ["ж" * 100, "вторая цитата"]
    assert all(len(quote) <= 100 for quote in judged["quotes"])


def test_judge_summary_counts_failed_cells_as_unscored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ячейка без статуса — ``unscored``, а не ``❓``: агрегация в ``main`` не выдумывает вердикт.

    Проверяется через ``main`` целиком (все швы — ``PageCache``/каталог/пары/ретривал/
    оценщик — подменены): сама агрегация живёт внутри ``main`` и другой точки входа не
    имеет. Подмена ``evaluate_cell`` возвращает ``status=None`` — ровно то, что даёт
    сбой судьи, и это не должно попасть в счётчик ``❓``.
    """
    param = _param("Синхронизация ролей", {"ru": ["Синхронизация ролей"]})
    pair = _pair(param, path="sync.html")
    fake_outcome = SimpleNamespace(
        cell=SimpleNamespace(status=None, evidence=[]), failure_kind="evaluation_failed"
    )

    monkeypatch.setattr(
        measure,
        "PageCache",
        lambda *args, **kwargs: SimpleNamespace(
            stats=SimpleNamespace(
                as_dict=lambda: {
                    "pages_network": 0,
                    "pages_from_cache": 0,
                    "mb_network": 0.0,
                    "seconds": 0.0,
                }
            )
        ),
    )
    monkeypatch.setattr(measure, "load_catalog", lambda path: [param])
    monkeypatch.setattr(measure, "_load_discovery", lambda cache: ((), []))
    monkeypatch.setattr(
        measure,
        "MarkdownChunkerPort",
        lambda: SimpleNamespace(chunk_size=1200, version="md-test"),
    )
    monkeypatch.setattr(measure, "HtmlExtractor", lambda: SimpleNamespace(version="html-test"))
    monkeypatch.setattr(measure, "build_pairs", lambda *args, **kwargs: ([pair], {}, []))
    monkeypatch.setattr(
        measure, "load_deploy_config", lambda path: SimpleNamespace(embed=None, llm=None)
    )
    monkeypatch.setattr(
        measure,
        "resolve_embed_spec",
        lambda cfg, override: SimpleNamespace(model_id="model", revision="0" * 40),
    )
    monkeypatch.setattr(measure, "resolve_embed_device", lambda cfg, override: "cpu")
    monkeypatch.setattr(measure, "resolve_embed_precision", lambda cfg, override: "fp32")
    monkeypatch.setattr(measure, "build_evaluator_llm", lambda **kwargs: object())
    monkeypatch.setattr(
        measure,
        "_lexical_index",
        lambda chunks, *, build_id: SimpleNamespace(close=lambda: None),
    )
    monkeypatch.setattr(measure, "_retrieve", lambda *args, **kwargs: [_retrieved("c0", 1.0)])
    monkeypatch.setattr(measure, "evaluate_cell", lambda **kwargs: fake_outcome)

    out = tmp_path / "xling-retrieval.json"
    code = measure.main(
        [
            "--catalog",
            str(tmp_path / "catalog.csv"),
            "--deploy",
            str(tmp_path / "deploy.yaml"),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--profiles",
            "R0",
            "--control-params",
            "1",
            "--judge",
            "--judge-profile",
            "R0",
            "--judge-cells",
            "5",
            "--bootstrap",
            "20",
            "--json",
            str(out),
        ]
    )

    assert code == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    judge = payload["control"]["judge"]
    assert judge["en"]["cells"] == 1
    assert judge["en"]["unscored"] == 1
    assert judge["en"]["unknown"] == 0
    assert judge["en"]["ok"] == 0
    assert judge["zh"]["unscored"] == 1


# --------------------------------------------------------------------------- #
# Разбор --profiles
# --------------------------------------------------------------------------- #


def test_profiles_rejects_unknown_name_and_normalises_order() -> None:
    """Неизвестный профиль — отказ, а порядок фиксирован (R0 → R1 → R2) независимо от флага."""
    assert measure._profiles("R2,R0") == ["R0", "R2"]
    assert measure._profiles("r1, r0") == ["R0", "R1"]
    assert measure._profiles("R0,R1,R2") == ["R0", "R1", "R2"]
    with pytest.raises(SystemExit):
        measure._profiles("R0,R9")
    with pytest.raises(SystemExit):
        measure._profiles(" , ")
