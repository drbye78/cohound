"""Эталон отвечающих документов: инварианты формата, каноническая запись, сводки (O42).

Предмет — **файл**, а не метрики: числа считает
:func:`core.pipeline.retrieval_docs.evaluate_documents` (один источник формул), а этот
модуль отвечает за то, чтобы набор нельзя было записать в противоречивом виде. Поэтому
здесь каждый инвариант проверяется **по одному**: тихая нормализация («додумаем префикс»,
«проигнорируем цитату») сделала бы эталон неотличимым от согласия с судьёй.

Отдельно проверяется воспроизводимость записи: два ``write`` одного набора обязаны дать
побайтово равные файлы — иначе «файл изменился» нельзя отличить от «файл переписан», и
диффы эталона перестают что-либо значить.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.pipeline.retrieval_answers import (
    ANSWER_SETS_FORMAT,
    AnswerDoc,
    AnswerEntry,
    AnswerPolicy,
    AnswerSets,
    JudgeSpec,
    answer_doc_ids,
    answer_doc_ids_without_retrieved,
    answers_summary,
    cited_doc_ids,
    load_answer_sets,
    sensitivity_by_cell,
    sensitivity_doc_ids,
    write_answer_sets,
)

PRODUCT = "p1"


def _policy() -> dict[str, Any]:
    return {
        "term_min_length": 4,
        "term_min_distinct": 2,
        "retrieved_depth": 0,
        "max_candidates_per_cell": 20,
    }


def _payload(**overrides: Any) -> dict[str, Any]:
    """Валидный файл: одна ячейка, два отвечающих документа (``cited`` и ``term``)."""
    payload: dict[str, Any] = {
        "format": ANSWER_SETS_FORMAT,
        "case_id": "case",
        "run_id": "run-1",
        "cutoff_k": 10,
        "judge": None,
        "policy": _policy(),
        "status": "candidate",
        "entries": [
            {
                "cell": f"{PRODUCT}/param_one",
                "product_id": PRODUCT,
                "param_id": "param_one",
                "cited_doc_ids": [f"{PRODUCT}:aaa"],
                "answer_docs": [
                    {"doc_id": f"{PRODUCT}:aaa", "how": ["cited"], "quote": ""},
                    {"doc_id": f"{PRODUCT}:bbb", "how": ["term"], "quote": ""},
                ],
            }
        ],
    }
    payload.update(overrides)
    return payload


def _load(tmp_path: Path, payload: dict[str, Any]) -> AnswerSets:
    path = tmp_path / "answers.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return load_answer_sets(path)


def _entry(**overrides: Any) -> dict[str, Any]:
    """Единственная запись валидного файла с точечной подменой полей."""
    entry = dict(_payload()["entries"][0])
    entry.update(overrides)
    return entry


# ---------------------------------------------------------------------------
# Загрузка и инварианты
# ---------------------------------------------------------------------------
def test_valid_file_loads_every_field(tmp_path: Path) -> None:
    sets = _load(tmp_path, _payload())
    assert sets.case_id == "case"
    assert sets.run_id == "run-1"
    assert sets.cutoff_k == 10
    assert sets.status == "candidate"
    assert sets.judge is None
    assert sets.policy == AnswerPolicy(4, 2, 0, 20)
    entry = sets.entry_for(f"{PRODUCT}/param_one")
    assert answer_doc_ids(entry) == (f"{PRODUCT}:aaa", f"{PRODUCT}:bbb")
    assert cited_doc_ids(entry) == (f"{PRODUCT}:aaa",)
    assert sets.by_cell()[entry.cell] is entry


def test_cited_document_outside_answer_set_is_refused(tmp_path: Path) -> None:
    """Адрес цитаты по определению отвечает: расхождение — ошибка разметки, не состояние."""
    entry = _entry(cited_doc_ids=[f"{PRODUCT}:ccc"])
    with pytest.raises(ValueError, match="cited_doc_ids"):
        _load(tmp_path, _payload(entries=[entry]))


def test_doc_id_of_another_product_is_refused(tmp_path: Path) -> None:
    """I17: документ другого продукта смешал бы продукты в одной метрике."""
    entry = _entry(
        answer_docs=[
            {"doc_id": f"{PRODUCT}:aaa", "how": ["cited"], "quote": ""},
            {"doc_id": "p2:bbb", "how": ["term"], "quote": ""},
        ]
    )
    with pytest.raises(ValueError, match="продукт"):
        _load(tmp_path, _payload(entries=[entry]))


def test_doc_id_without_product_prefix_is_refused(tmp_path: Path) -> None:
    entry = _entry(
        answer_docs=[{"doc_id": "bbb", "how": ["cited"], "quote": ""}],
        cited_doc_ids=["bbb"],
    )
    with pytest.raises(ValueError, match="продукт"):
        _load(tmp_path, _payload(entries=[entry]))


def test_duplicate_doc_id_in_a_cell_is_refused(tmp_path: Path) -> None:
    entry = _entry(
        answer_docs=[
            {"doc_id": f"{PRODUCT}:aaa", "how": ["cited"], "quote": ""},
            {"doc_id": f"{PRODUCT}:aaa", "how": ["term"], "quote": ""},
        ]
    )
    with pytest.raises(ValueError, match="дубль doc_id"):
        _load(tmp_path, _payload(entries=[entry]))


def test_empty_how_is_refused(tmp_path: Path) -> None:
    entry = _entry(answer_docs=[{"doc_id": f"{PRODUCT}:aaa", "how": [], "quote": ""}])
    with pytest.raises(ValueError, match="how"):
        _load(tmp_path, _payload(entries=[entry]))


def test_unknown_how_token_is_refused(tmp_path: Path) -> None:
    entry = _entry(answer_docs=[{"doc_id": f"{PRODUCT}:aaa", "how": ["vibes"], "quote": ""}])
    with pytest.raises(ValueError, match="неизвестные источники"):
        _load(tmp_path, _payload(entries=[entry]))


def test_duplicate_how_token_is_refused(tmp_path: Path) -> None:
    entry = _entry(
        answer_docs=[{"doc_id": f"{PRODUCT}:aaa", "how": ["cited", "cited"], "quote": ""}]
    )
    with pytest.raises(ValueError, match="дубли источников"):
        _load(tmp_path, _payload(entries=[entry]))


def test_llm_without_quote_is_refused(tmp_path: Path) -> None:
    """Суждение без цитаты — не доказательство (та же дисциплина, что I22)."""
    entry = _entry(
        cited_doc_ids=[],
        answer_docs=[{"doc_id": f"{PRODUCT}:aaa", "how": ["llm"], "quote": "  "}],
    )
    with pytest.raises(ValueError, match="quote"):
        _load(
            tmp_path,
            _payload(
                entries=[entry],
                judge={"model_id": "m", "revision": "r", "prompt_hash": "h"},
            ),
        )


def test_llm_without_judge_is_refused(tmp_path: Path) -> None:
    """Провенанс судьи обязателен: иначе набор неотличим от человеческой разметки."""
    entry = _entry(
        cited_doc_ids=[],
        answer_docs=[{"doc_id": f"{PRODUCT}:aaa", "how": ["llm"], "quote": "цитата"}],
    )
    with pytest.raises(ValueError, match="judge"):
        _load(tmp_path, _payload(entries=[entry], judge=None))


def test_human_without_quote_is_refused(tmp_path: Path) -> None:
    entry = _entry(
        cited_doc_ids=[],
        answer_docs=[{"doc_id": f"{PRODUCT}:aaa", "how": ["human"], "quote": ""}],
    )
    with pytest.raises(ValueError, match="quote"):
        _load(tmp_path, _payload(entries=[entry]))


def test_cell_not_matching_product_and_param_is_refused(tmp_path: Path) -> None:
    entry = _entry(cell=f"{PRODUCT}/other")
    with pytest.raises(ValueError, match="cell не совпадает"):
        _load(tmp_path, _payload(entries=[entry]))


def test_duplicate_cell_is_refused(tmp_path: Path) -> None:
    entry = _payload()["entries"][0]
    with pytest.raises(ValueError, match="дубль cell"):
        _load(tmp_path, _payload(entries=[entry, entry]))


def test_status_outside_the_set_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="status"):
        _load(tmp_path, _payload(status="approved"))


def test_cutoff_k_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cutoff_k"):
        _load(tmp_path, _payload(cutoff_k=0))


def test_empty_answer_docs_is_refused(tmp_path: Path) -> None:
    entry = _entry(answer_docs=[], cited_doc_ids=[])
    with pytest.raises(ValueError, match="answer_docs"):
        _load(tmp_path, _payload(entries=[entry]))


def test_foreign_format_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="format"):
        _load(tmp_path, _payload(format="cohound-retrieval-golden-docs/1"))


def test_policy_without_required_key_is_refused(tmp_path: Path) -> None:
    """Никаких тихих дефолтов: правило ``term`` — часть определения набора."""
    policy = _policy()
    policy.pop("term_min_distinct")
    with pytest.raises(ValueError, match="policy"):
        _load(tmp_path, _payload(policy=policy))


def test_missing_file_is_not_swallowed(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_answer_sets(tmp_path / "нет.json")


# ---------------------------------------------------------------------------
# Каноническая запись
# ---------------------------------------------------------------------------
def _canonical_sets() -> AnswerSets:
    """Набор с «неудобным» порядком: две ячейки, документы и how не по порядку."""
    return AnswerSets(
        case_id="case",
        run_id="run-1",
        cutoff_k=10,
        judge=JudgeSpec(model_id="m", revision="", prompt_hash="h"),
        policy=AnswerPolicy(4, 2, 0, 20),
        status="candidate",
        entries=(
            AnswerEntry(
                cell=f"{PRODUCT}/zzz",
                product_id=PRODUCT,
                param_id="zzz",
                cited_doc_ids=(f"{PRODUCT}:bbb",),
                answer_docs=(
                    AnswerDoc(f"{PRODUCT}:bbb", ("cited", "term"), ""),
                    AnswerDoc(f"{PRODUCT}:aaa", ("llm", "cited"), "цитата"),
                ),
            ),
            AnswerEntry(
                cell=f"{PRODUCT}/aaa",
                product_id=PRODUCT,
                param_id="aaa",
                cited_doc_ids=(),
                answer_docs=(AnswerDoc(f"{PRODUCT}:ccc", ("retrieved", "cited"), ""),),
            ),
        ),
    )


def test_write_is_byte_identical_on_repeat(tmp_path: Path) -> None:
    sets = _canonical_sets()
    first = tmp_path / "one.json"
    second = tmp_path / "two.json"
    write_answer_sets(first, sets)
    write_answer_sets(second, sets)
    assert first.read_bytes() == second.read_bytes()


def test_write_sorts_entries_docs_and_how(tmp_path: Path) -> None:
    path = tmp_path / "answers.json"
    write_answer_sets(path, _canonical_sets())
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert [entry["cell"] for entry in payload["entries"]] == [
        f"{PRODUCT}/aaa",
        f"{PRODUCT}/zzz",
    ]
    zzz = payload["entries"][1]
    assert [doc["doc_id"] for doc in zzz["answer_docs"]] == [
        f"{PRODUCT}:aaa",
        f"{PRODUCT}:bbb",
    ]
    # ``how`` — в порядке ANSWER_HOW (cited раньше term/llm), а не в порядке входа.
    assert zzz["answer_docs"][0]["how"] == ["cited", "llm"]
    assert zzz["answer_docs"][1]["how"] == ["cited", "term"]


def test_written_file_round_trips_through_the_loader(tmp_path: Path) -> None:
    """Запись → загрузка → запись побайтово равна: канонизация идемпотентна.

    Сравнивать загруженное с исходным объектом нельзя: запись **канонизирует** порядок
    документов и токенов ``how``, и это её работа. Значимое свойство — второй проход
    ничего не меняет, иначе файл «дрожал» бы при каждом пересчёте.
    """
    path = tmp_path / "answers.json"
    again = tmp_path / "again.json"
    sets = _canonical_sets()
    write_answer_sets(path, sets)
    loaded = load_answer_sets(path)
    write_answer_sets(again, loaded)
    assert path.read_bytes() == again.read_bytes()
    assert {entry.cell for entry in loaded.entries} == {entry.cell for entry in sets.entries}
    assert loaded.judge == sets.judge
    assert loaded.policy == sets.policy


def test_reviewed_by_is_written_only_when_present(tmp_path: Path) -> None:
    path = tmp_path / "answers.json"
    write_answer_sets(path, _canonical_sets())
    assert "reviewed_by" not in json.loads(path.read_text(encoding="utf-8"))
    # Запись без ``llm``: ``judge=None`` с суждением загрузчик справедливо отверг бы.
    plain = AnswerEntry(
        cell=f"{PRODUCT}/aaa",
        product_id=PRODUCT,
        param_id="aaa",
        cited_doc_ids=(f"{PRODUCT}:aaa",),
        answer_docs=(AnswerDoc(f"{PRODUCT}:aaa", ("cited",), ""),),
    )
    sets = AnswerSets(
        case_id="case",
        run_id="run-1",
        cutoff_k=10,
        judge=None,
        policy=AnswerPolicy(4, 2, 0, 20),
        status="reviewed",
        entries=(plain,),
        reviewed_by="roger",
    )
    write_answer_sets(path, sets)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["reviewed_by"] == "roger"
    assert load_answer_sets(path).reviewed_by == "roger"


# ---------------------------------------------------------------------------
# Сводки и чувствительность
# ---------------------------------------------------------------------------
def test_sensitivity_is_per_cell_not_global() -> None:
    """Один документ: в A из выдачи, в B по терминам — глобальный фильтр занизил бы B.

    Числа: глобальное множество дало бы ``sensitivity = {X}`` и выбросило X и из B, где он
    пришёл по ``term`` (независимый источник) — чувствительность B упала бы с 1.0 до 0.0.
    По-ячеечный фильтр оставляет B нетронутой: ``sensitivity(B) == ()``.
    """
    a = AnswerEntry(
        cell=f"{PRODUCT}/a",
        product_id=PRODUCT,
        param_id="a",
        cited_doc_ids=(f"{PRODUCT}:aaa",),
        answer_docs=(
            AnswerDoc(f"{PRODUCT}:aaa", ("cited",), ""),
            AnswerDoc(f"{PRODUCT}:x", ("retrieved",), ""),
        ),
    )
    b = AnswerEntry(
        cell=f"{PRODUCT}/b",
        product_id=PRODUCT,
        param_id="b",
        cited_doc_ids=(f"{PRODUCT}:bbb",),
        answer_docs=(
            AnswerDoc(f"{PRODUCT}:bbb", ("cited",), ""),
            AnswerDoc(f"{PRODUCT}:x", ("term",), ""),
        ),
    )
    assert sensitivity_doc_ids(a) == (f"{PRODUCT}:x",)
    assert sensitivity_doc_ids(b) == ()
    assert sensitivity_by_cell([a, b]) == {f"{PRODUCT}/a": (f"{PRODUCT}:x",), f"{PRODUCT}/b": ()}
    # Остаток: у A X выпадает, у B — остаётся (иначе B мерилась бы без своего ответа).
    assert answer_doc_ids_without_retrieved(a) == (f"{PRODUCT}:aaa",)
    assert answer_doc_ids_without_retrieved(b) == (f"{PRODUCT}:bbb", f"{PRODUCT}:x")


def test_sensitivity_cell_of_only_retrieved_answers_gives_empty_remainder() -> None:
    """Пустой остаток — законный ноль ячейки (прибор держит её в знаменателе)."""
    entry = AnswerEntry(
        cell=f"{PRODUCT}/a",
        product_id=PRODUCT,
        param_id="a",
        cited_doc_ids=(),
        answer_docs=(AnswerDoc(f"{PRODUCT}:x", ("retrieved",), ""),),
    )
    assert answer_doc_ids_without_retrieved(entry) == ()


def test_sensitivity_ignores_documents_without_retrieved() -> None:
    entry = AnswerEntry(
        cell=f"{PRODUCT}/param_one",
        product_id=PRODUCT,
        param_id="param_one",
        cited_doc_ids=(f"{PRODUCT}:aaa",),
        answer_docs=(
            AnswerDoc(f"{PRODUCT}:aaa", ("cited",), ""),
            AnswerDoc(f"{PRODUCT}:bbb", ("term",), ""),
        ),
    )
    assert sensitivity_by_cell([entry]) == {entry.cell: ()}


def test_answers_summary_on_synthetic_set() -> None:
    sets = _canonical_sets()
    summary = answers_summary(sets.entries)
    assert summary["cells"] == 2
    assert summary["documents"] == 3
    assert summary["distinct_documents"] == 3
    # ``how`` считается по документам: cited 3, term 1, retrieved 1, llm 1, human 0.
    assert summary["how"] == {
        "cited": 3,
        "term": 1,
        "retrieved": 1,
        "llm": 1,
        "human": 0,
    }
    assert summary["docs_per_cell"] == {"min": 1, "median": 1.5, "max": 2}
    # cited-only — ячеек нет; без term — одна (ячейка aaa: только cited+retrieved).
    assert summary["cells_cited_only"] == 0
    assert summary["cells_without_term"] == 1
    assert summary["judged_docs"] == 1
    # Классы по ячейкам: aaa = {cited, retrieved} → mixed; zzz = {cited, term, llm} → mixed.
    # Все классы присутствуют, нули видны как нули, порядок фиксирован.
    assert summary["cells_sources"] == {
        "cited": 0,
        "term": 0,
        "retrieved": 0,
        "llm": 0,
        "human": 0,
        "mixed": 2,
        "unknown": 0,
    }
    assert list(summary["cells_sources"]) == [
        "cited",
        "term",
        "retrieved",
        "llm",
        "human",
        "mixed",
        "unknown",
    ]


def test_cells_sources_separates_single_source_cells() -> None:
    """Класс ячейки — объединение ``how`` её членов: один токен → он, несколько → mixed."""
    entries = [
        AnswerEntry(
            cell=f"{PRODUCT}/cited_only",
            product_id=PRODUCT,
            param_id="cited_only",
            cited_doc_ids=(f"{PRODUCT}:a1",),
            answer_docs=(AnswerDoc(f"{PRODUCT}:a1", ("cited",), ""),),
        ),
        AnswerEntry(
            cell=f"{PRODUCT}/term_only",
            product_id=PRODUCT,
            param_id="term_only",
            cited_doc_ids=(),
            answer_docs=(AnswerDoc(f"{PRODUCT}:a2", ("term",), ""),),
        ),
        AnswerEntry(
            cell=f"{PRODUCT}/retrieved_only",
            product_id=PRODUCT,
            param_id="retrieved_only",
            cited_doc_ids=(),
            answer_docs=(AnswerDoc(f"{PRODUCT}:a3", ("retrieved",), ""),),
        ),
        AnswerEntry(
            cell=f"{PRODUCT}/human_judged",
            product_id=PRODUCT,
            param_id="human_judged",
            cited_doc_ids=(),
            answer_docs=(AnswerDoc(f"{PRODUCT}:a4", ("human",), "цитата"),),
        ),
        AnswerEntry(
            cell=f"{PRODUCT}/unknown_how",
            product_id=PRODUCT,
            param_id="unknown_how",
            cited_doc_ids=(),
            answer_docs=(AnswerDoc(f"{PRODUCT}:a5", (), ""),),
        ),
    ]
    sources = answers_summary(entries)["cells_sources"]
    assert sources["cited"] == 1
    assert sources["term"] == 1
    assert sources["retrieved"] == 1
    assert sources["human"] == 1
    assert sources["llm"] == 0
    assert sources["mixed"] == 0
    assert sources["unknown"] == 1
    assert sum(sources.values()) == len(entries)


def test_answers_summary_counts_cited_only_cell() -> None:
    entry = AnswerEntry(
        cell=f"{PRODUCT}/param_one",
        product_id=PRODUCT,
        param_id="param_one",
        cited_doc_ids=(f"{PRODUCT}:aaa",),
        answer_docs=(AnswerDoc(f"{PRODUCT}:aaa", ("cited",), ""),),
    )
    summary = answers_summary([entry])
    assert summary["cells_cited_only"] == 1
    assert summary["cells_without_term"] == 1
    assert summary["docs_per_cell"] == {"min": 1, "median": 1.0, "max": 1}


def test_entry_for_unknown_cell_names_the_cell() -> None:
    sets = _canonical_sets()
    with pytest.raises(KeyError, match="нет/такой"):
        sets.entry_for(f"{PRODUCT}/нет/такой")
