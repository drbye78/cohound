"""Тесты прибора ``scripts/measure_evaluator_ab.py``.

Прибор отвечает на один вопрос: **сдвинул ли промпт** поведение оценщика на тех же
ячейках. Поэтому проверяется не «работает ли статистика» вообще, а три вещи, без которых
ответ был бы подделкой:

* парность — дискордантные пары считаются по ячейкам, а не по агрегатам, и МакНемар точный
  (при малых парах аппроксимация ``χ²`` даёт другой вывод);
* срез «дефисные параметры» — **то же** множество, что меняет разбор запроса в адаптере
  (иначе срез мерил бы не то изменение);
* «тот же материал» — сверка по **тексту** фрагментов, а не по ``chunk_id``: после
  пересборки документа адрес меняется, а текст может остаться тем же (и наоборот).

Живой корпус не нужен: ``tmp_path`` + минимальные БД с нужными таблицами.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_evaluator_ab as measure  # noqa: E402

from core.domain.enums import Status  # noqa: E402

# ---------------------------------------------------------------------------
# Статистика
# ---------------------------------------------------------------------------


def test_mcnemar_exact_symmetric_and_equal_split_is_one() -> None:
    assert measure.mcnemar_exact(5, 5) == pytest.approx(1.0)
    assert measure.mcnemar_exact(0, 0) == pytest.approx(1.0)
    assert measure.mcnemar_exact(3, 7) == pytest.approx(measure.mcnemar_exact(7, 3))


def test_mcnemar_exact_matches_binomial_tails() -> None:
    # 1 против 7: 2 * (C(8,0) + C(8,1)) / 2**8 = 2 * 9 / 256.
    assert measure.mcnemar_exact(1, 7) == pytest.approx(18 / 256)
    # 0 против 10: 2 * 1 / 1024.
    assert measure.mcnemar_exact(0, 10) == pytest.approx(2 / 1024)


def test_bootstrap_ci_is_reproducible_and_brackets_the_mean() -> None:
    values = [1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0]
    first = measure.bootstrap_ci(values, n=2000, seed=7)
    second = measure.bootstrap_ci(values, n=2000, seed=7)
    assert first == second
    mean = sum(values) / len(values)
    assert first[0] <= mean <= first[1]


def test_bootstrap_ci_of_constant_sample_is_degenerate() -> None:
    assert measure.bootstrap_ci([1.0] * 10, n=500, seed=1) == pytest.approx((1.0, 1.0))


def test_bootstrap_ci_of_empty_sample_is_nan() -> None:
    lo, hi = measure.bootstrap_ci([], n=100, seed=1)
    assert lo != lo and hi != hi  # NaN с обеих сторон: выборки нет, а не «ноль»


# ---------------------------------------------------------------------------
# Срез «дефисные параметры» — тот же, что меняет разбор запроса
# ---------------------------------------------------------------------------


def test_hyphen_regex_is_the_adapter_rule() -> None:
    """Срез обязан быть тем же множеством, что разворачивает адаптер: сверка по образцу."""
    from core.adapters import lexical_tantivy

    assert measure._HYPHEN_COMPOUND.pattern == lexical_tantivy._HYPHEN_COMPOUND.pattern


def test_hyphen_params_uses_the_executed_query_composition() -> None:
    """Дефис в кандидате, отрезанном бюджетом запросов, в срез не входит."""
    real = _ROOT / "packages" / "ru-pg-dbms" / "catalog.csv"
    found = measure.hyphen_params(real)
    assert "xlsx_102_rezhim_autoprepare_sql_operatorov" in found
    # ``pluggable-toast`` есть в каталоге, но в первые три запроса не попадает ⇒ выдача
    # на нём не менялась, и в срез он не входит.
    assert "xlsx_119_pluggable_toast" not in found
    assert 0 < len(found) < 181


# ---------------------------------------------------------------------------
# «Тот же материал»
# ---------------------------------------------------------------------------


def test_same_material_compares_texts_not_addresses() -> None:
    prompts_a = {("p", "q"): ("dv_new:1", "dv_new:2")}
    prompts_b = {("p", "q"): ("dv_old:1", "dv_old:2")}
    hashes_a = {"dv_new:1": "aa", "dv_new:2": "bb"}
    hashes_b = {"dv_old:1": "bb", "dv_old:2": "aa"}  # другой порядок — тот же материал
    assert measure.same_material(prompts_a, prompts_b, hashes_a, hashes_b) == {
        ("p", "q"): True
    }


def test_same_material_detects_changed_text_under_the_same_address() -> None:
    prompts = {("p", "q"): ("dv:1",)}
    assert measure.same_material(prompts, prompts, {"dv:1": "aa"}, {"dv:1": "cc"}) == {
        ("p", "q"): False
    }


def test_same_material_treats_a_missing_prompt_as_different() -> None:
    assert measure.same_material({("p", "q"): ("dv:1",)}, {}, {"dv:1": "aa"}, {}) == {
        ("p", "q"): False
    }


def test_same_material_counts_a_missing_chunk_as_different() -> None:
    prompts = {("p", "q"): ("dv:1",)}
    assert measure.same_material(prompts, prompts, {"dv:1": "aa"}, {}) == {
        ("p", "q"): False
    }


# ---------------------------------------------------------------------------
# Парное сравнение среза
# ---------------------------------------------------------------------------


def _cell(status: Status | None, *, quote: bool = False, state: str = "evaluated"):
    return measure.ArmCell(state=state, status=status, has_quote=quote)


def test_slice_report_counts_discordant_pairs_by_cell() -> None:
    keys = [("p", "a"), ("p", "b"), ("p", "c"), ("p", "d")]
    arm_a = {
        ("p", "a"): _cell(Status.OK, quote=True),
        ("p", "b"): _cell(Status.PARTIAL, quote=True),
        ("p", "c"): _cell(Status.UNKNOWN),
        ("p", "d"): _cell(Status.ABSENT, quote=True),
    }
    arm_b = {
        ("p", "a"): _cell(Status.OK, quote=True),
        ("p", "b"): _cell(Status.UNKNOWN),
        ("p", "c"): _cell(Status.PARTIAL, quote=True),
        ("p", "d"): _cell(Status.ABSENT, quote=True),
    }
    stats = measure.slice_report(keys, arm_a, arm_b, bootstrap=200, seed=3)
    assert stats["cells"] == 4
    assert (stats["present_a"], stats["present_b"]) == (2, 2)
    assert (stats["gained"], stats["lost"]) == (1, 1)
    assert stats["mcnemar_p"] == pytest.approx(1.0)
    assert stats["quoted_a"] == 3 and stats["quoted_b"] == 3
    assert stats["present_quoted_a"] == 2 and stats["present_quoted_b"] == 2


def test_slice_report_keeps_failed_and_pending_out_of_presence() -> None:
    keys = [("p", "a"), ("p", "b")]
    arm_a = {
        ("p", "a"): _cell(None, state="failed"),
        ("p", "b"): _cell(None, state="pending"),
    }
    arm_b = {
        ("p", "a"): _cell(Status.OK, quote=True),
        ("p", "b"): _cell(Status.UNKNOWN),
    }
    stats = measure.slice_report(keys, arm_a, arm_b, bootstrap=100, seed=1)
    assert stats["statuses_a"]["failed"] == 1
    assert stats["statuses_a"]["pending"] == 1
    assert stats["present_a"] == 0
    assert stats["present_b"] == 1


# ---------------------------------------------------------------------------
# Чтение артефактов прогона
# ---------------------------------------------------------------------------


def _make_run(tmp_path: Path, *, cells, evidence=(), chunks=(), prompts=()) -> Path:
    run_dir = tmp_path / "run"
    (run_dir / "prompts").mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(run_dir / "cohound.db")
    conn.executescript(
        """
        create table cells (product_id text, param_id text, state text, status text);
        create table cell_evidence (
            product_id text, param_id text, citation_quote text
        );
        create table corpus_chunks (chunk_id text, text text, product_id text);
        """
    )
    conn.executemany("insert into cells values (?,?,?,?)", cells)
    conn.executemany("insert into cell_evidence values (?,?,?)", evidence)
    conn.executemany("insert into corpus_chunks values (?,?,null)", chunks)
    conn.commit()
    conn.close()
    for name, payload in prompts:
        (run_dir / "prompts" / f"{name}.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
    return run_dir


def test_read_arm_marks_a_cell_quoted_only_on_a_non_empty_quote(tmp_path: Path) -> None:
    run_dir = _make_run(
        tmp_path,
        cells=[
            ("p", "a", "evaluated", "✅"),
            ("p", "b", "evaluated", "❓"),
            ("p", "c", "failed", None),
        ],
        evidence=[("p", "a", "цитата"), ("p", "b", "   "), ("p", "c", None)],
    )
    arm = measure.read_arm(run_dir / "cohound.db")
    assert arm[("p", "a")].has_quote is True
    assert arm[("p", "b")].has_quote is False  # пробельная цитата — не цитата
    assert arm[("p", "c")].status is None
    assert arm[("p", "c")].present is False


def test_read_prompts_accepts_list_and_literal_string_cell_keys(tmp_path: Path) -> None:
    run_dir = _make_run(
        tmp_path,
        cells=[],
        prompts=[
            (
                "a",
                {"cell_key": ["p", "q"], "chunk_ids": ["c1"], "template_version": "v"},
            ),
            ("b", {"cell_key": "['p', 'r']", "chunk_ids": [], "template_version": "v"}),
        ],
    )
    prompts = measure.read_prompts(run_dir)
    assert prompts[("p", "q")] == ("c1",)
    assert prompts[("p", "r")] == ()


def test_chunk_text_hashes_only_returns_requested_addresses(tmp_path: Path) -> None:
    run_dir = _make_run(
        tmp_path,
        cells=[],
        chunks=[("c1", "текст"), ("c2", "другой")],
    )
    hashes = measure.chunk_text_hashes(run_dir / "cohound.db", {"c1"})
    assert set(hashes) == {"c1"}
    assert re.fullmatch(r"[0-9a-f]{64}", hashes["c1"])


def test_corpus_diff_reports_both_arms_per_product(tmp_path: Path) -> None:
    a = _make_run(
        tmp_path / "a",
        cells=[],
        chunks=[("c1", "t"), ("c2", "t")],
    )
    b = _make_run(tmp_path / "b", cells=[], chunks=[("c3", "t")])
    for run_dir, product in ((a, "p"), (b, "p")):
        conn = sqlite3.connect(run_dir / "cohound.db")
        conn.execute("create table corpus_docs (product_id text)")
        conn.executemany("insert into corpus_docs values (?)", [(product,)])
        conn.execute("update corpus_chunks set product_id = ?", (product,))
        conn.commit()
        conn.close()
    rows = measure.corpus_diff(a / "cohound.db", b / "cohound.db")
    assert rows == [
        {"product_id": "p", "docs_a": 1, "docs_b": 1, "chunks_a": 2, "chunks_b": 1}
    ]
