"""Тесты прибора «токенный лимит против адреса эталона» (``scripts/measure_token_limit_retrieval.py``).

Прибор отвечает на условие пересмотра O21 («``hit@10`` на эталоне покажет различие при
включённом лимите»), и цена ошибки — вердикт «ось не влияет», полученный прибором, который
на самом деле ничего не сравнил. Поэтому проверяются обе половины: что разрез **находится**
(``_limited``), что тронутое доказательство **считается** (``_touch``), и что смена порядка
при сохранённом попадании **видна** (``_rank_changes``) — иначе «hit@10 не изменился» читалось
бы как «выдача не изменилась».
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_token_limit_retrieval as tl  # noqa: E402

from core.ports.types import Chunk  # noqa: E402


class _CharCounter:
    """Счётчик, где один символ = один токен.

    Не заглушка ради удобства: прибор принимает **порт** счётчика, и подмена «символ =
    токен» делает границу лимита проверяемой глазами (лимит 20 — это 20 символов), тогда
    как настоящий токенизатор в тесте давал бы нечитаемые смещения.
    """

    spec = "test/char"

    def token_ends(self, text: str) -> list[int]:
        return list(range(1, len(text) + 1))


def _chunk(chunk_id: str, text: str, start: int = 0) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        chunk_no=0,
        text=text,
        heading_path="# Раздел",
        context_prefix="",
        content_hash=f"h{chunk_id}",
        char_start=start,
        char_end=start + len(text),
    )


def _limiter(limit: int, *, min_tail_tokens: int = 0):
    """Ограничитель на символьном счётчике.

    ``min_tail_tokens`` по умолчанию **0**, а не боевые 64: тест про разрез, а порог хвоста
    при бюджете 20 приклеил бы весь остаток к первому куску (``50 − 20 = 30 < 64``) и разрез
    не состоялся бы. Боевое поведение порога проверяется отдельным тестом ниже — чтобы
    «прибор не нашёл разрез» нельзя было списать на настройку теста.
    """
    from core.adapters.token_limit import TokenLimiter

    return TokenLimiter(_CharCounter(), limit, min_tail_tokens=min_tail_tokens)


# --- _limited: разрез находится, неизменённый кусок не попадает в тронутые --------


def test_limited_splits_long_chunk_and_marks_parent_touched() -> None:
    long_text = "а" * 50
    records = [("p:d1:0", "p:d1", _chunk("p:d1:0", long_text))]
    out, totals, changed, _ = tl._limited(records, _limiter(20))

    assert len(out) > 1
    assert totals["split_chunks"] == 1
    assert changed == {"p:d1:0"}
    # Куски-выходы адресуются префиксом родителя: карта «кусок → документ» строится по нему.
    assert all(chunk_id.startswith("p:d1:0#") for chunk_id, _, _ in out)


def test_limited_default_tail_threshold_absorbs_and_says_so() -> None:
    """Боевой порог хвоста (64) при малом бюджете не режет — и перебор виден числом.

    Это не побочный эффект, а объявленная политика: обрывок короче 64 токенов приклеивается,
    кусок выходит за бюджет, и ``over_limit`` обязан это показать. Прибор, который счёл бы
    такой чанк «разрезанным», объявил бы лимит соблюдённым там, где он не соблюдён.
    """
    from core.adapters.token_limit import MIN_TAIL_TOKENS

    records = [("p:d1:0", "p:d1", _chunk("p:d1:0", "а" * 50))]
    out, totals, changed, _ = tl._limited(
        records, _limiter(20, min_tail_tokens=MIN_TAIL_TOKENS)
    )

    assert len(out) == 1
    assert totals["split_chunks"] == 0
    assert totals["over_limit"] == 1
    # Текст родителя не изменился — значит и «тронутым» он быть не должен.
    assert changed == set()


def test_limited_keeps_short_chunk_and_reports_nothing_touched() -> None:
    records = [("p:d1:0", "p:d1", _chunk("p:d1:0", "короткий кусок"))]
    out, totals, changed, _ = tl._limited(records, _limiter(100))

    assert len(out) == 1
    assert totals["split_chunks"] == 0
    assert changed == set()


def test_limited_applies_per_document_not_across_documents() -> None:
    """Ограничитель не склеивает куски разных документов.

    Это не педантизм: ``TokenLimiter.apply`` приклеивает хвост к предыдущему куску, и на
    сквозном списке он приклеил бы хвост одного документа к другому — прибор измерил бы
    корпус, которого в пайплайне не бывает.
    """
    records = [
        ("p:d1:0", "p:d1", _chunk("p:d1:0", "а" * 18)),
        ("p:d2:0", "p:d2", _chunk("p:d2:0", "б" * 18, start=0)),
    ]
    out, _, _, _ = tl._limited(records, _limiter(20))

    by_doc = {doc_id for _, doc_id, _ in out}
    assert by_doc == {"p:d1", "p:d2"}
    # Каждый кусок-выход несёт текст ровно одного документа.
    for chunk_id, doc_id, chunk in out:
        assert chunk_id.startswith(doc_id + ":")
        assert ("б" in chunk.text) is (doc_id == "p:d2")


# --- _touch: тронутое доказательство считается по ожидаемым документам ----------


class _Entry:
    def __init__(self, cell: str, docs: list[str]) -> None:
        self.cell = cell
        self.expected_doc_ids = docs


def test_touch_counts_only_expected_documents() -> None:
    entries = [_Entry("p/x", ["p:d1"]), _Entry("p/y", ["p:d2"])]
    doc_chunks = {"p:d1": ["p:d1:0", "p:d1:1"], "p:d2": ["p:d2:0"]}
    changed = {"p:d1:0"}

    report = tl._touch(entries, doc_chunks, changed)

    assert report["cells"] == 2
    assert report["expected_chunks"] == 3
    assert report["touched_chunks"] == 1
    assert report["cells_with_touched_expected_chunks"] == 1
    assert report["per_cell"] == [
        {"cell": "p/x", "expected_chunks": 2, "touched_chunks": 1},
        {"cell": "p/y", "expected_chunks": 1, "touched_chunks": 0},
    ]


def test_touch_ignores_documents_outside_the_golden() -> None:
    """Тронутый кусок чужого документа не делает ячейку «задетой»."""
    entries = [_Entry("p/x", ["p:d1"])]
    doc_chunks = {"p:d1": ["p:d1:0"]}
    report = tl._touch(entries, doc_chunks, {"p:other:0"})

    assert report["touched_chunks"] == 0
    assert report["cells_with_touched_expected_chunks"] == 0


# --- _rank_changes: смена порядка при сохранённом попадании ---------------------


def test_rank_changes_sees_order_change_with_preserved_hit() -> None:
    """Попадание сохранено (rank 3 → 7), но выдача изменилась — и это видно числом."""
    baseline = [
        {"cell": "a", "rank": 3, "unresolved": False},
        {"cell": "b", "rank": 1, "unresolved": False},
    ]
    limited = [
        {"cell": "a", "rank": 7, "unresolved": False},
        {"cell": "b", "rank": 1, "unresolved": False},
    ]
    report = tl._rank_changes(baseline, limited)

    assert report["cells"] == 2
    assert report["rank_changed"] == 1
    assert report["rank_changed_hit_preserved"] == 1
    assert report["examples"] == [{"cell": "a", "baseline": 3, "limited": 7}]


def test_rank_changes_separates_lost_hit_from_order_change() -> None:
    baseline = [{"cell": "a", "rank": 2, "unresolved": False}]
    limited = [{"cell": "a", "rank": None, "unresolved": False}]
    report = tl._rank_changes(baseline, limited)

    assert report["rank_changed"] == 1
    # Потеря попадания — не «смена порядка»: это разные выводы, и склеивать их нельзя.
    assert report["rank_changed_hit_preserved"] == 0


def test_rank_changes_reports_nothing_when_identical() -> None:
    cells = [{"cell": "a", "rank": 1, "unresolved": False}]
    report = tl._rank_changes(cells, list(cells))

    assert report["rank_changed"] == 0
    assert report["examples"] == []


# --- отказы --------------------------------------------------------------------


def test_split_model_without_revision_declares_none() -> None:
    assert tl._split_model("BAAI/bge-m3") == ("BAAI/bge-m3", None)
    assert tl._split_model("BAAI/bge-m3@abc") == ("BAAI/bge-m3", "abc")


def test_missing_db_is_refused_with_exit_code(tmp_path: Path, capsys) -> None:
    code = tl.main(
        [
            "--db",
            str(tmp_path / "нет.db"),
            "--golden",
            str(tmp_path / "нет.json"),
        ]
    )
    assert code == 1
    assert "ОШИБКА" in capsys.readouterr().err


def test_nonpositive_limit_is_refused(tmp_path: Path, capsys) -> None:
    code = tl.main(
        [
            "--db",
            str(tmp_path / "нет.db"),
            "--golden",
            str(tmp_path / "нет.json"),
            "--token-limit",
            "0",
        ]
    )
    assert code == 1
    assert "положительным" in capsys.readouterr().err


def test_missing_inputs_snapshot_is_named(tmp_path: Path) -> None:
    """Без снимка входов запросы не рендерятся — прибор отказывает, а не угадывает."""
    db_path = tmp_path / "cohound.db"
    db_path.write_bytes(b"")
    with pytest.raises(SystemExit) as excinfo:
        tl._load_params(db_path)
    assert "plan_inputs.json" in str(excinfo.value)
