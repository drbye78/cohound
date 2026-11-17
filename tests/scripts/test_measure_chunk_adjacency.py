"""Тесты прибора семантической смежности (`scripts/measure_chunk_adjacency.py`, O9 плана 26).

Прибор отвечает на вопрос «сколько нарезка теряет на живом корпусе», и ломается он тихо:
счётчик получается правдоподобным при любом из трёх дефектов ниже, поэтому каждый проверяется
отдельно.

* **детектор ловит не то** — поэтому на каждый из четырёх детекторов взят положительный и
  отрицательный случай **из фикстур** `tests/adapters/test_chunker_semantics.py`: прибор обязан
  сработать там, где фикстура утверждает нарушение, и промолчать на том же тексте без разрыва.
  Это связывает прибор с находкой, а не с пересказом её в докстринге.
* **порядок чанков взят из БД, а не из `chunk_no`** — поэтому в тестовой БД строки кладутся
  **в обратном порядке**, и прибор обязан дать те же пары. Без этой проверки дефект невидим:
  `SELECT … ORDER BY doc_id, chunk_no` маскирует ошибку, пока индекс есть, и обнажает, когда
  его нет.
* **пустой документ и документ из одного чанка ломают проход** — `adjacent_pairs` на пустом
  списке и пара «чанк с пустым текстом + следующий» проверяются прямо: пустая строка — это
  чанк без строк, а не отсутствие чанка, и `nonempty_lines` обязан вернуть пустой список, а не
  упасть на индексе `[-1]`.

Доли проверяются вместе с `None` на нулевом знаменателе: «не измерено» и «ноль» — разные
факты, и прибор обязан их различать.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_chunk_adjacency.py"
    spec = importlib.util.spec_from_file_location("measure_chunk_adjacency", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_chunk_adjacency"] = module
    spec.loader.exec_module(module)
    return module


measure = _load_module()
ChunkRow = measure.ChunkRow


def _row(text: str, chunk_no: int, *, doc_id: str = "doc", context: str = "") -> ChunkRow:
    """Чанк для детектора: адрес, текст и `context_prefix`."""
    return ChunkRow(doc_id=doc_id, chunk_no=chunk_no, text=text, context_prefix=context)


# --------------------------------------------------------------------------- #
# D1. Вводная строка списка (фикстура: test_list_intro_line_stays_with_its_items)
# --------------------------------------------------------------------------- #


def test_d1_fires_when_list_intro_is_cut_off_from_its_items() -> None:
    """Положительный случай: «Возможности:» в хвосте, элементы — в следующем чанке."""
    prev = _row("Продукт предоставляет набор функций.\n\nВозможности:", 0)
    cur = _row("- резервное копирование;\n- потоковая репликация;", 1)
    hit = measure.detect_list_intro(prev, cur)
    assert hit is not None
    assert hit.flags["intro_is_list_item"] is False


def test_d1_silent_when_intro_and_items_are_in_one_chunk() -> None:
    """Отрицательный случай: тот же текст, но разрыва нет — срабатывания быть не должно."""
    prev = _row("Возможности:\n\n- резервное копирование;\n- потоковая репликация;", 0)
    cur = _row("Следующий раздел документа.", 1)
    assert measure.detect_list_intro(prev, cur) is None


def test_d1_names_its_own_false_positive_classes() -> None:
    """Уточнения: элемент списка с `:` в конце и `:` внутри ограды кода считаются отдельно."""
    item = measure.detect_list_intro(_row("- Параметр:", 0), _row("- значение", 1))
    assert item is not None
    assert item.flags["intro_is_list_item"] is True
    fenced = measure.detect_list_intro(
        _row("```yaml\nkey:", 0), _row("- value", 1)
    )
    assert fenced is not None
    assert fenced.flags["intro_in_fence"] is True


# --------------------------------------------------------------------------- #
# D2. Висячий заголовок (фикстура: test_limitations_block_stays_with_its_section_statement)
# --------------------------------------------------------------------------- #


def test_d2_fires_when_chunk_ends_with_a_heading() -> None:
    """Положительный случай: заголовок в хвосте, его тело — в следующем чанке."""
    prev = _row("Продукт хранит данные в кластере из нескольких узлов.\n\nОграничения", 0)
    cur = _row("- только Linux;\n- не более 64 ядер;", 1)
    hit = measure.detect_dangling_heading(prev, cur)
    assert hit is not None
    assert hit.flags["silent"] is True


def test_d2_silent_when_the_chunk_ends_with_a_sentence() -> None:
    """Отрицательный случай: чанк кончается утверждением, а не заголовком."""
    prev = _row("Продукт хранит данные в кластере из нескольких узлов.", 0)
    cur = _row("- только Linux;", 1)
    assert measure.detect_dangling_heading(prev, cur) is None


def test_d2_marks_heading_recovered_from_context_prefix() -> None:
    """Уточнение `named_in_next_context`: связь не потеряна, если заголовок виден в контексте."""
    prev = _row("Продукт хранит данные.\n\nОграничения", 0)
    cur = _row("- только Linux;", 1, context="Хранилище > Ограничения")
    hit = measure.detect_dangling_heading(prev, cur)
    assert hit is not None
    assert hit.flags["named_in_next_context"] is True
    assert hit.flags["silent"] is False


def test_d2_ignores_heading_only_chunk() -> None:
    """Заголовок, составляющий чанк целиком, — не «висячий хвост»: до него нет содержания."""
    assert measure.detect_dangling_heading(_row("## Синтаксис", 0), _row("текст", 1)) is None


def test_d2_separates_atx_branch_from_the_loose_one() -> None:
    """ATX-заголовок в хвосте — дефект (`primary`); короткая строка без точки — вторая ветка.

    Разделение обязательно: на живом корпусе рыхлая ветка ловит код и определения, и если
    сложить её с ATX, число свойства вырастет в шестьдесят раз без всякого дефекта.
    """
    atx = measure.detect_dangling_heading(_row("Тело раздела.\n\n## Описание", 0), _row("текст", 1))
    assert atx is not None
    assert atx.primary is True
    assert atx.flags["atx"] is True

    loose = measure.detect_dangling_heading(
        _row("CREATE TABLE data(id serial primary key, data text);\n\nCREATE TABLE", 0),
        _row("postgres=# -- DDL не реплицируется", 1),
    )
    assert loose is not None
    assert loose.primary is False
    assert loose.flags["loose"] is True


# --------------------------------------------------------------------------- #
# D3. Примечание без объекта (фикстура: test_note_keeps_its_own_object)
# --------------------------------------------------------------------------- #


def test_d3_fires_when_note_opens_a_chunk() -> None:
    """Положительный случай: примечание в голове чанка, до него — чужой объект."""
    prev = _row("Объект А поддерживает потоковую репликацию.", 0)
    cur = _row(
        "> **Примечание.** Относится только к объекту А.\n\nОбъект Б работает в одиночном режиме.",
        1,
    )
    hit = measure.detect_note_without_object(prev, cur)
    assert hit is not None
    assert hit.flags["followed_by_other_text"] is True


def test_d3_silent_when_note_stays_with_its_object() -> None:
    """Отрицательный случай: примечание внутри чанка со своим объектом."""
    prev = _row("Объект А поддерживает репликацию.\n\n> **Примечание.** Только для А.", 0)
    cur = _row("Объект Б работает в одиночном режиме.", 1)
    assert measure.detect_note_without_object(prev, cur) is None


def test_d3_silent_for_first_chunk_and_word_boundary() -> None:
    """Первый чанк объекта не имеет вовсе; `Notebook` — не примечание."""
    assert measure.detect_note_without_object(_row("текст", 0), _row("Note: важно", 0)) is None
    assert measure.is_note_start("Note: важно") is True
    assert measure.is_note_start("Notebook для инженера") is False


# --------------------------------------------------------------------------- #
# D4. Отрицание — прокси, класс не измерен
# --------------------------------------------------------------------------- #


def test_d4_fires_only_on_a_negation_particle_in_the_tail() -> None:
    """Положительный случай: разрез между частицей и её глаголом (самая грубая форма)."""
    prev = _row("продукт проверен на платформах и в разных сетевых условиях не", 0)
    cur = _row("поддерживает потоковую репликацию в этой конфигурации.", 1)
    hit = measure.detect_dangling_negation(prev, cur)
    assert hit is not None
    assert hit.flags["block_boundary"] is False


def test_d4_silent_when_negation_travels_with_its_verb() -> None:
    """Отрицательный случай: «не поддерживает» целиком в одном чанке — прокси молчит.

    Это ровно то, что прокси **не** видит: класс из фикстуры (отрицание в `i`, предмет в
    `i+1`) здесь не детектируется, и число по D4 не выдаётся за его размер.
    """
    prev = _row("продукт не поддерживает потоковую репликацию", 0)
    cur = _row("в этой конфигурации, и это важно учитывать.", 1)
    assert measure.detect_dangling_negation(prev, cur) is None


def test_sentence_internal_cut_is_the_upper_bound_of_the_d4_class() -> None:
    """Разрез внутри предложения считается отдельно и назван верхней границей, не нарушением."""
    prev = _row("продукт проверен на платформах и в разных сетевых условиях", 0)
    cur = _row("не поддерживает потоковую репликацию.", 1)
    assert measure.is_sentence_internal_cut(prev, cur) is True
    ended = _row("продукт проверен на платформах.", 0)
    assert measure.is_sentence_internal_cut(ended, cur) is False


# --------------------------------------------------------------------------- #
# Проход по корпусу: порядок, пустой документ, один чанк
# --------------------------------------------------------------------------- #


def _fake_db(rows: list[tuple[str, int, str, str | None]]) -> sqlite3.Connection:
    """БД с одной таблицей `corpus_chunks`: ровно те колонки, которые читает прибор."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE corpus_chunks (doc_id TEXT, chunk_no INTEGER, text TEXT, context_prefix TEXT)"
    )
    conn.executemany("INSERT INTO corpus_chunks VALUES (?, ?, ?, ?)", rows)
    return conn


def test_pairs_follow_chunk_no_not_row_order() -> None:
    """Порядок берётся из `chunk_no`: строки БД нарочно лежат в обратном порядке."""
    conn = _fake_db(
        [
            ("doc-a", 1, "- резервное копирование;", None),
            ("doc-a", 0, "Возможности:", None),
        ]
    )
    documents = list(measure.iter_documents(conn))
    assert [chunk.chunk_no for chunk in documents[0]] == [0, 1]
    pairs = list(measure.adjacent_pairs(documents[0]))
    assert [(prev.chunk_no, cur.chunk_no) for prev, cur in pairs] == [(0, 1)]
    assert measure.detect_list_intro(*pairs[0]) is not None
    conn.close()


def test_empty_and_single_chunk_documents_do_not_break_the_pass() -> None:
    """Документ из одного чанка пар не даёт; чанк с пустым текстом не роняет детекторы."""
    conn = _fake_db(
        [
            ("doc-one", 0, "единственный чанк без соседей.", None),
            ("doc-empty", 0, "", None),
            ("doc-empty", 1, "", None),
            ("doc-pair", 0, "Возможности:", None),
            ("doc-pair", 1, "- резервное копирование;", None),
        ]
    )
    summary = measure.measure(conn, examples=2)
    conn.close()

    assert summary["corpus"]["documents"] == 3
    assert summary["corpus"]["chunks"] == 5
    # пары: doc-empty (0→1) и doc-pair (0→1); doc-one пар не даёт
    assert summary["corpus"]["pairs"] == 2
    assert summary["corpus"]["documents_with_pairs"] == 2
    assert summary["detectors"]["D1"]["pairs"] == 1
    assert summary["detectors"]["D1"]["chunks_to_rechunk"] == 2
    assert summary["detectors"]["D1"]["documents"] == 1
    # пустой документ ничего не сработало, но и не упал
    assert summary["detectors"]["D2"]["pairs"] == 0
    assert summary["detectors"]["D3"]["pairs"] == 0


def test_measure_collects_examples_and_union_of_affected_chunks() -> None:
    """Примеры ограничены `examples`, объединение задетых чанков не дублирует адреса."""
    rows: list[tuple[str, int, str, str | None]] = []
    for index in range(3):
        doc = f"doc-{index}"
        rows.append((doc, 0, "Возможности:", None))
        rows.append((doc, 1, "- пункт один;", None))
        rows.append((doc, 2, "Ограничения", None))
        rows.append((doc, 3, "| a | b |", None))
    conn = _fake_db(rows)
    summary = measure.measure(conn, examples=2)
    conn.close()

    d1 = summary["detectors"]["D1"]
    assert d1["pairs"] == 3
    assert len(d1["examples"]) == 2
    assert d1["chunks_to_rechunk"] == 6
    assert d1["examples"][0]["prev_tail"] == "Возможности:"
    assert d1["examples"][0]["next_head"] == "- пункт один;"
    assert summary["affected_chunks_union"] == 6


def test_price_uses_both_rates_and_reports_full_corpus() -> None:
    """Цена считается от объединения задетых чанков; полный корпус — от всех чанков."""
    summary = {
        "corpus": {"chunks": 10_000, "documents": 1, "documents_with_pairs": 1, "pairs": 1},
        "affected_chunks_union": 1_000,
        "detectors": {},
        "sentence_internal_cuts": {},
    }
    block = measure.price(summary)
    assert block["hours_affected_ac"] == pytest.approx(1_000 / 8.16 / 3600, abs=1e-3)
    assert block["hours_affected_usb_c"] == pytest.approx(1_000 / 6.85 / 3600, abs=1e-3)
    assert block["hours_full_corpus_ac"] > block["hours_affected_ac"]
    assert block["affected_share"] == pytest.approx(0.1)


def test_share_is_none_for_empty_denominator() -> None:
    """«Не измерено» и «ноль» — разные факты: пустой знаменатель даёт `None`, не `0.0`."""
    assert measure._share(0, 0) is None
    assert measure._share(0, 5) == 0.0
