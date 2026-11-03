"""Пины замерных функций ``scripts/measure_md_chunking.py`` (без БД, без файлов, без сети).

Проверяются ровно те вычисления, на которых держатся публикуемые числа, — на синтетике,
где ответ известен заранее:

* перцентили (линейная интерполяция) и гистограмма длин;
* класс письменности ``ZH``/``LAT`` по доле CJK среди непробельных символов;
* скелет чанка (посылка P4b: пробелы, повторы знаков, числа);
* отказ при недоступном токенизаторе — **без** символьного прокси: подмена единицы
  измерения молча сделала бы отчёт ложью, поэтому проверяется именно исключение;
* согласование модели замера с ``runs.embed_model_id`` (мерить чужой моделью нельзя).

``transformers`` тестам не нужен: модуль импортирует его лениво, а здесь подменяется
функция импорта. Если пакета нет в окружении, тесты всё равно проходят.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "measure_md_chunking.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_measure_md_chunking", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MEASURE = _load_harness()


# -- перцентили и гистограмма ------------------------------------------------


def test_percentile_linear_interpolation() -> None:
    """p50/p90 считаются интерполяцией по позиции ``q/100 * (n-1)``, а не «соседним»."""
    ordered = [1, 2, 3, 4]
    assert MEASURE._percentile(ordered, 50) == pytest.approx(2.5)  # type: ignore[attr-defined]
    assert MEASURE._percentile(ordered, 90) == pytest.approx(3.7)  # type: ignore[attr-defined]
    assert MEASURE._percentile(ordered, 0) == pytest.approx(1.0)  # type: ignore[attr-defined]
    assert MEASURE._percentile(ordered, 100) == pytest.approx(4.0)  # type: ignore[attr-defined]


def test_percentile_single_value_and_empty() -> None:
    """Ряд из одного значения определён; пустой ряд — ошибка, а не ноль."""
    assert MEASURE._percentile([7], 90) == pytest.approx(7.0)  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        MEASURE._percentile([], 50)  # type: ignore[attr-defined]


def test_distribution_and_histogram_bins() -> None:
    """``max`` — наблюдённое значение, границы гистограммы полуоткрытые."""
    values = [10, 63, 64, 512, 5000]
    distribution = MEASURE._distribution(values)  # type: ignore[attr-defined]
    assert distribution["n"] == 5
    assert distribution["max"] == 5000
    histogram = MEASURE._histogram(values)  # type: ignore[attr-defined]
    assert histogram["<64"] == 2  # 10 и 63
    assert histogram["64–128"] == 1
    assert histogram["512–1024"] == 1
    assert histogram[">4096"] == 1


# -- класс письменности ------------------------------------------------------


def test_script_class_zh_and_lat() -> None:
    """Класс — доля CJK среди непробельных: ≥0.20 → ZH, иначе LAT."""
    assert MEASURE._script_class("数据库管理系统") == "ZH"  # type: ignore[attr-defined]
    assert MEASURE._script_class("База данных PostgreSQL") == "LAT"  # type: ignore[attr-defined]


def test_script_class_threshold_is_inclusive_and_ignores_spaces() -> None:
    """Порог включающий (ровно 0.20 → ZH); пробелы в знаменатель не входят."""
    assert MEASURE._script_class("数 a b c d") == "ZH"  # type: ignore[attr-defined]
    assert MEASURE._script_class("数 a b c d e") == "LAT"  # type: ignore[attr-defined]
    assert MEASURE._cjk_share("   ") == 0.0  # type: ignore[attr-defined]


# -- скелет ------------------------------------------------------------------


def test_skeleton_collapses_spaces_repeats_and_numbers() -> None:
    """Скелет: ``\\s+`` → пробел, повторы знаков → один, числа → ``0`` (как в d2c.py)."""
    assert MEASURE._skeleton("|  +++  |\n\n 42 Листов") == "| + | 0 Листов"  # type: ignore[attr-defined]


def test_skeleton_makes_forms_of_different_width_one_class() -> None:
    """Форма таблицы любой ширины — один скелет, строка матрицы — другой."""
    narrow = MEASURE._skeleton("| + | + |")  # type: ignore[attr-defined]
    wide = MEASURE._skeleton("|   +   |   +   |")  # type: ignore[attr-defined]
    matrix = MEASURE._skeleton("| ja_anonymizer | маскирование данных |")  # type: ignore[attr-defined]
    assert narrow == wide
    assert matrix != narrow


def test_skeleton_stats_groups() -> None:
    """Доли групп ≥2/≥3 считаются по чанкам и по символам, а не по числу скелетов."""
    form = "| + | + |"
    matrix = "| ja_anonymizer | маскирование данных |"
    stats = MEASURE.SkeletonStats()  # type: ignore[attr-defined]
    for _ in range(3):
        stats.add(form)
    stats.add(matrix)
    summary = stats.summary()
    assert summary["chunks"] == 4
    assert summary["unique_skeletons"] == 2
    assert summary["groups_ge2"]["chunks"] == 3
    assert summary["groups_ge3"]["chunks"] == 3
    assert summary["groups_ge2"]["chunk_share"] == pytest.approx(0.75)
    assert summary["groups_ge2"]["char_share"] == pytest.approx(
        3 * len(form) / (3 * len(form) + len(matrix))
    )
    assert summary["top"][0]["skeleton"] == form
    assert summary["top"][0]["count"] == 3


# -- токенизатор: отказ вместо прокси ----------------------------------------


def test_missing_transformers_raises_instead_of_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Нет ``transformers`` — внятная ошибка; символьная подмена не появляется."""

    def _boom() -> object:
        raise ImportError("No module named 'transformers'")

    monkeypatch.setattr(MEASURE, "_import_auto_tokenizer", _boom)
    with pytest.raises(MEASURE.TokenizerUnavailable) as excinfo:  # type: ignore[attr-defined]
        MEASURE._load_tokenizer("BAAI/bge-m3")  # type: ignore[attr-defined]
    message = str(excinfo.value)
    assert "transformers" in message
    assert "прокси" in message


def test_unloadable_tokenizer_model_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Снапшот модели не загрузился — отказ, а не «ноль токенов»."""

    class _Factory:
        @staticmethod
        def from_pretrained(model_id: str, *, local_files_only: bool) -> object:
            raise OSError(f"нет снапшота {model_id} (local_files_only={local_files_only})")

    monkeypatch.setattr(MEASURE, "_import_auto_tokenizer", lambda: _Factory)
    with pytest.raises(MEASURE.TokenizerUnavailable):  # type: ignore[attr-defined]
        MEASURE._load_tokenizer("BAAI/bge-m3")  # type: ignore[attr-defined]


def test_count_tokens_uses_tokenizer_ids() -> None:
    """Счёт идёт по ``input_ids`` самого токенизатора, а не по длине строки."""

    class _FakeTokenizer:
        def __call__(
            self, text: str, *, add_special_tokens: bool, truncation: bool
        ) -> dict[str, list[int]]:
            assert add_special_tokens is True
            assert truncation is False
            return {"input_ids": list(range(len(text.split()) + 2))}

    tokenizer = _FakeTokenizer()
    assert MEASURE._count_tokens(tokenizer, "два слова") == 4  # type: ignore[attr-defined]
    assert MEASURE._count_tokens(tokenizer, "") == 2  # type: ignore[attr-defined]


# -- модель замера против манифеста ------------------------------------------


def test_tokenizer_model_mismatch_is_refused() -> None:
    """Чужая модель манифеста — отказ: токены другой модели не описывают этот корпус."""
    with pytest.raises(MEASURE.TokenizerModelMismatch):  # type: ignore[attr-defined]
        MEASURE._resolve_tokenizer_model("other/model", "BAAI/bge-m3")  # type: ignore[attr-defined]


def test_tokenizer_model_revision_pin_is_allowed() -> None:
    """Пин ревизии того же словаря допустим: sha256 файла публикуется в провенансе."""
    resolution = MEASURE._resolve_tokenizer_model(  # type: ignore[attr-defined]
        "BAAI/bge-m3@5617a9f", "BAAI/bge-m3"
    )
    assert resolution["assumed"] is False
    assert resolution["revision_pinned"] is True


def test_tokenizer_model_absent_in_manifest_is_an_assumption() -> None:
    """NULL в манифесте — допущение замера, и оно помечается, а не выдаётся за факт."""
    resolution = MEASURE._resolve_tokenizer_model(None, "BAAI/bge-m3")  # type: ignore[attr-defined]
    assert resolution["assumed"] is True
    assert resolution["declared_embed_model_id"] is None


# -- перепись: правила «кандидат» --------------------------------------------


def test_grid_table_needs_two_frame_lines() -> None:
    """Одна строка рамки — не кандидат: правило требует ≥2 строк."""
    one = "text\n+---+---+\ntext\n"
    two = "text\n+---+---+\n| a | b |\n+---+---+\n"
    assert MEASURE._census_hits("grid_table", one) == []  # type: ignore[attr-defined]
    assert len(MEASURE._census_hits("grid_table", two)) == 2  # type: ignore[attr-defined]


def test_line_block_needs_two_consecutive_lines() -> None:
    """line block — две подряд строки с ``| `` и ровно одной чертой."""
    single = "| одна строка\nтекст\n"
    pair = "| первая\n| вторая\n"
    assert MEASURE._census_hits("line_block", single) == []  # type: ignore[attr-defined]
    assert len(MEASURE._census_hits("line_block", pair)) == 2  # type: ignore[attr-defined]

# -- P14: смещения чанка срезают ровно свой текст (L1b-2b) --------------------


def _chunk(text: str, *, start: int | None = None, end: int | None = None) -> object:
    from core.ports.types import Chunk

    return Chunk(
        chunk_id=f"d#{start}",
        chunk_no=0,
        text=text,
        heading_path="",
        context_prefix="",
        content_hash="0" * 16,
        char_start=start,
        char_end=end,
    )


def test_span_check_accepts_exact_slice_including_trailing_newlines() -> None:
    """Срез с краевыми переводами строки — точный: P1 их не срезает, и P14 тоже.

    Это тот случай, из-за которого `CorpusChunk` перестал стрипать текст: ``\n\n``
    принадлежат срезу, и без них длина диапазона не сошлась бы с ``len(text)``.
    """
    document = "первый абзац\n\nвторой абзац\n\n"
    violations, without = MEASURE._span_violations(
        document, [_chunk("первый абзац\n\n", start=0, end=14)]
    )

    assert (violations, without) == (0, 0)


def test_span_check_counts_mismatched_slice_as_violation() -> None:
    """Смещения, которые не срезают свой текст, — нарушение, а не «почти верно»."""
    document = "первый абзац\n\nвторой абзац\n\n"
    violations, without = MEASURE._span_violations(
        document, [_chunk("второй абзац", start=0, end=14)]
    )

    assert (violations, without) == (1, 0)


def test_span_check_separates_chunks_without_offsets() -> None:
    """Чанк без смещений — не нарушение, но считается отдельно (абзацный чанкер)."""
    violations, without = MEASURE._span_violations("текст документа\n\n", [_chunk("текст")])

    assert (violations, without) == (0, 1)
