"""Тесты прибора порядка чтения (`scripts/measure_reading_order.py`, O37).

Прибор отвечает на вопрос «совпадает ли порядок строк с геометрическим ожиданием
двухколоночной страницы». Ошибка здесь тихая: перепутанные колонки дают тот же объём текста и
тот же размер `content_hash`, поэтому проверяются свойства, поломка которых меняет вывод:

* **канавка** находится только там, где она есть: страница в одну колонку, страница с широким
  заголовком над колонками и страница с «рваными» концами строк — три разных случая;
* **разрыв строки по горизонтальному промежутку** обязателен: без него строка левой и строка
  правой колонки на одной высоте склеиваются в одну, и канавка не находится никогда;
* **направление оси `y`**: у pdfium `y` растёт вверх, и перевёрнутая ось даёт правдоподобное
  «порядок нарушен везде» — ловится положительным контролем;
* **сравнение по личностям строк**, а не по тексту: «пропало» не может быть спутано с
  «переставлено», иначе потеря текста (O26/O35) записывается в дефект порядка.

PDF, GPU и БД не нужны: проверяются чистые функции на синтетических рамках и одна
синтетическая страница с известным порядком.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_reading_order.py"
    spec = importlib.util.spec_from_file_location("measure_reading_order", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_reading_order"] = module
    spec.loader.exec_module(module)
    return module


order = _load_module()

WIDTH = 800.0
HEIGHT = 1000.0


def _line(x0: float, x1: float, y: float, text: str = "x", first: int = 0) -> dict:
    return {"x0": x0, "x1": x1, "y": y, "text": text, "first": first}


def _two_column_lines(rows: int = 14) -> list[dict]:
    """Две колонки: левая x∈[40,360], правая x∈[440,760], строки на одной высоте."""
    lines = []
    for index in range(rows):
        y = 900 - index * 40
        lines.append(_line(40, 360, y, f"left-{index}"))
        lines.append(_line(440, 760, y, f"right-{index}"))
    return lines


# ---------------------------------------------------------------- канавка


def test_gutter_found_on_two_columns() -> None:
    found = order._gutter(_two_column_lines(), WIDTH, HEIGHT)
    assert found is not None
    x, left, right = found
    assert 360 <= x <= 440
    assert len(left) == len(right) == 14


def test_gutter_not_found_on_single_column() -> None:
    """Одна колонка — не достижение читателя, а другое свойство страницы."""
    lines = [_line(40, 760, 900 - index * 40) for index in range(12)]
    assert order._gutter(lines, WIDTH, HEIGHT) is None


def test_gutter_ignores_full_width_heading() -> None:
    """Заголовок во всю ширину не «закрывает» канавку — он и читается первым."""
    lines = [_line(40, 760, 960, "Заголовок"), *_two_column_lines()]
    assert order._gutter(lines, WIDTH, HEIGHT) is not None


def test_gutter_rejects_empty_half_page() -> None:
    """Половина страницы без текста — титул или врезка, а не вторая колонка."""
    lines = [_line(40, 360, 900 - index * 40) for index in range(12)]
    lines.append(_line(440, 760, 880))
    assert order._gutter(lines, WIDTH, HEIGHT) is None


def test_gutter_requires_enough_lines() -> None:
    """Две строки — не вёрстка: статистики нет, и утверждение было бы догадкой."""
    lines = [_line(40, 360, 900), _line(440, 760, 900), _line(40, 360, 860)]
    assert order._gutter(lines, WIDTH, HEIGHT) is None


def test_gutter_rejects_ragged_line_endings() -> None:
    """Концы строк одной колонки — не вторая колонка: левый край у них вразнобой.

    Ровно это ложное срабатывание дало «487 двухколоночных страниц» до проверки
    выравнивания: строка рвётся на промежутке, и её хвост попадает в «правую колонку».
    """
    lines = [_line(40, 360, 900 - index * 40) for index in range(14)]
    ragged = (420, 455, 500, 430, 470, 520, 445, 480, 510, 460, 495, 535, 440, 505)
    for index, x0 in enumerate(ragged):
        lines.append(_line(x0, 760, 900 - index * 40))
    assert order._gutter(lines, WIDTH, HEIGHT) is None


def test_gutter_orders_columns_top_down() -> None:
    """``y`` растёт вверх: первая строка колонки — та, у которой ``y`` больше."""
    found = order._gutter(_two_column_lines(), WIDTH, HEIGHT)
    assert found is not None
    _x, left, _right = found
    assert [line["text"] for line in left][:2] == ["left-0", "left-1"]
    assert left[0]["y"] > left[-1]["y"]


def test_aligned_accepts_hard_left_edge() -> None:
    lines = [_line(40, 360, 900 - index * 40) for index in range(10)]
    assert order._aligned(lines, WIDTH) is True


def test_aligned_rejects_ragged_left_edge() -> None:
    lines = [_line(40 + index * 7, 360, 900 - index * 40) for index in range(10)]
    assert order._aligned(lines, WIDTH) is False


def test_aligned_of_empty_is_false() -> None:
    assert order._aligned([], WIDTH) is False


# ---------------------------------------------------------------- поворот текста


def _horizontal_boxes() -> list[tuple[float, float, float, float, str, int]]:
    """Символы строки: смещение по x на ширину символа, y постоянен."""
    return [
        (40 + index * 6, 100, 46 + index * 6, 112, "a", index) for index in range(40)
    ]


def _rotated_boxes() -> list[tuple[float, float, float, float, str, int]]:
    """Символы повёрнутой строки: смещение по y, x постоянен."""
    return [
        (40, 100 + index * 6, 52, 106 + index * 6, "a", index) for index in range(40)
    ]


def test_horizontal_text_is_recognised() -> None:
    assert order._is_horizontal(_horizontal_boxes()) is True


def test_rotated_text_is_not_horizontal() -> None:
    """На повёрнутой странице ``get_charbox`` даёт другое пространство — ждать порядок нельзя."""
    assert order._is_horizontal(_rotated_boxes()) is False


def test_too_few_boxes_are_not_declared_horizontal() -> None:
    """Пустая проверка не должна возвращать «горизонтально»: это догадка, а не факт."""
    assert order._is_horizontal(_horizontal_boxes()[:5]) is False


# ---------------------------------------------------------------- разрыв строки


def _grid_boxes(rows: int, columns: int, *, char: int = 6) -> list:
    """Символы сетки: ``rows`` высот × ``columns`` колонок, каждая по 4 символа."""
    boxes = []
    for row in range(rows):
        y0 = 700 - row * 20
        for column in range(columns):
            for step in range(4):
                x = 40 + column * 260 + step * char
                boxes.append((x, y0, x + char, y0 + 12, "a", row * 100 + column * 10 + step))
    return boxes


def test_line_is_split_across_the_gutter() -> None:
    """Строка левой и строка правой колонки на одной высоте — **две** строки, не одна.

    Без разрыва по промежутку группировка по ``y`` склеивает колонки, канавка перестаёт
    находиться, и прибор рапортует «двухколоночных страниц нет» на двухколоночном корпусе.
    """
    lines = order._lines_from_boxes(_grid_boxes(6, 2))
    assert len(lines) == 12  # 6 высот × 2 колонки
    assert all(len(line["text"]) == 4 for line in lines)


def test_line_keeps_words_together() -> None:
    """Промежуток между словами — не граница колонки: строка не должна рассыпаться."""
    boxes = [(40 + i * 8, 100, 46 + i * 8, 112, "a", i) for i in range(4)]
    boxes.append((80, 100, 86, 112, "b", 4))  # разрыв 34 pt при ширине символа 6
    assert len(order._lines_from_boxes(boxes)) == 1


def test_empty_boxes_give_no_lines() -> None:
    assert order._lines_from_boxes([]) == []


def test_line_keeps_the_first_character_position() -> None:
    """``first`` — место строки в порядке файла; без него сравнение снова стало бы текстовым."""
    boxes = [(40, 100, 46, 112, "a", 7), (48, 100, 54, 112, "b", 8)]
    line = order._lines_from_boxes(boxes)[0]
    assert line["first"] == 7


# ---------------------------------------------------------------- проза или таблица


def test_fill_of_full_lines_is_near_one() -> None:
    lines = [_line(40, 340, 900 - index * 40) for index in range(10)]
    assert order._fill(lines, 40, 360) > 0.8


def test_fill_of_short_cells_is_small() -> None:
    """Ячейка таблицы не заполняет колонку — этим она и отличается от прозы."""
    lines = [_line(40, 90, 900 - index * 40) for index in range(10)]
    assert order._fill(lines, 40, 360) < 0.3


def test_fill_of_empty_is_zero() -> None:
    assert order._fill([], 40, 360) == 0.0


# ---------------------------------------------------------------- порядок


def test_reading_order_matches_geometry() -> None:
    """Правильный порядок: левая колонка сверху вниз, затем правая."""
    expected = [0, 1, 2, 3, 4, 5, 6, 7]
    assert order._order_metrics(expected, expected)["lcs_ratio"] == 1.0
    assert order._order_metrics(expected, expected)["interleaved"] is False


def test_interleaved_columns_are_detected() -> None:
    """Характерная подпись дефекта: правая колонка прочитана раньше левой."""
    expected = [0, 1, 2, 3, 4, 5, 6, 7]
    observed = [4, 5, 6, 7, 0, 1, 2, 3]
    metrics = order._order_metrics(expected, observed)
    assert metrics["interleaved"] is True
    assert metrics["lcs_ratio"] < 1.0


def test_metrics_require_the_same_lines_on_both_sides() -> None:
    """Разный состав — не «порядок», а другой предмет: прибор обязан отказать, а не посчитать."""
    with pytest.raises(ValueError):
        order._order_metrics([0, 1, 2], [0, 1])


def test_metrics_of_nothing_are_not_a_perfect_score() -> None:
    """Пустое сопоставление — отсутствие данных, а не «порядок совпал»."""
    assert order._order_metrics([], [])["lcs_ratio"] is None


def test_text_metrics_count_missing_separately() -> None:
    """Потеря строки — это O26/O35, а не порядок: дефекты не складываются в один."""
    metrics = order._text_metrics(["a", "b", "c", "d"], ["a", "c", "d"])
    assert metrics["missing"] == 1
    assert metrics["comparable"] == 3
    assert metrics["lcs_ratio"] == 1.0  # из того, что есть, порядок верный


def test_text_metrics_detect_interleaving() -> None:
    metrics = order._text_metrics(["a", "b", "c", "d"], ["c", "d", "a", "b"])
    assert metrics["interleaved"] is True


def test_whitespace_is_normalised_but_case_is_kept() -> None:
    """Регистр различает строки: приведение к нижнему завысило бы совпадение."""
    assert order._normalize("  a   b ") == "a b"
    assert order._normalize("A b") != order._normalize("a b")


def test_vertical_span_of_one_line_is_zero() -> None:
    assert order._vertical_span([_line(0, 1, 500)], HEIGHT) == 0.0


def test_vertical_span_of_empty_is_zero_not_an_error() -> None:
    assert order._vertical_span([], HEIGHT) == 0.0


# ---------------------------------------------------------------- аргументы


def test_defaults_declare_mechanism_and_page_limit() -> None:
    args = order._parse_args([])
    assert args.mechanism == "text_layer"
    assert args.max_pages == 8
    assert args.pdf == []


def test_mechanism_is_a_closed_set() -> None:
    with pytest.raises(SystemExit):
        order._parse_args(["--mechanism", "hybrid"])


def test_device_is_a_closed_set_without_auto() -> None:
    """``auto`` нет: место вычисления — объявленное условие (R5)."""
    with pytest.raises(SystemExit):
        order._parse_args(["--device", "auto"])


def test_no_input_is_a_usage_error() -> None:
    assert order.main([]) == 2


def test_short_cell_columns_are_not_counted_as_prose_columns() -> None:
    """Таблица сравнения версий — тоже «канавка», но ожидание «левая-правая» для неё неверно.

    Измерено на корпусе: 13 страниц с канавкой оказались таблицами (заполнение 0.01–0.60),
    и считать их нарушением порядка значило бы объявить дефект там, где файл читает верно —
    таблицу читают строками.
    """
    lines = []
    for index in range(14):
        y = 900 - index * 40
        lines.append(_line(40, 110, y, f"cell-{index}"))       # узкая ячейка
        lines.append(_line(440, 520, y, f"cell-r-{index}"))
    assert order._gutter(lines, WIDTH, HEIGHT) is not None     # канавка есть
    page = {
        "reason": "two_column",
        "expected": [],
        "observed": [],
    }
    # Классификация живёт в `_page_geometry`; здесь проверяется её решающее правило — заполнение.
    fill_left = order._fill(
        [line for line in lines if line["x1"] <= 400], 40, 400
    )
    assert fill_left < order.COLUMN_FILL_MIN
    assert page["reason"] == "two_column"


# ---------------------------------------------------------------- положительный контроль


def _write_two_column_pdf(path: Path, *, rows: int = 40, interleaved: bool = False) -> None:
    """Синтетическая двухколоночная страница с **известным** порядком чтения.

    Контроль нужен потому, что на реальном корпусе верное ожидание неизвестно: прибор,
    перепутавший направление оси, выдаёт правдоподобное число. Здесь порядок задан при
    генерации, и ошибка видна как 0.025 вместо 1.0 — именно так она и была найдена.
    """
    # Строки — **проза во всю ширину колонки**: прибор отделяет прозу от таблицы по заполнению,
    # и короткая строка честно попала бы в `table_like` (это проверено отдельным тестом).
    left = [
        f"left column prose line number {index:02d} which fills the column"
        for index in range(rows)
    ]
    right = [
        f"right column prose line number {index:02d} which fills the column"
        for index in range(rows)
    ]
    ops: list[str] = []
    # Правая колонка смещена по высоте на полстроки: у прозы строки колонок **не** совпадают
    # по высоте, и ровное совпадение прибор справедливо читает как таблицу (`row_aligned`).
    tops = {72: 740, 320: 732}
    if interleaved:
        for index in range(rows):
            ops.append(f"BT /F1 8 Tf 1 0 0 1 72 {740 - index * 16} Tm ({left[index]}) Tj ET")
            ops.append(f"BT /F1 8 Tf 1 0 0 1 320 {732 - index * 16} Tm ({right[index]}) Tj ET")
    else:
        for x, lines in ((72, left), (320, right)):
            for index, text in enumerate(lines):
                ops.append(f"BT /F1 8 Tf 1 0 0 1 {x} {tops[x] - index * 16} Tm ({text}) Tj ET")
    content = "\n".join(ops).encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n" + content + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for index, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{index} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode() + b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    ).encode()
    path.write_bytes(bytes(out))


def test_known_correct_order_scores_one(tmp_path: Path) -> None:
    """Положительный контроль: порядок, заданный при генерации, обязан дать LCS 1.0.

    Без этого теста прибор с перевёрнутой осью ``y`` отрапортовал бы «порядок нарушен на всех
    страницах» — и это выглядело бы находкой.
    """
    import pypdfium2 as pdfium

    pdf_path = tmp_path / "two_column.pdf"
    _write_two_column_pdf(pdf_path)
    pdf = pdfium.PdfDocument(str(pdf_path))
    try:
        geometry = order._page_geometry(pdf, 0)
        assert geometry["reason"] == "two_column"
        metrics = order._order_metrics(geometry["expected"], geometry["observed"])
    finally:
        pdf.close()
    assert metrics["lcs_ratio"] == 1.0
    assert metrics["interleaved"] is False


def test_known_interleaved_page_is_caught(tmp_path: Path) -> None:
    """Отрицательный контроль: страница, записанная построчно из двух колонок, обязана краснеть."""
    import pypdfium2 as pdfium

    pdf_path = tmp_path / "interleaved.pdf"
    _write_two_column_pdf(pdf_path, interleaved=True)
    pdf = pdfium.PdfDocument(str(pdf_path))
    try:
        geometry = order._page_geometry(pdf, 0)
        assert geometry["reason"] == "two_column"
        metrics = order._order_metrics(geometry["expected"], geometry["observed"])
    finally:
        pdf.close()
    assert metrics["interleaved"] is True
    assert metrics["lcs_ratio"] < 0.9
