#!/usr/bin/env python3
"""Порядок чтения двухколоночных страниц: геометрия против выдачи читателя (O37, план 26).

**Вопрос, на который прибор отвечает.** «Порядок чтения» в плане 26 назван ограничением, но
ни разу не измерялся. Проверить его можно без разметки человеком там, где правильный порядок
задан **геометрией**: у двухколоночной страницы сначала читается левая колонка сверху вниз,
затем правая. Это проверяемое свойство страницы, а не мнение о качестве текста.

**Почему сравнение идёт по личностям строк, а не по тексту.** Первая версия сопоставляла
тексты строк и на реальном корпусе дала `comparable` 9 из 88: строки, собранные из рамок
символов, и строки из `get_text_range()` режутся по-разному, поэтому «не нашлось» смешивалось
с «прочитано не в том порядке». Здесь обе последовательности строятся из **одних и тех же**
объектов-строк: ожидание — левая колонка сверху вниз, затем правая; наблюдение — те же строки
в том порядке, в каком их отдаёт файл (позиция первого символа строки). Тогда расхождение
означает ровно перестановку и ничего больше. Текст нужен только механизму `structured`, где
позиций символов нет, и там метод назван в отчёте.

**Как строится ожидание.** Рамки символов берутся из самого PDF (`pypdfium2`), группируются в
строки по вертикали, строка **рвётся** на широком горизонтальном промежутке (иначе строка левой
и строка правой колонки на одной высоте склеиваются в одну, и канавка не находится никогда),
затем ищется вертикальная канавка — полоса, которую не пересекает ни одна строка.

**Три проверки, без которых прибор врёт правдоподобно** (все три найдены на корпусе, а не
придуманы):

* **выравнивание колонок** — у настоящей колонки жёсткий левый край; если «правые» строки
  начинаются вразнобой, это концы строк одной колонки, разрезанные промежутком, а не колонка
  (без этой проверки — 487 «двухколоночных» страниц вместо 13);
* **поворот текста** — при тексте, нарисованном с поворотом, `get_charbox` даёт координаты
  повёрнутого пространства, и «строка» по `y` склеивает несвязанные символы (измерено:
  страница 108 документа `2618e873…` даёт `'eRc-CCCCCCCC8'`); такая страница называется
  `rotated_text` и в знаменатель **не** входит;
* **направление оси** — у pdfium `y` растёт вверх, поэтому «сверху вниз» — это убывание `y`.
  Перевёрнутая ось даёт правдоподобное «порядок нарушен на всех страницах»; ловится
  положительным контролем — синтетической страницей с известным порядком (доля LCS 0.025
  вместо 1.0 до правки).

**Границы.** Прибор не судит, потерян ли текст (это O26/O35), и не сравнивает механизмы по
качеству. Страница с таблицей может быть принята за двухколоночную — поэтому результат
публикуется по страницам, а не одним числом, и документы с подозрением называются.

Запуск::

    .venv/bin/python scripts/measure_reading_order.py \
      --from-state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \
      --mechanism text_layer --max-pages 0 --out-json /tmp/reading_order.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

#: Предел символов на страницу: страховка от патологической страницы, объявляется в отчёте.
MAX_CHARS_PER_PAGE = 20000

#: Допуск группировки символов в строку — доля высоты символа.
LINE_TOLERANCE = 0.5

#: Минимальная доля соседних символов с горизонтальным смещением: ниже — текст повёрнут.
HORIZONTAL_SHARE = 0.7

#: Допуск жёсткого левого края колонки — доля ширины страницы.
COLUMN_ALIGN_TOLERANCE = 0.03

#: Доля строк колонки, обязанная попасть в допуск выравнивания.
COLUMN_ALIGN_SHARE = 0.8

#: Строка считается «во всю ширину», если она шире этой доли страницы.
FULL_WIDTH_SHARE = 0.8

#: Минимальное заполнение колонки, при котором она читается как **проза**.
#:
#: Колонка прозы заполняет свою ширину (перенос строки идёт по правому краю), ячейка таблицы —
#: нет. Измерено на корпусе 16.11.2026: у 13 «двухколоночных» страниц заполнение 0.01–0.60 и
#: все они — таблицы сравнения версий; синтетическая проза даёт 0.89–0.97.
COLUMN_FILL_MIN = 0.7

#: Доля левых строк, у которых есть правая строка на той же высоте, при которой это **таблица**.
#:
#: У колонок прозы совпадение высот случайно (строки независимы), у таблицы — систематично:
#: это строки сетки. Измерено 16.11.2026: 13 «двухколоночных» страниц корпуса дали 0.48–1.00
#: и все оказались таблицами сравнения версий; синтетическая проза даёт 0.00.
ROW_ALIGN_TABLE = 0.3


def _normalize(text: str) -> str:
    """Строка для сравнения текстом: пробелы схлопнуты, регистр сохранён.

    Регистр **не** трогается: две строки, различающиеся регистром, — разные строки текста, и
    приведение к нижнему регистру склеило бы их, завысив совпадение порядка.
    """
    return " ".join(text.split())


def _is_pdf(path: Path) -> bool:
    """PDF ли это — по сигнатуре: retained-файл называется ``{sha256}.bin``."""
    try:
        with path.open("rb") as handle:
            return handle.read(5).startswith(b"%PDF")
    except OSError:
        return False


def _candidates(db: str) -> list[dict[str, Any]]:
    """Документы с retained-байтами из ``state.db`` (только чтение)."""
    if not Path(db).is_file():
        raise SystemExit(f"measure: нет файла state.db: {db}")
    uri = f"file:{Path(db).expanduser().resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        rows = connection.execute(
            "select distinct raw_path, extract_page_count from document_version"
            " where raw_path is not null"
        ).fetchall()
    except sqlite3.Error as exc:
        raise SystemExit(f"measure: state.db не читается: {exc}") from exc
    finally:
        connection.close()
    picked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_path, page_count in rows:
        path = Path(raw_path)
        if not path.is_file() or not _is_pdf(path) or str(path) in seen:
            continue
        seen.add(str(path))
        picked.append({"path": str(path), "page_count": page_count})
    return picked


def _line_payload(
    segment: list[tuple[float, float, float, float, str, int]]
) -> dict[str, Any]:
    """Одна визуальная строка: рамки, центр по высоте, текст, позиция в файле.

    ``first`` — наименьший индекс символа строки, то есть **место строки в порядке файла**.
    Именно он сравнивается с геометрическим ожиданием: сравнение идёт по личностям строк, а не
    по тексту, поэтому «не нашлось» не может быть спутано с «прочитано не в том порядке».
    """
    return {
        "x0": segment[0][0],
        "x1": segment[-1][2],
        "y": sum((b[1] + b[3]) / 2 for b in segment) / len(segment),
        "text": "".join(b[4] for b in segment),
        "first": min(b[5] for b in segment),
    }


def _lines_from_boxes(
    boxes: list[tuple[float, float, float, float, str, int]]
) -> list[dict[str, Any]]:
    """Группирует символы в строки: ``[{'x0','x1','y','text','first'}]``.

    Строка — символы с пересекающимися вертикальными интервалами. Внутри строки она **рвётся**
    на широком горизонтальном промежутке: в двухколоночной вёрстке строка левой и строка правой
    колонки лежат на одной высоте, и группировка только по ``y`` склеила бы их в одну — канавка
    после этого не находится никогда (проверено: 0 страниц на корпусе до этой правки). Порог
    берётся от ширины символов, а не от страницы: пробел между словами масштабируется с кеглем.
    """
    if not boxes:
        return []
    ordered = sorted(boxes, key=lambda b: (-(b[1] + b[3]) / 2, b[0]))
    lines: list[list[tuple[float, float, float, float, str, int]]] = []
    for box in ordered:
        center = (box[1] + box[3]) / 2
        height = max(1e-6, box[3] - box[1])
        placed = False
        for line in lines:
            ref = (line[0][1] + line[0][3]) / 2
            ref_height = max(1e-6, line[0][3] - line[0][1])
            if abs(center - ref) <= LINE_TOLERANCE * max(height, ref_height):
                line.append(box)
                placed = True
                break
        if not placed:
            lines.append([box])
    result: list[dict[str, Any]] = []
    for line in lines:
        xs = sorted(line, key=lambda b: b[0])
        median_width = sorted(b[2] - b[0] for b in xs)[len(xs) // 2] or 1.0
        gap_limit = max(3.0 * median_width, 0.02 * max(b[2] for b in xs))
        segment: list[tuple[float, float, float, float, str, int]] = [xs[0]]
        for box in xs[1:]:
            if box[0] - segment[-1][2] > gap_limit:
                result.append(_line_payload(segment))
                segment = [box]
            else:
                segment.append(box)
        result.append(_line_payload(segment))
    return result


def _is_horizontal(
    boxes: list[tuple[float, float, float, float, str, int]], *, limit: int = 400
) -> bool:
    """Текст нарисован горизонтально?

    Соседние символы строки смещаются по ``x``; у повёрнутого текста они смещаются по ``y``.
    Проверка нужна до всякой геометрии: на повёрнутой странице ``get_charbox`` даёт координаты
    другого пространства, и «строка по y» склеивает несвязанные символы.
    """
    sample = boxes[:limit]
    if len(sample) < 20:
        return False
    horizontal = 0
    pairs = 0
    for previous, current in itertools.pairwise(sample):
        dx = abs(current[0] - previous[0])
        dy = abs(current[1] - previous[1])
        if dx < 1e-6 and dy < 1e-6:
            continue
        pairs += 1
        if dx > dy:
            horizontal += 1
    if not pairs:
        return False
    return horizontal / pairs >= HORIZONTAL_SHARE


def _fill(lines: list[dict[str, Any]], left_edge: float, right_edge: float) -> float:
    """Медианная доля заполнения колонки по ширине: проза ≈1, ячейка таблицы много меньше.

    Медиана, а не среднее: одна короткая строка (заголовок, номер) не должна менять вывод, а
    ячейка таблицы коротка **вся**, и медиана это видит.
    """
    width = right_edge - left_edge
    if not lines or width <= 0:
        return 0.0
    widths = sorted((line["x1"] - line["x0"]) / width for line in lines)
    return widths[len(widths) // 2]


def _row_aligned_share(
    left: list[dict[str, Any]], right: list[dict[str, Any]], height: float
) -> float:
    """Доля левых строк, у которых правая строка лежит на той же высоте.

    Это и отличает таблицу от двух колонок прозы: в таблице строка — объект, и левая ячейка
    всегда имеет пару справа; в прозе высоты независимы, и совпадение — случайность. Порог
    назван константой, а не подобран: см. :data:`ROW_ALIGN_TABLE`.
    """
    if not left or not right or height <= 0:
        return 0.0
    tolerance = 0.006 * height
    matched = sum(
        1 for line in left if any(abs(line["y"] - other["y"]) <= tolerance for other in right)
    )
    return matched / len(left)


def _vertical_span(lines: list[dict[str, Any]], height: float) -> float:
    """Доля высоты страницы, которую занимает группа строк."""
    if not lines or height <= 0:
        return 0.0
    return (max(line["y"] for line in lines) - min(line["y"] for line in lines)) / height


def _aligned(lines: list[dict[str, Any]], width: float) -> bool:
    """Есть ли у группы строк жёсткий левый край.

    Колонка начинается там, где начинается; концы строк — нет. Без этой проверки за колонку
    сойдут концы строк одной колонки, разрезанные промежутком: левый край у них вразнобой.
    """
    if not lines or width <= 0:
        return False
    lefts = sorted(line["x0"] for line in lines)
    median = lefts[len(lefts) // 2]
    tolerance = COLUMN_ALIGN_TOLERANCE * width
    inside = sum(1 for value in lefts if abs(value - median) <= tolerance)
    return inside / len(lefts) >= COLUMN_ALIGN_SHARE


def _gutter(
    lines: list[dict[str, Any]], width: float, height: float
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Ищет вертикальную канавку: ``(x, левые строки, правые строки)`` либо ``None``.

    Канавка обязана быть **чистой** (ни одна строка её не пересекает), обе стороны — покрывать
    ≥50 % высоты и иметь жёсткий левый край. Три проверки, три разных ложных срабатывания:
    без первой за канавку сойдёт пробел внутри строки, без второй — пустая половина страницы
    (титул, односторонняя врезка), без третьей — разрезанные концы строк.
    """
    if len(lines) < 10:
        return None
    # Строки во всю ширину (заголовок, разделитель) не должны «закрывать» канавку: на реальной
    # двухколоночной странице заголовок идёт во всю ширину, а колонки под ним. Исключаем их из
    # проверки, но оставляем в ожидании — они и читаются первыми.
    narrow = [
        line for line in lines if (line["x1"] - line["x0"]) <= FULL_WIDTH_SHARE * width
    ]
    for step in range(35, 66):
        x = width * step / 100
        if any(line["x0"] < x < line["x1"] for line in narrow):
            continue
        left = [line for line in narrow if line["x1"] <= x]
        right = [line for line in narrow if line["x0"] >= x]
        if len(left) < 5 or len(right) < 5:
            continue
        if _vertical_span(left, height) <= 0.5 or _vertical_span(right, height) <= 0.5:
            continue
        if not (_aligned(left, width) and _aligned(right, width)):
            continue
        # ``y`` у pdfium растёт **вверх** (PDF-пространство): верх страницы — большее ``y``.
        # Сортировка по возрастанию читала бы страницу снизу вверх, и «порядок» сравнивался бы
        # с перевёрнутым ожиданием — измерено положительным контролем (синтетическая страница
        # с известным порядком): доля LCS 0.025 вместо 1.0.
        left.sort(key=lambda line: (-line["y"], line["x0"]))
        right.sort(key=lambda line: (-line["y"], line["x0"]))
        return x, left, right
    return None


def _page_geometry(pdf: Any, page_index: int) -> dict[str, Any]:
    """Геометрическое ожидание порядка строк страницы либо названная причина отказа.

    Возвращает ``{'reason': ..., 'expected': [...], 'observed': [...], ...}``. Причина
    называется, а не прячется: «страница не двухколоночная» и «текст повёрнут» — разные факты,
    и склеивать их в одно `None` значило бы потерять, сколько страниц выпало из замера и почему.
    """
    page = pdf[page_index]
    textpage = page.get_textpage()
    count = textpage.count_chars()
    if count < 400:
        return {"reason": "too_few_chars", "chars": count}
    width, height = page.get_width(), page.get_height()
    limit = min(count, MAX_CHARS_PER_PAGE)
    boxes: list[tuple[float, float, float, float, str, int]] = []
    for index in range(limit):
        box = textpage.get_charbox(index)
        if box:
            boxes.append((box[0], box[1], box[2], box[3], textpage.get_text_range(index, 1), index))
    if len(boxes) < 150:
        return {"reason": "too_few_boxes", "boxes": len(boxes)}
    if not _is_horizontal(boxes):
        return {"reason": "rotated_text", "chars": count}
    lines = _lines_from_boxes(boxes)
    found = _gutter(lines, width, height)
    if found is None:
        return {"reason": "not_two_column", "lines": len(lines)}
    x, left, right = found
    # Проза или таблица? Обе дают «канавку», но читаются по-разному: прозу — колонками,
    # таблицу — строками. Ожидание «левая колонка, затем правая» для таблицы было бы **неверным**
    # эталоном, поэтому такие страницы называются и в знаменатель не входят.
    fill_left = _fill(left, min(line["x0"] for line in left), x)
    fill_right = _fill(right, x, max(line["x1"] for line in right))
    row_aligned = _row_aligned_share(left, right, height)
    if min(fill_left, fill_right) < COLUMN_FILL_MIN or row_aligned >= ROW_ALIGN_TABLE:
        return {
            "reason": "table_like",
            "fill_left": round(fill_left, 3),
            "fill_right": round(fill_right, 3),
            "row_aligned": round(row_aligned, 3),
            "lines": len(lines),
        }
    column_lines = left + right
    # Ожидание: левая колонка сверху вниз, затем правая. Наблюдение: те же строки в порядке
    # файла (по позиции первого символа). Полные строки (заголовки во всю ширину) в сравнение
    # не входят: их место относительно колонок задаёт вёрстка, а не «левая-правая», и включать
    # их значило бы приписать прибору собственную догадку.
    expected = [line["first"] for line in column_lines]
    observed = [line["first"] for line in sorted(column_lines, key=lambda line: line["first"])]
    return {
        "reason": "two_column",
        "expected": expected,
        "observed": observed,
        "expected_text": [_normalize(line["text"]) for line in column_lines],
        "width": width,
        "height": height,
        "fill_left": round(fill_left, 3),
        "fill_right": round(fill_right, 3),
        "row_aligned": round(row_aligned, 3),
        "full_width_lines": len(lines) - len(column_lines),
        "truncated": count > limit,
    }


def _longest_increasing(values: list[int]) -> int:
    """Длина наибольшей возрастающей подпоследовательности (O(n log n)).

    Это и есть «сколько строк сохранили относительный порядок»: ожидание упорядочено, значит
    любая инверсия в наблюдённых позициях — перестановка строк местами.
    """
    tails: list[int] = []
    for value in values:
        low, high = 0, len(tails)
        while low < high:
            middle = (low + high) // 2
            if tails[middle] < value:
                low = middle + 1
            else:
                high = middle
        if low == len(tails):
            tails.append(value)
        else:
            tails[low] = value
    return len(tails)


def _interleaved(observed: list[int]) -> bool:
    """Есть ли пара «правая строка раньше левой» — подпись перепутанных колонок.

    Ожидание упорядочено: сначала левая колонка, затем правая, поэтому любая инверсия
    «большой индекс раньше меньшего» означает, что строки колонок перемешаны.
    """
    seen_max = -1
    for value in observed:
        if value < seen_max:
            return True
        seen_max = max(seen_max, value)
    return False


def _order_metrics(expected: list[int], observed: list[int]) -> dict[str, Any]:
    """Метрики порядка по личностям строк: доля LCS и признак чередования.

    Обе последовательности содержат **одни и те же** элементы, поэтому «пропало» здесь быть не
    может по построению: различие — только перестановка. Потеря строки — отдельный предмет
    (O26/O35), и она считается другим прибором, а не этим.
    """
    if len(expected) != len(observed) or sorted(expected) != sorted(observed):
        raise ValueError("последовательности обязаны содержать одни и те же строки")
    if not expected:
        return {"comparable": 0, "lcs_ratio": None, "interleaved": None}
    positions = {value: index for index, value in enumerate(observed)}
    sequence = [positions[value] for value in expected]
    return {
        "comparable": len(expected),
        "lcs_ratio": _longest_increasing(sequence) / len(sequence),
        "interleaved": _interleaved(sequence),
    }


def _text_metrics(expected: list[str], extracted: list[str]) -> dict[str, Any]:
    """Метрики порядка **по тексту** — для механизма без позиций символов (`structured`).

    Здесь «не нашлось» возможно, и оно считается отдельно (`missing`): строка, которой нет в
    выдаче, — это потеря текста, а не порядок. Этот метод слабее основного и назван в отчёте.
    """
    wanted = [text for text in (_normalize(item) for item in expected) if text]
    if not wanted:
        return {"comparable": 0, "lcs_ratio": None, "interleaved": None, "missing": 0}
    positions: dict[str, list[int]] = {}
    for index, text in enumerate(extracted):
        positions.setdefault(_normalize(text), []).append(index)
    consumed: dict[str, int] = {}
    present: list[int] = []
    for text in wanted:
        slots = positions.get(text)
        if not slots:
            continue
        cursor = consumed.get(text, 0)
        if cursor >= len(slots):
            continue
        consumed[text] = cursor + 1
        present.append(slots[cursor])
    if not present:
        return {
            "comparable": 0,
            "lcs_ratio": None,
            "interleaved": None,
            "missing": len(wanted),
        }
    return {
        "comparable": len(present),
        "lcs_ratio": _longest_increasing(present) / len(present),
        "interleaved": _interleaved(present),
        "missing": len(wanted) - len(present),
    }


def _page_text_lines(pdf: Any, page_index: int) -> list[str]:
    """Строки страницы так, как их отдаёт сам PDF (``text_layer`` читает именно их)."""
    textpage = pdf[page_index].get_textpage()
    raw = textpage.get_text_range()
    return [line for line in (_normalize(part) for part in raw.splitlines()) if line]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Порядок чтения двухколоночных страниц (O37)"
    )
    parser.add_argument("--pdf", action="append", default=[], help="PDF (можно несколько)")
    parser.add_argument("--from-state-db", metavar="PATH", help="state.db оператора")
    parser.add_argument(
        "--mechanism",
        default="text_layer",
        choices=("text_layer", "structured"),
        help="text_layer — порядок файла (сравнение по личностям строк); "
        "structured — текст docling по странице (сравнение по тексту, слабее)",
    )
    parser.add_argument("--device", default="cpu", choices=("cpu", "cuda"))
    parser.add_argument(
        "--max-pages", type=int, default=8, help="первые N страниц каждого документа (0 — все)"
    )
    parser.add_argument(
        "--top", type=int, default=0, help="ограничить число документов (0 — все)"
    )
    parser.add_argument("--out-json", metavar="PATH", help="полный отчёт по страницам")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    import pypdfium2 as pdfium

    documents: list[dict[str, Any]] = [
        {"path": str(Path(raw).expanduser().resolve())} for raw in args.pdf
    ]
    if args.from_state_db:
        documents.extend(_candidates(args.from_state_db))
    documents = [item for item in documents if Path(item["path"]).is_file()]
    if args.top:
        documents = documents[: args.top]
    if not documents:
        print("measure: нет входных данных", file=sys.stderr)
        return 2

    converter = None
    if args.mechanism == "structured":
        # Механизм ``structured`` — это docling: его порядок берётся из **его** объекта
        # (``export_to_markdown(page_no=…)``), а не из pdfium, иначе под двумя именами мерился
        # бы один источник. Геометрия ожидания всё равно из PDF: она — свойство страницы.
        from core.adapters.extract_document import DoclingDocumentExtractor
        from core.domain.enums import DocumentLevel, PdfDevice

        converter = DoclingDocumentExtractor(
            DocumentLevel.FULL, pdf_device=PdfDevice(args.device)
        )._build_converter(pdf=True)  # noqa: SLF001 — иного входа «конвертер без извлечения» нет

    rows: list[dict[str, Any]] = []
    reasons: dict[str, int] = {}
    for position, item in enumerate(documents):
        # Прогресс печатается: перепись по всему корпусу идёт минутами, и молчащий прибор
        # неотличим от зависшего.
        print(
            f"[{position + 1}/{len(documents)}] {Path(item['path']).name[:16]}",
            flush=True,
        )
        path = item["path"]
        try:
            pdf = pdfium.PdfDocument(path)
        except Exception as exc:  # noqa: BLE001 — нечитаемый PDF это факт, а не падение
            rows.append({"path": path, "error": f"{type(exc).__name__}: {exc}"})
            continue
        pages = len(pdf)
        limit = pages if args.max_pages == 0 else min(args.max_pages, pages)
        structured = None
        if converter is not None:
            try:
                structured = converter.convert(path).document
            except Exception as exc:  # noqa: BLE001 — отказ конвертации это факт, а не падение
                rows.append({"path": path, "error": f"docling: {type(exc).__name__}: {exc}"})
                pdf.close()
                continue
        for index in range(limit):
            try:
                geometry = _page_geometry(pdf, index)
            except Exception as exc:  # noqa: BLE001
                rows.append({"path": path, "page": index + 1, "error": str(exc)})
                continue
            reason = geometry["reason"]
            reasons[reason] = reasons.get(reason, 0) + 1
            if reason == "table_like":
                # Страница с канавкой, но не проза: публикуется с числами, по которым решение
                # принято, иначе «13 страниц оказались таблицами» нельзя пересчитать из
                # артефакта, а такое утверждение — гипотеза.
                rows.append(
                    {
                        "path": path,
                        "page": index + 1,
                        "reason": reason,
                        "column_lines": geometry["lines"],
                        "fill_left": geometry["fill_left"],
                        "fill_right": geometry["fill_right"],
                        "row_aligned": geometry["row_aligned"],
                    }
                )
                continue
            if reason != "two_column":
                continue
            if args.mechanism == "text_layer":
                metrics = _order_metrics(geometry["expected"], geometry["observed"])
                method = "identity"
            else:
                try:
                    markdown = (
                        structured.export_to_markdown(page_no=index + 1) if structured else ""
                    )
                except Exception as exc:  # noqa: BLE001
                    rows.append({"path": path, "page": index + 1, "error": str(exc)})
                    continue
                metrics = _text_metrics(geometry["expected_text"], markdown.splitlines())
                method = "text"
            rows.append(
                {
                    "path": path,
                    "page": index + 1,
                    "method": method,
                    "column_lines": len(geometry["expected"]),
                    "full_width_lines": geometry["full_width_lines"],
                    "fill_left": geometry["fill_left"],
                    "fill_right": geometry["fill_right"],
                    "row_aligned": geometry["row_aligned"],
                    "truncated": geometry["truncated"],
                    **metrics,
                }
            )
        pdf.close()

    scored = [row for row in rows if row.get("lcs_ratio") is not None]
    summary = {
        "documents": len(documents),
        "pages_examined": sum(reasons.values()),
        "two_column_pages": reasons.get("two_column", 0),
        "scored_pages": len(scored),
        "interleaved_pages": sum(1 for row in scored if row["interleaved"]),
        "perfect_order_pages": sum(1 for row in scored if row["lcs_ratio"] == 1.0),
        "mean_lcs_ratio": (
            sum(row["lcs_ratio"] for row in scored) / len(scored) if scored else None
        ),
        "reasons": dict(sorted(reasons.items())),
        "errors": sum(1 for row in rows if row.get("error")),
    }
    payload = {
        "instrument": "scripts/measure_reading_order.py",
        "conditions": {
            "mechanism": args.mechanism,
            "device": args.device,
            "max_pages": args.max_pages,
            "documents": len(documents),
        },
        "summary": summary,
        "pages": rows,
    }
    print(
        f"страниц просмотрено: {summary['pages_examined']}, "
        f"двухколоночных: {summary['two_column_pages']}, "
        f"сопоставимых: {summary['scored_pages']}, "
        f"с чередованием: {summary['interleaved_pages']}, "
        f"порядок совпал полностью: {summary['perfect_order_pages']}, "
        f"средняя доля LCS: "
        + (f"{summary['mean_lcs_ratio']:.4f}" if summary["mean_lcs_ratio"] is not None else "—")
    )
    print("причины: " + ", ".join(f"{key}={value}" for key, value in summary["reasons"].items()))
    if args.out_json:
        Path(args.out_json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}")
    return 0 if scored else 1


if __name__ == "__main__":
    raise SystemExit(main())
