"""Пины прибора ``scripts/prepare_importance_pairs.py`` (без сети, без стенда, без пакетов).

Проверяются ровно те свойства выборки, на которых держится калибровка весов важности
(O20): если пары воспроизводятся, покрывают каталог и не повторяются, из ответов эксперта
можно считать матрицу предпочтений; иначе 60 строк описывают 30 параметров и один и тот же
порядок.

* **детерминизм**: две сборки на одном каталоге дают один список пар в том же порядке;
* **лимит использований**: ни один параметр не встречается чаще ``MAX_USES`` (3) — при
  лимите 1 пары были бы попарно несвязными (30 пар = 60 слотов = 30 параметров);
* **стратификация по модулям**: минимум один параметр от каждого модуля, иначе модуль с
  малым числом параметров выпадает из калибровки целиком;
* **перемешивание ``a``/``b``**: порядок задан хешем пары, а не ``random``, поэтому
  проверяется непостоянство, а не «случайность»: эксперт не должен читать закономерность
  «слева всегда важнее»;
* ``validate`` ловит пустой/неизвестный ``more_important``, дубль пары (в том числе в
  обратном порядке — так код и работает) и превышение лимита использований;
* ``summary`` считает распределение ответов и печатает ``equal``-пары;
* отказ без каталога и отказ при недостижимом ``--pairs``; недостижимое 50/50 при
  достижимом общем числе пар — **предупреждение**, а не отказ (обоснование в приборе:
  дизайн добирается внутри страт, расхождение печатается с числами).

Каталог синтетический, в ``tmp_path``; настоящие ``packages/`` не читаются.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "prepare_importance_pairs.py"
)


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_prepare_importance_pairs", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PAIRS = _load_harness()

CRITICAL = "Кр"
IMPORTANT = "Вж"
BASIC = "Баз"
OPTIONAL = "Опц"


def _write_catalog(path: Path, rows: list[tuple[str, str, str]]) -> Path:
    """Каталог из ``(param_id, module, importance)``; остальные колонки — нейтральные."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["param_id", "title", "module", "importance", "rule"])
        for param_id, module, importance in rows:
            writer.writerow([param_id, f"заголовок {param_id}", module, importance, f"правило {param_id}"])
    return path


def _wide_catalog(tmp_path: Path) -> Path:
    """72 параметра, 4 модуля, все четыре страты важности — каталог, где 48/24 достижимы.

    Модули разложены по кругу (``idx % 4``), поэтому ни одна страта важности не совпадает
    с одним модулем: стратификация по модулям проверяется внутри страт, а не подменяется
    ими.
    """
    rows: list[tuple[str, str, str]] = []
    for importance, count in ((CRITICAL, 8), (IMPORTANT, 16), (BASIC, 32), (OPTIONAL, 16)):
        for _ in range(count):
            index = len(rows) + 1
            rows.append((f"p{index:03d}", f"m{index % 4 + 1}", importance))
    return _write_catalog(tmp_path / "catalog.csv", rows)


def _build(catalog: Path, out: Path, params: int, pairs: int) -> int:
    return PAIRS.main(
        [
            "build",
            "--catalog",
            str(catalog),
            "--out",
            str(out),
            "--params",
            str(params),
            "--pairs",
            str(pairs),
        ]
    )


def _pairs_of(path: Path) -> list[tuple[str, str]]:
    return [(row["param_a"], row["param_b"]) for row in PAIRS.read_xlsx(path)]


def _fill(path: Path, values: list[str]) -> None:
    """Проставляет ``more_important`` построчно (openpyxl)."""
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(path)
    try:
        sheet = book["лист"]
        column = PAIRS.COLUMNS.index("more_important") + 1
        for offset, value in enumerate(values, start=2):
            sheet.cell(row=offset, column=column).value = value
        book.save(path)
    finally:
        book.close()


def _set(path: Path, row: int, column_name: str, value: str) -> None:
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(path)
    try:
        sheet = book["лист"]
        sheet.cell(row=row, column=PAIRS.COLUMNS.index(column_name) + 1).value = value
        book.save(path)
    finally:
        book.close()


@pytest.fixture()
def sheet_24(tmp_path: Path) -> Path:
    """24 пары на 48 параметрах — рабочий лист для проверок ``validate``/``summary``."""
    catalog = _wide_catalog(tmp_path)
    out = tmp_path / "pairs.xlsx"
    assert _build(catalog, out, 48, 24) == 0
    return out


# -- детерминизм --------------------------------------------------------------


def test_build_is_deterministic(tmp_path: Path) -> None:
    """Две сборки на одном каталоге дают один список пар в том же порядке.

    Закрывает дефект «повторная калибровка — другой лист»: если отбор или ориентация
    пар опираются на ``random``/порядок обхода множества, два эксперта размечают разные
    вопросы, и фит весов нельзя ни воспроизвести, ни проверить.
    """
    catalog = _wide_catalog(tmp_path)
    first_path = tmp_path / "first.xlsx"
    second_path = tmp_path / "second.xlsx"

    assert _build(catalog, first_path, 48, 24) == 0
    assert _build(catalog, second_path, 48, 24) == 0

    assert _pairs_of(first_path) == _pairs_of(second_path)
    assert len(_pairs_of(first_path)) == 24


# -- лимит использований ------------------------------------------------------


def test_no_param_exceeds_max_uses(tmp_path: Path) -> None:
    """Лимит ``MAX_USES`` (3) исчерпан ровно, а не превышен: 30 × 3 / 2 = 45 пар.

    Закрывает дефект «60 пар покрыли 12 параметров»: без лимита жадный выбор крутит самые
    удобные параметры, и лист перестаёт говорить о каталоге. Насыщенный набор проверяет
    лимит с двух сторон: 45 пар дают каждому параметру ровно 3 использования (все 30
    покрыты), а 46-я пара уже не существует — значит ограничение задаёт ёмкость дизайна,
    а не декоративную проверку после отбора. Тест падает и при ослаблении лимита (тогда
    46 пар достижимы), и при его ужесточении (45 не набираются).
    """
    rows = [(f"p{index:03d}", "m1", BASIC) for index in range(1, 31)]
    catalog = _write_catalog(tmp_path / "catalog.csv", rows)
    out = tmp_path / "pairs.xlsx"
    assert _build(catalog, out, 30, 45) == 0

    uses = Counter()
    for first, second in _pairs_of(out):
        uses[first] += 1
        uses[second] += 1
    assert len(uses) == 30
    assert max(uses.values()) == PAIRS.MAX_USES

    params = PAIRS.read_catalog(catalog)
    order, _source = PAIRS.importance_order([param.importance for param in params])
    with pytest.raises(ValueError):
        PAIRS.build_pairs(params, 46, order)


# -- стратификация по модулям -------------------------------------------------


def test_every_module_gets_a_param(tmp_path: Path) -> None:
    """При ``--params`` ≥ числа модулей каждый модуль представлен хотя бы одним параметром.

    Закрывает дефект «модуль выпал из калибровки»: при пропорциональном отборе внутри
    страты ``Баз`` (m1 20, m2 9, m3 1) и квоте 3 модуль ``m3`` получил бы 0 параметров, и
    вопрос «важен ли он» никогда бы не задался. Проверяется отбор, а не лист: параметр,
    не попавший в пары, в листе не виден и о пропуске ничего не сказал бы.
    """
    rows: list[tuple[str, str, str]] = []
    for index in range(1, 21):
        rows.append((f"p{index:03d}", "m1", BASIC))
    for index in range(21, 30):
        rows.append((f"p{index:03d}", "m2", BASIC))
    rows.append(("p030", "m3", BASIC))
    catalog = _write_catalog(tmp_path / "catalog.csv", rows)

    selected, conditions = PAIRS.select_params(PAIRS.read_catalog(catalog), 3)

    assert len(selected) == 3
    assert {param.module for param in selected} == {"m1", "m2", "m3"}
    # Пропорционально по числу параметров было бы 2/1/0 — минимальное присутствие меняет
    # именно этот исход, а не порядок строк.
    assert conditions["importance_quotas"] == {BASIC: 3}


# -- перемешивание a/b --------------------------------------------------------


def test_pair_orientation_is_mixed_not_constant(tmp_path: Path) -> None:
    """Порядок ``a``/``b`` непостоянен: на 24 парах доля ``param_a < param_b`` — между 0 и 1.

    Закрывает дефект «эксперт видит закономерность»: если слева всегда стоит меньший
    ``param_id`` (или всегда первый по стратификации), ответ перестаёт быть независимым —
    человек начинает выбирать позицию, а не параметр. Проверяется именно непостоянство:
    выбор детерминирован (хеш пары), поэтому «случайность» здесь не требуется и не
    проверяется.
    """
    catalog = _wide_catalog(tmp_path)
    out = tmp_path / "pairs.xlsx"
    assert _build(catalog, out, 48, 24) == 0

    pairs = _pairs_of(out)
    assert len(pairs) >= 20, "для проверки перемешивания нужно не менее 20 пар"
    left_smaller = sum(1 for first, second in pairs if first < second)
    assert 0 < left_smaller < len(pairs)


# -- validate -----------------------------------------------------------------


def test_validate_accepts_complete_sheet(sheet_24: Path, capsys: Any) -> None:
    """Заполненный лист проходит: иначе отказ ``validate`` не отличить от дефекта листа."""
    _fill(sheet_24, ["a"] * 24)
    code = PAIRS.main(["validate", "--xlsx", str(sheet_24), "--pairs", "24"])
    captured = capsys.readouterr()
    assert code == 0
    assert "OK:" in captured.out


def test_validate_rejects_empty_choice(sheet_24: Path, capsys: Any) -> None:
    """Пустой ``more_important`` — отказ с кодом 2: неразмеченная пара не даёт данных.

    Закрывает дефект «пустая строка посчиталась как ответ»: ``summary`` тогда печатал бы
    распределение по меньшему знаменателю, и недозаполненный лист выглядел бы полным.
    """
    _fill(sheet_24, ["a"] * 24)
    _set(sheet_24, 3, "more_important", "")
    code = PAIRS.main(["validate", "--xlsx", str(sheet_24), "--pairs", "24"])
    captured = capsys.readouterr()
    assert code == 2
    assert "more_important не заполнен" in captured.err


def test_validate_rejects_unknown_choice(sheet_24: Path, capsys: Any) -> None:
    """Значение вне ``{a,b,equal}`` — отказ: опечатка не должна стать отдельным ответом."""
    _fill(sheet_24, ["a"] * 24)
    _set(sheet_24, 3, "more_important", "first")
    code = PAIRS.main(["validate", "--xlsx", str(sheet_24), "--pairs", "24"])
    captured = capsys.readouterr()
    assert code == 2
    assert "не из" in captured.err


def test_validate_rejects_duplicate_pair_same_order(sheet_24: Path, capsys: Any) -> None:
    """Повтор той же пары в том же порядке — отказ: один вопрос нельзя задать дважды.

    Закрывает дефект «дубль утяжеляет свой голос»: пара, попавшая в лист дважды,
    получает двойной вес в матрице предпочтений, и фит перестаёт быть оценкой по
    независимым сравнениям.
    """
    _fill(sheet_24, ["a"] * 24)
    rows = PAIRS.read_xlsx(sheet_24)
    _set(sheet_24, 4, "param_a", rows[0]["param_a"])
    _set(sheet_24, 4, "param_b", rows[0]["param_b"])
    code = PAIRS.main(["validate", "--xlsx", str(sheet_24), "--pairs", "24"])
    captured = capsys.readouterr()
    assert code == 2
    assert "дубль пары" in captured.err


def test_validate_rejects_duplicate_pair_reversed(sheet_24: Path, capsys: Any) -> None:
    """Пара в обратном порядке — тоже дубль: ключ пары неупорядочен (фактическое поведение).

    Проверяется **то, как код работает**, а не желаемое: ``validate_rows`` строит
    ``tuple(sorted((param_a, param_b)))``, поэтому ``x/y`` и ``y/x`` — одна пара. Это
    правильно по существу (сравнение симметрично), но тест фиксирует именно это, чтобы
    смена правила была видна как смена поведения, а не как «тест сломался».
    """
    _fill(sheet_24, ["a"] * 24)
    rows = PAIRS.read_xlsx(sheet_24)
    _set(sheet_24, 4, "param_a", rows[0]["param_b"])
    _set(sheet_24, 4, "param_b", rows[0]["param_a"])
    code = PAIRS.main(["validate", "--xlsx", str(sheet_24), "--pairs", "24"])
    captured = capsys.readouterr()
    assert code == 2
    assert "дубль пары" in captured.err


def test_validate_rejects_wrong_pair_count(sheet_24: Path, capsys: Any) -> None:
    """Число пар сверяется с объявленным: 24 строки при ``--pairs 30`` — отказ."""
    _fill(sheet_24, ["a"] * 24)
    code = PAIRS.main(["validate", "--xlsx", str(sheet_24), "--pairs", "30"])
    captured = capsys.readouterr()
    assert code == 2
    assert "пар 24, ожидалось 30" in captured.err


# -- summary ------------------------------------------------------------------


def test_summary_counts_choices_and_lists_equal(sheet_24: Path, capsys: Any) -> None:
    """``summary`` печатает распределение ответов и сами ``equal``-пары.

    Закрывает дефект «``equal`` посчитан, но не назван»: именно эти пары первыми идут в
    разбор ординальности весов (Кр > Вж > Баз > Опц), и без списка проверять нечего —
    число без адресов не даёт гипотезы.
    """
    _fill(sheet_24, ["a"] * 10 + ["b"] * 9 + ["equal"] * 5)
    code = PAIRS.main(["summary", "--xlsx", str(sheet_24)])
    captured = capsys.readouterr()
    assert code == 0
    assert "a 10, b 9, equal 5, не размечено 0" in captured.out
    assert "equal: 5 — первыми в разбор:" in captured.out
    rows = PAIRS.read_xlsx(sheet_24)
    for row in rows:
        if row["more_important"] == "equal":
            assert f"{row['pair_id']}:" in captured.out


# -- отказы -------------------------------------------------------------------


def test_build_refuses_without_catalog(tmp_path: Path, capsys: Any) -> None:
    """Нет каталога — код 2 и путь в сообщении: прибор не выдумывает параметры."""
    missing = tmp_path / "absent" / "catalog.csv"
    code = PAIRS.main(
        ["build", "--catalog", str(missing), "--out", str(tmp_path / "x.xlsx")]
    )
    captured = capsys.readouterr()
    assert code == 2
    assert "каталог не найден" in captured.err
    assert str(missing) in captured.err
    assert not (tmp_path / "x.xlsx").exists()


def test_build_refuses_unreachable_pair_count(tmp_path: Path, capsys: Any) -> None:
    """``--pairs`` больше достижимого — **отказ** (код 2) с числами, а не урезанный лист.

    Закрывает дефект «прибор тихо выдал меньше пар, чем просили»: дизайн эксперимента
    (60 пар) объявлен, и молчаливое сокращение изменило бы предмет калибровки. 4 параметра
    при лимите 3 дают ровно 6 пар — на 10 их не хватает.
    """
    rows = [(f"p{index:03d}", "m1", BASIC) for index in range(1, 5)]
    catalog = _write_catalog(tmp_path / "catalog.csv", rows)
    out = tmp_path / "pairs.xlsx"

    code = _build(catalog, out, 4, 10)
    captured = capsys.readouterr()
    assert code == 2
    assert "пар 6 вместо 10" in captured.err
    assert not out.exists()


def test_build_warns_when_half_between_is_unreachable(tmp_path: Path, capsys: Any) -> None:
    """Недостижимое 50/50 — предупреждение, а не отказ (обоснование в приборе).

    Фиксируется фактическое поведение: при 4 параметрах ``Кр`` и 30 ``Баз`` между
    соседними стратами возможно не более ``4 × 3 = 12`` пар, а цель при 40 парах — 20.
    Прибор добирает остаток **внутри** страт и печатает расхождение с числами: дизайн
    меняется видимо, лист остаётся пригодным. Отказ здесь был бы отказом измерять там,
    где ответ достижим другим составом пар.
    """
    rows: list[tuple[str, str, str]] = [
        (f"p{index:03d}", "m1", CRITICAL) for index in range(1, 5)
    ] + [(f"p{index:03d}", "m2", BASIC) for index in range(5, 35)]
    catalog = _write_catalog(tmp_path / "catalog.csv", rows)
    out = tmp_path / "pairs.xlsx"

    code = _build(catalog, out, 34, 40)
    captured = capsys.readouterr()
    assert code == 0
    assert out.is_file()
    assert "50/50 недостижимо" in captured.err
    assert len(_pairs_of(out)) == 40
    assert "между соседними стратами" in captured.out
