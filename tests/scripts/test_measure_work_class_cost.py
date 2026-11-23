"""Тесты прибора цены класса работы (`scripts/measure_work_class_cost.py`).

Проверяются свойства, поломка которых **меняет опубликованное число**, а не форма вывода:

* **класс не подменяется** — `pdf_structured`/`office` прибор обязан отвергнуть с
  указателем на свой прибор, а не снять на них цену HTML (иначе опубликованное число
  описывало бы не тот класс);
* **отбор документов** — берутся только HTML-строки `document_version` с существующим
  `raw_path`, крупнейшие первыми: медиана по случайной странице не является ценой класса;
* **отсутствие входа — не ноль** — пустая выборка даёт код 2 и «замер не снят», а не
  строку с нулевыми пиками;
* **пик и прирост не смешиваются** — прибор публикует обе величины (`peak` процесса и
  `delta` над базой), и `delta <= peak` обязано выполняться: перепутанные местами поля
  дали бы цену воркера, умноженную на ширину окна.

Реальный корпус и `state.db` не нужны: песочница в `tmp_path`.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: ``scripts/`` не пакет."""
    path = _ROOT / "scripts" / "measure_work_class_cost.py"
    spec = importlib.util.spec_from_file_location("measure_work_class_cost", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_work_class_cost"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def instrument():
    return _load_module()


def _state_db(tmp_path: Path, rows: list[tuple[str, str, str, int]]) -> Path:
    """Минимальный `state.db`: только колонки, которые читает прибор."""
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        create table document_version (
            url text not null,
            raw_path text,
            extractor_version text,
            extract_page_count integer
        )
        """
    )
    conn.executemany(
        "insert into document_version (url, raw_path, extractor_version, extract_page_count)"
        " values (?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return db


def test_foreign_class_is_refused_with_pointer(instrument, tmp_path: Path, capsys) -> None:
    """Чужой класс не меряется этим прибором: код 2 и указатель, а не число HTML."""
    db = _state_db(tmp_path, [])
    code = instrument.main(["--state-db", str(db), "--class", "pdf_structured"])
    assert code == 2
    err = capsys.readouterr().err
    assert "measure_extract_peak.py" in err
    assert "office" not in err.split("не обслуживается")[0]


def test_office_is_named_as_unmeasured(instrument, tmp_path: Path, capsys) -> None:
    """`office` — не «ноль», а названная дыра: отказ говорит именно это."""
    db = _state_db(tmp_path, [])
    code = instrument.main(["--state-db", str(db), "--class", "office"])
    assert code == 2
    assert "не измерен" in capsys.readouterr().err


def test_missing_state_db_is_exit_2(instrument, tmp_path: Path) -> None:
    """Нет файла состояния — не пустой замер, а отказ."""
    assert instrument.main(["--state-db", str(tmp_path / "нет.db")]) == 2


def test_rows_selects_only_html_with_existing_bytes(instrument, tmp_path: Path) -> None:
    """Отбор: HTML-строки с существующим `raw_path`, крупнейшие первыми."""
    good = tmp_path / "a.bin"
    good.write_bytes(b"<html></html>")
    big = tmp_path / "b.bin"
    big.write_bytes(b"<html></html>")
    db = _state_db(
        tmp_path,
        [
            ("https://h/small", str(good), "trafilatura-html-8", 1),
            ("https://h/big", str(big), "trafilatura-html-8", 99),
            # не HTML — не класс model_free
            ("https://h/doc.pdf", str(good), "docling-doc-4:layout/cpu", 500),
            # файла нет — считать нечего
            ("https://h/gone", str(tmp_path / "нет.bin"), "trafilatura-html-8", 10),
        ],
    )
    rows = instrument._rows(db, 10)
    assert [url for url, _raw in rows] == ["https://h/big", "https://h/small"]


def test_empty_selection_is_not_a_zero_measurement(instrument, tmp_path: Path, capsys) -> None:
    """Пустая выборка: код 2 и явный текст, а не строка с нулевыми пиками."""
    db = _state_db(tmp_path, [])
    code = instrument.main(["--state-db", str(db)])
    assert code == 2
    assert "замер не снят" in capsys.readouterr().err


def test_percentile_is_nearest_rank(instrument) -> None:
    """p90 не выходит за границы и не выдумывает промежуточных значений."""
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert instrument._percentile(values, 0.0) == 1.0
    assert instrument._percentile(values, 0.9) == 5.0
    assert instrument._percentile(values, 1.0) == 5.0


def test_delta_never_exceeds_peak(instrument, tmp_path: Path) -> None:
    """Прирост над базой не больше полного пика — иначе поля перепутаны."""
    html = tmp_path / "page.html"
    html.write_text("<html><body><h1>Заголовок</h1><p>Текст страницы.</p></body></html>",
                    encoding="utf-8")
    db = _state_db(tmp_path, [("https://h/p", str(html), "trafilatura-html-8", 1)])
    result = instrument._measure(db, 5)
    assert result["documents"] == 1
    assert result["delta_mb"]["max"] <= result["peak_mb"]["max"]
    assert result["ms"]["median"] >= 0.0
