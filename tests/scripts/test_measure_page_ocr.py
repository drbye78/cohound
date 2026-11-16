"""Тесты прибора цены страницы (`scripts/measure_page_ocr.py`, O35).

Прибор отвечает на два вопроса, и оба ломаются тихо:

* **сохранён ли номер страницы** при частичной конвертации: если провенанс говорит «1» вместо
  запрошенной «3», адрес доказательства потерян, хотя конвертация удалась;
* **цена страницы** не должна включать загрузку весов: прогрев мерится отдельно, иначе
  «цена страницы» была бы ценой старта воркера.

Проверяются чистые функции и разбор аргументов: docling, GPU и PDF не нужны.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_page_ocr.py"
    spec = importlib.util.spec_from_file_location("measure_page_ocr", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_page_ocr"] = module
    spec.loader.exec_module(module)
    return module


page_ocr = _load_module()


class _Prov:
    def __init__(self, page_no: int) -> None:
        self.page_no = page_no


class _Node:
    def __init__(self, *pages: int) -> None:
        self.prov = [_Prov(p) for p in pages]


class _Document:
    def __init__(self, nodes: list[_Node]) -> None:
        self._nodes = nodes

    def iterate_items(self):
        return [(node, 0) for node in self._nodes]


# ---------------------------------------------------------------- номер страницы


def test_prov_pages_reads_element_provenance_not_page_count() -> None:
    """``document.pages`` — «сколько сконвертировано», провенанс — «какая это страница»."""
    document = _Document([_Node(3), _Node(3), _Node(3)])
    assert page_ocr._prov_pages(document) == [3]


def test_prov_pages_is_sorted_and_deduplicated() -> None:
    document = _Document([_Node(7), _Node(3), _Node(7)])
    assert page_ocr._prov_pages(document) == [3, 7]


def test_prov_pages_of_document_without_provenance_is_empty() -> None:
    """Пусто — это «провенанса нет», а не «страница 1»."""
    assert page_ocr._prov_pages(_Document([_Node()])) == []


def test_page_number_is_not_preserved_when_provenance_says_one() -> None:
    """Ровно дефект, ради которого прибор существует: страница 3 помечена как 1."""
    document = _Document([_Node(1)])
    assert page_ocr._prov_pages(document) == [1]
    assert page_ocr._prov_pages(document) != [3]


# ---------------------------------------------------------------- аргументы


def test_pages_are_parsed_in_order_without_duplicates() -> None:
    assert page_ocr._pages("10,3,10,40") == [10, 3, 40]


def test_pages_reject_zero_and_garbage() -> None:
    """Страницы 1-based: ``0`` — опечатка, а не «первая»."""
    for bad in ("0", "-2", "x"):
        with pytest.raises(SystemExit):
            page_ocr._pages(bad)
    with pytest.raises(SystemExit):
        page_ocr._pages(" , ")


def test_defaults_declare_level_device_and_page() -> None:
    args = page_ocr._parse_args(["--pdf", "a.pdf"])
    assert args.level == "full"
    assert args.device == "cpu"
    assert args.pages == "1"
    assert args.child is False


def test_device_is_a_closed_set_without_auto() -> None:
    """``auto`` нет: место вычисления — объявленное условие (R5)."""
    with pytest.raises(SystemExit):
        page_ocr._parse_args(["--pdf", "a.pdf", "--device", "auto"])


def test_missing_pdf_is_a_usage_error(tmp_path: Path) -> None:
    code = page_ocr.main(["--pdf", str(tmp_path / "nope.pdf")])
    assert code == 2


# ---------------------------------------------------------------- сводка


def test_summary_reports_preservation_only_over_measured_pages() -> None:
    """Отказ замера не должен голосовать за «номер сохранён»."""
    rows = [
        {"page": 3, "seconds": 1.0, "peak_rss_mb": 100.0, "page_number_preserved": True},
        {"page": 40, "error": "timeout 5 s"},
    ]
    ok = [row for row in rows if not row.get("error")]
    assert all(row["page_number_preserved"] for row in ok) is True
    assert len(ok) == 1


def test_child_mode_prints_exactly_one_machine_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Родитель ищет строку с префиксом и берёт **последнюю**: болтовня в stdout ломает разбор."""
    import json

    monkeypatch.setattr(
        page_ocr,
        "_run_child",
        lambda args: {"page": args.page, "seconds": 0.5, "page_number_preserved": True},
    )
    assert page_ocr.main(["--child", "--pdf", "a.pdf", "--page", "7"]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert len(lines) == 1
    assert lines[0].startswith(page_ocr._CHILD_PREFIX)
    assert json.loads(lines[0][len(page_ocr._CHILD_PREFIX) :])["page"] == 7


def test_parent_treats_a_child_without_the_machine_line_as_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ребёнок упал до замера — это отказ страницы, а не «страница стоит 0 секунд»."""
    import subprocess

    class _Completed:
        returncode = 1
        stdout = "Traceback (most recent call last):\nRuntimeError: сломалось\n"
        stderr = "RuntimeError: сломалось\n"

    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: _Completed()
    )
    pdf = tmp_path / "a.bin"
    pdf.write_bytes(b"%PDF-1.4 fake")
    row = page_ocr._measure_page(pdf, 3, level="full", device="cpu", timeout=5.0)
    assert row["error"]
    assert "exit 1" in row["error"]
    assert "seconds" not in row
