"""Пины прибора живости адресов эталона (O14 плана 26, D5).

Предмет — ровно те решения, на которых держится колонка `live_kind` листа на разметку:
класс по коду и конечному адресу, кодирование не-ASCII пути и **отказ идти по
редиректу**. Последнее — не педантизм: с авто-переходом `urlopen` возвращает `200` с
конечного адреса, класс `moved` становится недостижимым, и прибор отвечает «все живы»
именно там, где страница переехала (измерено 15.11.2026: `live` 102 из 102 вместо
98 + 4 `moved`).

Сеть здесь не нужна: проверяются функции, а не хосты.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "measure_golden_liveness.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_measure_golden_liveness", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


LIVE = _load_harness()


def test_same_address_answering_is_live() -> None:
    """Код 2xx на исходном адресе — `live`, а не `moved`."""
    assert LIVE._classify(200, "https://a.test/x", "https://a.test/x") == "live"  # type: ignore[attr-defined]
    # 206 — ответ на `Range`, то есть тот же `live`.
    assert LIVE._classify(206, "https://a.test/x", "https://a.test/x") == "live"  # type: ignore[attr-defined]


def test_other_final_address_is_moved() -> None:
    """Конечный адрес отличается — страница переехала, и это видно классом."""
    assert (
        LIVE._classify(  # type: ignore[attr-defined]
            200, "https://a.test/di", "https://a.test/dlh"
        )
        == "moved"
    )


def test_missing_and_forbidden_are_different_facts() -> None:
    """`gone` и `auth` — разные факты: «страницы нет» и «нас не пускают»."""
    assert LIVE._classify(404, "https://a.test/x", "https://a.test/x") == "gone"  # type: ignore[attr-defined]
    assert LIVE._classify(410, "https://a.test/x", "https://a.test/x") == "gone"  # type: ignore[attr-defined]
    assert LIVE._classify(403, "https://a.test/x", "https://a.test/x") == "auth"  # type: ignore[attr-defined]
    assert LIVE._classify(401, "https://a.test/x", "https://a.test/x") == "auth"  # type: ignore[attr-defined]


def test_unresolved_redirect_is_moved_not_live() -> None:
    """Редирект, до цели которого не дошли, — всё равно `moved` (адрес жив)."""
    assert LIVE._classify(301, "https://a.test/x", "https://a.test/x") == "moved"  # type: ignore[attr-defined]


def test_cyrillic_path_is_percent_encoded() -> None:
    """Путь из исходного документа может быть на кириллице — запрос обязан её закодировать."""
    encoded = LIVE._ascii_url(  # type: ignore[attr-defined]
        "https://jatoba.ru/Программа-ТП-2023.pdf"
    )
    assert encoded == (
        "https://jatoba.ru/%D0%9F%D1%80%D0%BE%D0%B3%D1%80%D0%B0%D0%BC%D0%BC%D0%B0"
        "-%D0%A2%D0%9F-2023.pdf"
    )
    # Уже закодированный путь не кодируется второй раз.
    assert LIVE._ascii_url(encoded) == encoded  # type: ignore[attr-defined]


def test_redirect_handler_refuses_to_follow() -> None:
    """Обработчик обязан **не** возвращать запрос: иначе `urlopen` пройдёт 301 молча."""
    handler = LIVE._NoRedirect()  # type: ignore[attr-defined]
    assert (
        handler.redirect_request(
            object(), object(), 301, "Moved", {}, "https://a.test/new"
        )
        is None
    )


def test_non_http_scheme_is_refused_without_request() -> None:
    """Схема из листа не считается доверенной: `file:` не открывается, причина названа."""
    result = LIVE.probe("file:///etc/passwd")  # type: ignore[attr-defined]
    assert result.code == -1
    assert result.kind == "unreachable"
    assert "http" in (result.error or "")


def test_urls_are_read_from_xlsx_and_csv_alike(tmp_path: Path) -> None:
    """Лист читается в обоих форматах: XLSX для разметки, CSV — машинный отчёт.

    Иначе «проверить живость» можно было бы только по одной из двух форм одного и того же
    листа, и вторая тихо отставала бы (тот самый класс: число из документа вместо
    артефакта).
    """
    import csv

    from openpyxl import Workbook

    book = Workbook()
    sheet = book.active
    sheet.append(["product_id", "source_url", "decision"])
    sheet.append(["p1", "https://a.test/x", ""])
    sheet.append(["p1", "https://a.test/y", ""])
    sheet.append(["p1", "https://a.test/x", ""])  # дубль адреса — не второй адрес
    xlsx = tmp_path / "sheet.xlsx"
    book.save(xlsx)

    csv_path = tmp_path / "sheet.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["product_id", "source_url", "decision"])
        writer.writerow(["p1", "https://a.test/x", ""])
        writer.writerow(["p1", "https://a.test/y", ""])
        writer.writerow(["p1", "https://a.test/x", ""])

    expected = ["https://a.test/x", "https://a.test/y"]
    assert LIVE._urls(xlsx) == expected  # type: ignore[attr-defined]
    assert LIVE._urls(csv_path) == expected  # type: ignore[attr-defined]
