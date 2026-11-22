"""Пины прибора ``scripts/measure_zh_retrieval.py`` — находимость в чужом по языку корпусе.

Два дефекта, которые этот прибор уже дал, и оба тихие: числа выглядят нормально, а
вопрос измерен не тот.

1. **Общий якорь.** Первая версия считала любой латинский токен параметра и получила
   47/68 при **всех трёх** настройках языка — `ru`, `en` и `all` дали одно число, потому
   что `PostgreSQL` (6 параметров), `GUI` (7) и `SQL` (5) находятся в любом чанке
   документации. Метрика мерила «страница вообще про СУБД», а не «достали материал этого
   параметра». Симптом был виден (ось не различает настроек), причина — нет.
2. **Знак дельты.** В первой версии печаталось «R0 → R2, дельта −0.0441», хотя R2 нашёл
   **больше** (0.7353 против 0.6912): вычиталось в обратную сторону. Читатель понял бы
   «dense хуже» из подписи, которая говорит обратное.

Тесты гоняют внутренние функции прибора напрямую: это арифметика и правило отбора, а не
CLI, и сеть/БД им не нужны.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from measure_zh_retrieval import (  # noqa: E402 — путь к прибору добавляется выше
    CellResult,
    _paired,
    anchor_census,
    anchor_visibility,
    is_anchor,
)

from core.domain.models import CatalogRow  # noqa: E402


def _row(param_id: str, title: str, terms: dict[str, list[str]] | None = None) -> CatalogRow:
    """Минимальная строка каталога: прибору нужны только ``title`` и ``concept_terms``."""
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="core",
        group="g",
        importance="Вж",
        param_type="fact",
        rule="правило",
        absence_rule="distribution_absence",
        role_binding="core_engine",
        concept_terms=terms or {},
    )


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("JSONB", True),  # всё в верхнем регистре
        ("SLA", True),
        ("pg_cron", True),  # подчёркивание
        ("ARM64", True),  # цифра
        ("pgBouncer", True),  # внутренняя заглавная
        ("support", False),  # обычное английское слово
        ("version", False),
        ("based", False),
        ("ab", False),  # короче трёх
    ],
)
def test_anchor_rule_is_declared(token: str, expected: bool) -> None:
    """Правило якоря: идентификатор, а не английское слово.

    Без него метрика превращается в «поиск что-нибудь нашёл»: ``support`` есть в любом
    чанке документации, и попадание перестаёт что-либо означать.
    """
    assert is_anchor(token) is expected


def test_shared_anchor_is_not_counted_for_any_param() -> None:
    """Якорь, который есть у двух параметров, не адресует ни один из них.

    Это и был первый дефект: попадание общего токена засчитывалось как находка материала
    параметра, и ось языка не различалась (47/68 при `ru`, `en` и `all`).
    """
    rows = [
        _row("p1", "Версия PostgreSQL в основе", {"en": ["PostgreSQL version"]}),
        _row("p2", "Поддержка PostgreSQL 15", {"en": ["PostgreSQL"]}),
        _row("p3", "Плагируемый TOAST", {"en": ["TOAST storage"]}),
    ]
    unique, df = anchor_census(rows)
    assert df["postgresql"] == 2
    assert unique["p1"] == []
    assert unique["p2"] == []
    # TOAST есть только у p3 — он и адресует параметр.
    assert unique["p3"] == ["TOAST"]


@pytest.mark.parametrize(
    ("title", "terms", "expected"),
    [
        ("Плагируемый TOAST", {}, "both"),  # якорь в заголовке — виден обоим плечам
        ("Хранилище", {"en": ["TOAST storage"], "ru": ["хранилище TOAST"]}, "both"),
        ("Хранилище", {"en": ["TOAST storage"], "ru": ["хранилище"]}, "en"),
        ("Хранилище", {"en": ["storage"], "ru": ["TOAST хранилище"]}, "ru"),
        ("Хранилище", {"en": ["storage"], "ru": ["хранилище"]}, "none"),
    ],
)
def test_anchor_visibility_separates_confounded_params(
    title: str, terms: dict[str, list[str]], expected: str
) -> None:
    """Якорь, живущий только в ``concept_terms.en``, попадает в англоязычный запрос дословно.

    Это смешение, найденное замером 22.11.2026 на русском корпусе: 6 «только en» параметров
    дали всю разницу +17.1 п.п., а на остальных 28 дискордансов было ноль. Поэтому класс
    параметра обязан считаться, иначе сравнение языков читается как «en лучше ищет».
    """
    row = _row("p1", title, terms)
    unique, _df = anchor_census([row])
    assert anchor_visibility(row, unique["p1"]) == expected


def _cell(param_id: str, lang: str, profile: str, hit: bool) -> CellResult:
    return CellResult(
        param_id=param_id,
        lang=lang,
        profile=profile,
        queries=("q",),
        chunks=1,
        hit=hit,
    )


def test_delta_sign_is_right_minus_left() -> None:
    """Знак дельты совпадает с подписью: «R0 → R2, +0.5» значит R2 нашёл больше.

    Первая версия печатала «R0 → R2, дельта −0.0441» при R2 0.7353 против R0 0.6912 —
    арифметика в обратную сторону, и подпись читалась наоборот.
    """
    cells = [
        _cell("a", "ru", "R0", False),
        _cell("a", "ru", "R2", True),
        _cell("b", "ru", "R0", False),
        _cell("b", "ru", "R2", True),
    ]
    item = _paired(cells, axis="profile", fixed="ru", left="R0", right="R2")
    assert item is not None
    assert item["right_rate"] == 1.0
    assert item["left_rate"] == 0.0
    assert item["delta"] == pytest.approx(1.0)
    assert item["only_right"] == 2
    assert item["only_left"] == 0


def test_lang_axis_compares_languages_at_fixed_profile() -> None:
    """Ось ``lang`` держит плечо фиксированным, иначе сравнивались бы две оси сразу."""
    cells = [
        _cell("a", "ru", "R0", False),
        _cell("a", "en", "R0", True),
        _cell("b", "ru", "R0", True),
        _cell("b", "en", "R0", True),
        # Другое плечо не должно попасть в сравнение языков.
        _cell("c", "ru", "R2", False),
        _cell("c", "en", "R2", True),
    ]
    item = _paired(cells, axis="lang", fixed="R0", left="ru", right="en")
    assert item is not None
    assert item["n"] == 2
    assert item["only_right"] == 1
    assert item["delta"] == pytest.approx(0.5)


def test_params_filter_keeps_only_named_parameters() -> None:
    """Чистое сравнение считается на подмножестве, а не на всех параметрах.

    Фильтр нужен именно для смешения: параметр с якорем только в ``concept_terms.en``
    не может попасть в русское плечо по построению запроса, и его нельзя держать в
    сравнении «какой язык лучше ищет».
    """
    cells = [
        _cell("a", "ru", "R0", False),
        _cell("a", "en", "R0", True),
        _cell("b", "ru", "R0", True),
        _cell("b", "en", "R0", True),
    ]
    item = _paired(cells, axis="lang", fixed="R0", left="ru", right="en", params={"b"})
    assert item is not None
    assert item["n"] == 1
    assert item["delta"] == pytest.approx(0.0)
    assert item["only_right"] == 0


def test_unpaired_params_are_dropped_not_zeroed() -> None:
    """Параметр без обеих настроек выпадает из пары, а не считается «не попал».

    Иначе «не мерили» превратилось бы в «не нашли», и дельта поехала бы составом.
    """
    cells = [
        _cell("a", "ru", "R0", True),
        _cell("a", "ru", "R2", True),
        _cell("b", "ru", "R0", False),  # у b нет R2
    ]
    item = _paired(cells, axis="profile", fixed="ru", left="R0", right="R2")
    assert item is not None
    assert item["n"] == 1
    assert item["delta"] == pytest.approx(0.0)
