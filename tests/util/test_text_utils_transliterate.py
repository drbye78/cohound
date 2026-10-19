"""Транслитерация RU→Latin для стабильных slug-идентификаторов."""

from __future__ import annotations

from core.util.text_utils import transliterate


def test_transliterate_cyrillic_product_name() -> None:
    """«Квант-Гибрид» → ``Kvant-Gibrid`` (регистр сохраняется)."""
    assert transliterate("Квант-Гибрид").lower() == "kvant-gibrid"


def test_transliterate_mixed_latin_and_cyrillic() -> None:
    """Латиница проходит без изменений, кириллица транслитерируется."""
    assert transliterate("Postgres Квант") == "Postgres Kvant"


def test_transliterate_special_cyrillic_letters() -> None:
    """ё/й/ъ/ь/ы/э/ю/я и верхний регистр покрыты таблицей."""
    assert transliterate("ёйъьыэюя") == "yoyyeyuya"
    assert transliterate("ЁЙЪЬЫЭЮЯ") == "YoYYEYuYa"
    assert transliterate("") == ""
