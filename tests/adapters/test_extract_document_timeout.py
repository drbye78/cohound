"""Таймаут **документа** как условие измерения: таблица уровней, ось и оба пути (O30).

Приёмка O30 состоит из двух проверок, и обе про одно: потолок извлечения документа не должен
быть ни зашитым числом «на глаз», ни свойством только пула.

* **Дефолт уровня** — `_LEVEL_DOCUMENT_TIMEOUT_S`. `full: None` — это не забывчивость, а
  записанное следствие: таймаута нет, значит слот воркера занят до внешнего guard-а
  (`future.result(timeout=…)` в пуле). Числа остальных уровней — щедрые намеренно: таймаут
  документа, выведенный из «типичного» документа, выбрасывал бы законные большие документы.
* **Guard есть на обоих путях** — пул и inline извлекают **один и тот же** экстрактор, поэтому
  потолок живёт в `_pdf_options` (``document_timeout`` пайплайна docling), а не в коде пула.
  Проверять надо именно это: если таймаут окажется свойством пула, inline-путь (а это
  `frozen`/отключённый пул) останется без потолка, и «guard есть» будет верно только наполовину.

Тесты не запускают docling: проверяются объявленная таблица и то, что адаптер кладёт в опции.
"""

from __future__ import annotations

import pytest

from core.adapters.extract_document import (
    _LEVEL_DOCUMENT_TIMEOUT_S,
    DoclingDocumentExtractor,
)
from core.domain.enums import DocumentLevel, PdfDevice


def _extractor(level: DocumentLevel, *, timeout: float | None = None) -> DoclingDocumentExtractor:
    return DoclingDocumentExtractor(level, pdf_device=PdfDevice.CPU, document_timeout=timeout)


def test_default_table_declares_a_value_for_every_level() -> None:
    """Каждый уровень объявлен явно: отсутствие ключа читалось бы как «нет таймаута»."""
    assert set(_LEVEL_DOCUMENT_TIMEOUT_S) == set(DocumentLevel)


def test_full_has_no_timeout_and_that_is_the_declared_state() -> None:
    """`full: None` — названное следствие «слот занят до внешнего guard-а», а не пробел."""
    assert _LEVEL_DOCUMENT_TIMEOUT_S[DocumentLevel.FULL] is None


def test_shorter_levels_have_shorter_timeouts() -> None:
    """Потолок растёт с уровнем: `text` дешевле `layout`, `layout` дешевле `layout_tables`.

    Иначе «щедрый» таймаут был бы щедрым одинаково для всех, то есть не выражал бы разницу
    цены уровней.
    """
    text = _LEVEL_DOCUMENT_TIMEOUT_S[DocumentLevel.TEXT]
    layout = _LEVEL_DOCUMENT_TIMEOUT_S[DocumentLevel.LAYOUT]
    tables = _LEVEL_DOCUMENT_TIMEOUT_S[DocumentLevel.LAYOUT_TABLES]
    assert text is not None and layout is not None and tables is not None
    assert text < layout < tables


def test_declared_axis_overrides_the_level_table() -> None:
    """Объявленная ось `crawl.document_timeout_s` приоритетнее таблицы — на любом уровне."""
    extractor = _extractor(DocumentLevel.LAYOUT, timeout=60.0)
    assert extractor._document_timeout == 60.0  # noqa: SLF001 — проверяется ровно поле


@pytest.mark.parametrize("level", [DocumentLevel.LAYOUT, DocumentLevel.LAYOUT_TABLES, DocumentLevel.FULL])
def test_pipeline_options_carry_the_level_timeout(level: DocumentLevel) -> None:
    """Guard живёт в опциях пайплайна ⇒ он общий для пула и для inline-пути.

    Проверяется значение, а не «объект создан»: `None` для `full` — тоже значение, и оно
    обязано доехать до docling именно как «без таймаута», а не быть подменено дефолтом
    библиотеки.
    """
    options = _extractor(level)._pdf_options()  # noqa: SLF001 — публичного входа нет
    assert getattr(options, "document_timeout", "нет поля") == _LEVEL_DOCUMENT_TIMEOUT_S[level]


def test_pipeline_options_carry_the_declared_axis_not_the_table() -> None:
    """Объявленное значение доезжает до docling, а не теряется по дороге."""
    options = _extractor(DocumentLevel.LAYOUT, timeout=45.0)._pdf_options()  # noqa: SLF001
    assert getattr(options, "document_timeout", None) == 45.0


def test_text_level_options_carry_the_timeout_too() -> None:
    """Ветка `text` (нативный пайплайн) — тоже путь извлечения, и у неё свой объект опций."""
    options = _extractor(DocumentLevel.TEXT)._pdf_options()  # noqa: SLF001
    assert getattr(options, "document_timeout", None) == _LEVEL_DOCUMENT_TIMEOUT_S[DocumentLevel.TEXT]
