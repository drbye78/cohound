"""F0: класс работы конверсии и окно полосы (чистые функции, без I/O и сети).

Что доказывается:

1. **Таблица классов** (:func:`work_class_for`): контейнер сильнее PDF, PDF сильнее Office,
   остальное — ``model_free``; порядок правил проверяется на противоречивых входах
   (``notes.txt``, отданный как ``application/pdf``).
2. **Окно класса** (:func:`workers_for_class`): PDF-класс даёт **ровно** то же число, что
   прежняя :func:`workers_from_budget` (пин: полосы не должны менять прежнее окно PDF);
   ``model_free`` считается по своему измеренному пику; неизмеренный пик (Office) — окно 1
   **с причиной**, а не выдуманное число.
3. **Пики — измеренные числа, а не доли**: у каждого класса либо число из замера, либо
   названное «не измерено».
"""

from __future__ import annotations

import pytest

from core.domain.enums import DocumentLevel, PdfPipeline, WorkClass
from core.pipeline.resources import (
    DOCLING_PEAK_MB,
    MODEL_FREE_PEAK_MB,
    PDFIUM_TEXT_PEAK_MB,
    workers_for_class,
    workers_from_budget,
)
from core.pipeline.work_class import work_class_for

#: Бюджет, при котором ``model_free`` помещается трижды, а ``layout_tables`` — ни разу
#: (317 × 3 + 500 = 1451 < 3679 + 500). Числа берутся из кода, а не вписаны руками:
#: пересъёмка пика не должна ломать тест, но обязана менять ожидание.
BUDGET_TIGHT_MB = 500 + 3 * MODEL_FREE_PEAK_MB


# --------------------------------------------------------------------------- #
# (1) Таблица классов
# --------------------------------------------------------------------------- #

TABLE = [
    # 1. Контейнер — сильнее всего: архив не документ, его байты уходят в спуск.
    (
        {"content_type": "application/zip", "url": "https://x/a.zip"},
        WorkClass.CONTAINER,
    ),
    ({"content_type": None, "url": "https://x/a.7z"}, WorkClass.CONTAINER),
    ({"content_type": None, "url": "https://x/a.rar"}, WorkClass.CONTAINER),
    # ``x-tar`` — MIME сверх реестра (у tar нет читателя): класс назван, допуск — нет.
    (
        {"content_type": "application/x-tar", "url": "https://x/download?id=7"},
        WorkClass.CONTAINER,
    ),
    # 2. PDF — механизм задан осью, поэтому класс выбирает она.
    (
        {"content_type": "application/pdf", "url": "https://x/download?id=7"},
        WorkClass.PDF_STRUCTURED,
    ),
    ({"content_type": None, "url": "https://x/a.PDF"}, WorkClass.PDF_STRUCTURED),
    # Суффикс документа сильнее противоречащего заголовка (правило реестра).
    (
        {"content_type": "text/html", "url": "https://x/a.pdf"},
        WorkClass.PDF_STRUCTURED,
    ),
    # 3. Office — docling, но без уровня и устройства.
    (
        {
            "content_type": (
                "application/vnd.openxmlformats-officedocument."
                "wordprocessingml.document"
            ),
            "url": "https://x/download?id=7",
        },
        WorkClass.OFFICE,
    ),
    ({"content_type": None, "url": "https://x/a.docx"}, WorkClass.OFFICE),
    ({"content_type": None, "url": "https://x/a.pptx"}, WorkClass.OFFICE),
    ({"content_type": None, "url": "https://x/a.xlsx"}, WorkClass.OFFICE),
    # 4–6. HTML, текст, EPUB, пустой/неизвестный тип — читатели без моделей.
    ({"content_type": "text/html; charset=UTF-8", "url": "https://x/"}, WorkClass.MODEL_FREE),
    ({"content_type": "application/xhtml+xml", "url": "https://x/"}, WorkClass.MODEL_FREE),
    ({"content_type": None, "url": "https://x/page.html"}, WorkClass.MODEL_FREE),
    # Пустой/отсутствующий тип — фолбэк композита в HTML (wiring.build_worker_extractor).
    ({"content_type": None, "url": "https://x/"}, WorkClass.MODEL_FREE),
    ({"content_type": "", "url": "https://x/"}, WorkClass.MODEL_FREE),
    ({"content_type": "text/plain", "url": "https://x/a.txt"}, WorkClass.MODEL_FREE),
    ({"content_type": None, "url": "https://x/a.log"}, WorkClass.MODEL_FREE),
    ({"content_type": None, "url": "https://x/a.sql"}, WorkClass.MODEL_FREE),
    ({"content_type": None, "url": "https://x/a.rst"}, WorkClass.MODEL_FREE),
    ({"content_type": "text/markdown", "url": "https://x/a"}, WorkClass.MODEL_FREE),
    ({"content_type": None, "url": "https://x/a.md"}, WorkClass.MODEL_FREE),
    ({"content_type": "application/epub+zip", "url": "https://x/b"}, WorkClass.MODEL_FREE),
    ({"content_type": None, "url": "https://x/a.epub"}, WorkClass.MODEL_FREE),
    (
        {"content_type": "application/octet-stream", "url": "https://x/a.bin"},
        WorkClass.MODEL_FREE,
    ),
]


@pytest.mark.parametrize(("kwargs", "expected"), TABLE)
def test_work_class_table(kwargs: dict[str, str | None], expected: WorkClass) -> None:
    """Правила класса — таблицей: порядок правил и есть предмет проверки."""
    assert (
        work_class_for(
            **kwargs,  # type: ignore[arg-type]
            level=DocumentLevel.FULL,
            pdf_pipeline=PdfPipeline.STRUCTURED,
        )
        is expected
    )


def test_pdf_class_follows_declared_pipeline_not_suffix() -> None:
    """``pdf_pipeline=text_layer`` — другой класс (pdfium, моделей нет), тот же URL."""
    kwargs = {"content_type": "application/pdf", "url": "https://x/a.pdf"}
    assert (
        work_class_for(
            **kwargs, level=DocumentLevel.FULL, pdf_pipeline=PdfPipeline.TEXT_LAYER
        )
        is WorkClass.PDF_TEXT_LAYER
    )
    assert (
        work_class_for(
            **kwargs, level=DocumentLevel.FULL, pdf_pipeline=PdfPipeline.STRUCTURED
        )
        is WorkClass.PDF_STRUCTURED
    )


def test_level_does_not_change_the_class() -> None:
    """Уровень — множитель цены **внутри** класса, а не класс: ось не дробит полосы."""
    for level in DocumentLevel:
        assert (
            work_class_for(
                content_type="application/pdf",
                url="https://x/a.pdf",
                level=level,
                pdf_pipeline=PdfPipeline.STRUCTURED,
            )
            is WorkClass.PDF_STRUCTURED
        )


def test_container_member_is_classified_by_member_suffix() -> None:
    """Член контейнера (``{archive}!/{member}``) — свой класс по суффиксу члена."""
    assert (
        work_class_for(
            content_type=None,
            url="https://x/pack.zip!/docs/manual.pdf",
            level=DocumentLevel.FULL,
            pdf_pipeline=PdfPipeline.STRUCTURED,
        )
        is WorkClass.PDF_STRUCTURED
    )


# --------------------------------------------------------------------------- #
# (2) Окно класса
# --------------------------------------------------------------------------- #


def test_pdf_structured_window_equals_legacy_workers_from_budget() -> None:
    """Пин: окно PDF-класса — **то же число**, что прежняя формула, до последней единицы.

    Полосы не имеют права изменить окно документной экстракции: это условие, при котором
    прогон помещается в память, и оно уже проверено preflight'ом и замером.
    """
    for level in DocumentLevel:
        for budget in (0, 100, 500, 1451, 4186, 10_000, 100_000):
            for pipeline in PdfPipeline:
                for cpu_cap in (1, 4, 12):
                    assert workers_for_class(
                        WorkClass.PDF_STRUCTURED,
                        level,
                        pipeline,
                        budget,
                        cpu_cap=cpu_cap,
                    ) == (
                        workers_from_budget(
                            level,
                            budget,
                            cpu_cap=cpu_cap,
                            pdf_pipeline=PdfPipeline.STRUCTURED,
                        ),
                        None,
                    ), (level, budget, pipeline, cpu_cap)


def test_model_free_window_uses_its_own_measured_peak() -> None:
    """HTML-полоса считается по **своему** пику (317 MB), а не по пику docling.

    Это и есть предмет правки: одно окно на все классы морило страницу в очереди за PDF.
    """
    workers, reason = workers_for_class(
        WorkClass.MODEL_FREE,
        DocumentLevel.LAYOUT_TABLES,
        PdfPipeline.STRUCTURED,
        BUDGET_TIGHT_MB,
        cpu_cap=12,
    )
    assert reason is None
    assert workers == 3
    # Тот же бюджет для PDF-класса не помещает ни одного воркера — и это не ошибка.
    pdf_workers, pdf_reason = workers_for_class(
        WorkClass.PDF_STRUCTURED,
        DocumentLevel.LAYOUT_TABLES,
        PdfPipeline.STRUCTURED,
        BUDGET_TIGHT_MB,
        cpu_cap=12,
    )
    assert (pdf_workers, pdf_reason) == (0, None)


def test_pdf_text_layer_window_uses_pdfium_peak() -> None:
    """``text_layer`` — моделей нет: окно по измеренному пику pdfium, а не по docling."""
    workers, reason = workers_for_class(
        WorkClass.PDF_TEXT_LAYER,
        DocumentLevel.FULL,
        PdfPipeline.TEXT_LAYER,
        500 + 3 * PDFIUM_TEXT_PEAK_MB,
        cpu_cap=12,
    )
    assert (workers, reason) == (3, None)
    # VRAM-слагаемое к pdfium не применяется вовсе: карта не участвует (измеренный 0).
    assert workers_for_class(
        WorkClass.PDF_TEXT_LAYER,
        DocumentLevel.FULL,
        PdfPipeline.TEXT_LAYER,
        500 + 3 * PDFIUM_TEXT_PEAK_MB,
        usable_vram_mb=0,
        cpu_cap=12,
    ) == (3, None)


@pytest.mark.parametrize("work_class", [WorkClass.OFFICE, WorkClass.CONTAINER])
def test_unmeasured_peak_gives_window_one_with_reason(work_class: WorkClass) -> None:
    """Неизмеренный пик — окно 1 **и причина**, а не правдоподобное число (A3)."""
    workers, reason = workers_for_class(
        work_class, DocumentLevel.FULL, PdfPipeline.STRUCTURED, 100_000, cpu_cap=12
    )
    assert workers == 1
    assert reason == "work_class_peak_unmeasured"


def test_window_is_capped_by_cpu_and_reports_missing_budget() -> None:
    """Потолок CPU действует, а отсутствие бюджета называется, а не выдумывается."""
    huge = 500 + 100 * MODEL_FREE_PEAK_MB
    assert workers_for_class(
        WorkClass.MODEL_FREE, DocumentLevel.FULL, PdfPipeline.STRUCTURED, huge, cpu_cap=4
    ) == (4, None)
    assert workers_for_class(
        WorkClass.MODEL_FREE, DocumentLevel.FULL, PdfPipeline.STRUCTURED, None
    ) == (0, "work_class_budget_unknown")
    assert workers_for_class(
        WorkClass.MODEL_FREE, DocumentLevel.FULL, PdfPipeline.STRUCTURED, 400
    ) == (0, "work_class_budget_exhausted")


def test_measured_peaks_are_numbers_not_fractions() -> None:
    """Пики классов — измеренные константы; ``office`` в таблице нет намеренно."""
    assert MODEL_FREE_PEAK_MB > 0
    assert PDFIUM_TEXT_PEAK_MB > 0
    # ``off`` пика не имеет по построению (документы не извлекаются вовсе).
    assert set(DOCLING_PEAK_MB) == set(DocumentLevel) - {DocumentLevel.OFF}
    # Office-пика нет: подставлять пик PDF-уровня значило бы выдать чужой замер за свой.
    assert (
        workers_for_class(
            WorkClass.OFFICE, DocumentLevel.FULL, PdfPipeline.STRUCTURED, 100_000
        )[1]
        == "work_class_peak_unmeasured"
    )
