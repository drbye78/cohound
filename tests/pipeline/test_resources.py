"""Тесты :mod:`core.pipeline.resources` (F0, `11` §2.10/§3.3).

Покрывают: вывод числа воркеров экстракции из бюджета памяти (границы формулы, «не
помещается» как валидный ответ), резолв бюджета (явный ``COHOUND_MEM_BUDGET_MB`` >
``MemAvailable − reserve``) и его провенанс, сообщение :class:`MemoryBudgetError`
(числа, а не «не помещается»), классификацию семейства формата по URL, nearest-rank
распределение чанков на документ, сборку блока ``resources`` для ``metrics.json``
(объединение фаз, нейтральность при отсутствии наблюдения) и чтение пика RSS /
свободной памяти с фолбэком на ``resource.getrusage``.

**Почему числа закреплены литералами.** Пики docling и пол родителя — измеренный факт
(``docs/reports/measurements/2026-10-23-docling-level-peaks.md``), а не параметр
реализации: тест обязан сверяться с замером, иначе константа модуля проверит сама себя.
Пересъёмка меняет число воркеров, и это должно ломать тест — видимо, а не молча.

Реальная память хоста читается ровно в двух тестах (помечены ``LINUX_ONLY``);
все остальные идут через ``available_mb=…`` или подменённый ``resources.Path``.
"""

from __future__ import annotations

import resource
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain.enums import DocumentLevel, PdfDevice, PdfPipeline
from core.pipeline import resources
from core.pipeline.resources import (
    MEM_BUDGET_ENV,
    MEM_RESERVE_MB,
    PARENT_FLOOR_MB,
    MemoryBudgetError,
    ResourcesObservation,
    build_resources_block,
    document_family,
    mem_budget_provenance,
    read_available_mb,
    read_peak_rss_mb,
    resolve_mem_budget_mb,
    summarize_chunks_per_doc,
    workers_from_budget,
)

#: Замеренные пики docling на один документ (MB): пер-уровневые —
#: ``2026-10-23-docling-level-peaks.md``, ``layout_tables`` —
#: ``2026-10-24-docling-peaks-real-corpus.md`` (максимум по реальному корпусу, 2763 стр.).
#: ``full`` = 4833 — 3679 (измеренный реальный ``layout_tables``) + 1154 (синтетическая
#: дельта OCR+ACCURATE против FAST без OCR): прежнее 3107 было **ниже** ``layout_tables``,
#: то есть ошибка перестала быть консервативной.
PEAK_TEXT = 855
PEAK_LAYOUT = 1563
PEAK_LAYOUT_TABLES = 3679
PEAK_FULL = 4833

#: Бюджет хоста 6.9 GiB, округлённый вниз до MB (тот же замер).
HOST_BUDGET_MB = 6500

LINUX_ONLY = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="пик RSS и MemAvailable читаются только из procfs Linux",
)


class _FakeProcFile:
    """Подмена файла ``/proc``: ``text=None`` — файла нет (``OSError``)."""

    def __init__(self, text: str | None) -> None:
        self._text = text

    def read_text(self, encoding: str = "utf-8") -> str:
        if self._text is None:
            raise OSError("procfs недоступен")
        return self._text


def _fake_proc(monkeypatch: pytest.MonkeyPatch, text: str | None) -> None:
    """Заменяет ``resources.Path``: тест не читает реальную память хоста."""
    monkeypatch.setattr(resources, "Path", lambda path: _FakeProcFile(text))


# --------------------------------------------------------------------------- #
# workers_from_budget — границы формулы
# --------------------------------------------------------------------------- #


def test_workers_from_budget_one_worker_at_exact_fit() -> None:
    """Граница включительна: ``budget = floor + peak`` даёт 1 воркер, а не 0."""
    assert workers_from_budget(DocumentLevel.TEXT, PARENT_FLOOR_MB + PEAK_TEXT) == 1
    assert workers_from_budget(DocumentLevel.FULL, PARENT_FLOOR_MB + PEAK_FULL) == 1


def test_workers_from_budget_two_workers_at_exact_fit() -> None:
    assert workers_from_budget(DocumentLevel.TEXT, PARENT_FLOOR_MB + 2 * PEAK_TEXT, cpu_cap=8) == 2


def test_workers_from_budget_one_mb_below_threshold_drops_one() -> None:
    """Не «примерно три»: за MB до порога третий воркер уже не помещается."""
    threshold = PARENT_FLOOR_MB + 3 * PEAK_TEXT  # 3065 MB
    assert workers_from_budget(DocumentLevel.TEXT, threshold, cpu_cap=8) == 3
    assert workers_from_budget(DocumentLevel.TEXT, threshold - 1, cpu_cap=8) == 2

def test_workers_from_budget_is_capped_by_cpu() -> None:
    """Память разрешила бы 148 процессов — CPU оставляет столько, сколько успеет.

    Без потолка хост с запасом RAM получил бы абсурд: ``text`` 855 MB на 128 GB — это 148
    процессов docling на 12 ядрах, то есть потеря на переключениях и на инициализации
    конвертера в каждом воркере. Формула — ``min(по памяти, по CPU)``: объём работы не
    ограничен, ограничено число одновременных исполнителей.
    """
    huge_budget = PARENT_FLOOR_MB + 148 * PEAK_TEXT
    assert workers_from_budget(DocumentLevel.TEXT, huge_budget, cpu_cap=12) == 12
    assert workers_from_budget(DocumentLevel.TEXT, huge_budget, cpu_cap=1) == 1
    # Потолок по CPU не воскрешает непомещающуюся конфигурацию.
    assert workers_from_budget(DocumentLevel.FULL, PARENT_FLOOR_MB, cpu_cap=64) == 0
    assert workers_from_budget(DocumentLevel.FULL, None, cpu_cap=64) == 0

def test_workers_from_budget_default_cap_is_host_cpu() -> None:
    """Без явного ``cpu_cap`` потолок — факт хоста, и он не меньше 1."""
    assert resources.cpu_parallelism_cap() >= 1
    huge_budget = PARENT_FLOOR_MB + 1000 * PEAK_TEXT
    workers = workers_from_budget(DocumentLevel.TEXT, huge_budget)
    assert workers <= resources.cpu_parallelism_cap()


@pytest.mark.parametrize("budget_mb", [PARENT_FLOOR_MB, PARENT_FLOOR_MB - 1, 0])
def test_workers_from_budget_within_parent_floor_is_zero(budget_mb: int) -> None:
    """Пол родителя занят до воркеров: свободного места нет — воркеров нет."""
    assert workers_from_budget(DocumentLevel.TEXT, budget_mb) == 0


def test_workers_from_budget_unknown_budget_is_zero() -> None:
    """Бюджет неизвестен — выводить нечего (не подменять «возьми один»)."""
    assert workers_from_budget(DocumentLevel.FULL, None) == 0


def test_workers_from_budget_off_level_is_zero() -> None:
    """``off`` — документной экстракции нет; 0 здесь не значит «не помещается»."""
    assert workers_from_budget(DocumentLevel.OFF, HOST_BUDGET_MB) == 0
    assert resources.docling_peak_mb(DocumentLevel.OFF) is None


def test_pdf_peak_mb_text_layer_is_the_measured_pdfium_peak() -> None:
    """``text_layer`` — пик **своего** стека (134 MB), а не пик docling'а.

    Источник — замер на всём PDF-корпусе ``corpus_raw/postgrespro`` (240 документов,
    15 929 страниц, 20.9 млн символов): пик процесса 134 MB. Подстановка пика docling
    (855 MB для ``text``) дала бы один воркер там, где помещается восемь, то есть
    превратила бы выигрыш оси в его отсутствие.
    """
    peak = resources.pdf_peak_mb(DocumentLevel.LAYOUT_TABLES, PdfPipeline.TEXT_LAYER)
    assert peak == 134
    assert peak == resources.PDFIUM_TEXT_PEAK_MB
    # уровень к ``text_layer`` не применяется: у механизма нет ступеней
    assert (
        resources.pdf_peak_mb(DocumentLevel.FULL, PdfPipeline.TEXT_LAYER)
        == resources.pdf_peak_mb(DocumentLevel.TEXT, PdfPipeline.TEXT_LAYER)
    )


def test_pdf_peak_mb_structured_keeps_docling_table() -> None:
    """``structured`` — прежняя таблица docling'а, значение по умолчанию не изменилось."""
    assert (
        resources.pdf_peak_mb(DocumentLevel.LAYOUT_TABLES) == resources.DOCLING_PEAK_MB[
            DocumentLevel.LAYOUT_TABLES
        ]
    )
    assert resources.pdf_peak_mb(DocumentLevel.OFF) is None


def test_workers_from_budget_text_layer_uses_own_peak() -> None:
    """Окно ``text_layer`` выводится по его пику: 8 воркеров там, где docling берёт 1.

    При бюджете 5128 MB и ``layout_tables``: docling `floor((5128−500)/3679)` = 1,
    ``text_layer`` `floor(4628/134)` = 34, ограничено CPU (8). Это не «щедрость»: 8 × 134 MB
    = 1072 MB против 4179 MB у одного docling-воркера.
    """
    budget = 5128
    assert (
        workers_from_budget(DocumentLevel.LAYOUT_TABLES, budget, cpu_cap=8) == 1
    )
    assert (
        workers_from_budget(
            DocumentLevel.LAYOUT_TABLES,
            budget,
            cpu_cap=8,
            pdf_pipeline=PdfPipeline.TEXT_LAYER,
        )
        == 8
    )


def test_workers_from_budget_text_layer_zero_below_floor() -> None:
    """Пик мал, но пол родителя всё равно не обходится: ниже него — 0, а не 1."""
    assert (
        workers_from_budget(
            DocumentLevel.TEXT,
            resources.PARENT_FLOOR_MB + resources.PDFIUM_TEXT_PEAK_MB - 1,
            cpu_cap=8,
            pdf_pipeline=PdfPipeline.TEXT_LAYER,
        )
        == 0
    )


def test_workers_from_budget_matches_measured_host() -> None:
    """Хост 6.9 GiB: числа следуют из замера реального корпуса, а не из предвидения (§3.3).

    ``layout_tables`` — 3679 MB (2763-страничный документ корпуса), поэтому даже при
    бюджете 6500 MB помещается **один** воркер: 500 + 2 × 3679 = 7858 MB. Это не
    «осторожность», а арифметика: прежняя константа 1953 MB давала здесь 3 воркера и
    была бы OOM-kill'ом на первом же большом PDF.
    """
    assert workers_from_budget(DocumentLevel.LAYOUT_TABLES, HOST_BUDGET_MB, cpu_cap=8) == 1
    assert workers_from_budget(DocumentLevel.FULL, HOST_BUDGET_MB, cpu_cap=8) == 1
    # Один воркер помещается ровно с полом родителя: 500 + 3679 = 4179 MB.
    assert workers_from_budget(DocumentLevel.LAYOUT_TABLES, 4179, cpu_cap=8) == 1
    assert workers_from_budget(DocumentLevel.LAYOUT_TABLES, 4178, cpu_cap=8) == 0


# --------------------------------------------------------------------------- #
# P9: окно — минимум по двум ограничениям (host-память и VRAM)
# --------------------------------------------------------------------------- #


def test_workers_from_budget_cpu_device_ignores_vram() -> None:
    """Под ``cpu`` VRAM-слагаемого нет вовсе: параметр даже не читается.

    ``usable_vram_mb=0`` при ``cpu`` не должен обнулить окно — иначе объявление
    устройством CPU стоило бы воркеров на машине, где видеопамяти просто нет.
    """
    assert (
        workers_from_budget(
            DocumentLevel.LAYOUT_TABLES,
            HOST_BUDGET_MB,
            cpu_cap=8,
            pdf_device=PdfDevice.CPU,
            usable_vram_mb=0,
        )
        == 1
    )


def test_workers_from_budget_cuda_is_capped_by_vram() -> None:
    """``cuda``: окно ограничено и видеопамятью — по замеренному пику 817 MB.

    Конфигурация, ради которой слагаемое существует: host-памяти много, карта маленькая.
    Без него формула вывела бы 8 воркеров (потолок CPU) и упала бы в CUDA OOM.
    ``usable_vram`` здесь явный: тест не должен зависеть от карты хоста.
    """
    budget = PARENT_FLOOR_MB + 100 * PEAK_LAYOUT_TABLES  # host разрешает много
    peak = resources.DOCLING_VRAM_PEAK_MB[DocumentLevel.LAYOUT_TABLES]
    assert peak == 817  # замер: 665-стр. документ на RTX 3050, layout_tables
    assert (
        workers_from_budget(
            DocumentLevel.LAYOUT_TABLES,
            budget,
            cpu_cap=8,
            pdf_device=PdfDevice.CUDA,
            usable_vram_mb=2 * peak,
        )
        == 2
    )
    # Меньше одного пика — ноль: «не помещается», а не «возьми один».
    assert (
        workers_from_budget(
            DocumentLevel.LAYOUT_TABLES,
            budget,
            cpu_cap=8,
            pdf_device=PdfDevice.CUDA,
            usable_vram_mb=peak - 1,
        )
        == 0
    )


def test_workers_from_budget_cuda_result_is_min_of_both_limits() -> None:
    """Минимум, а не произведение: связывает **то** ограничение, которое меньше."""
    # Host даёт 1 воркер, VRAM — 4: побеждает host.
    assert (
        workers_from_budget(
            DocumentLevel.LAYOUT_TABLES,
            PARENT_FLOOR_MB + PEAK_LAYOUT_TABLES,
            cpu_cap=8,
            pdf_device=PdfDevice.CUDA,
            usable_vram_mb=4 * 817,
        )
        == 1
    )
    # Host даёт 4, VRAM — 1: побеждает VRAM.
    assert (
        workers_from_budget(
            DocumentLevel.LAYOUT_TABLES,
            PARENT_FLOOR_MB + 4 * PEAK_LAYOUT_TABLES,
            cpu_cap=8,
            pdf_device=PdfDevice.CUDA,
            usable_vram_mb=817,
        )
        == 1
    )


def test_workers_from_budget_cuda_unmeasured_level_does_not_invent_a_peak() -> None:
    """Уровень без VRAM-замера (``full``) не ограничивается выдуманным числом.

    ``None`` в :func:`vram_peak_mb` — «не измеряли», а не «бесплатно»: окно считает
    host-память и CPU, а факт отсутствия VRAM-границы виден в preflight (A3).
    """
    assert resources.vram_peak_mb(DocumentLevel.FULL) is None
    big = PARENT_FLOOR_MB + 2 * PEAK_FULL
    assert (
        workers_from_budget(
            DocumentLevel.FULL,
            big,
            cpu_cap=8,
            pdf_device=PdfDevice.CUDA,
            usable_vram_mb=817,  # намеренно мало: пик уровня не измерен — не применяется
        )
        == 2
    )


def test_vram_peak_text_layer_is_zero_not_unknown() -> None:
    """``text_layer`` (pdfium) в VRAM не ходит — измеренный 0, а не «неизвестно».

    Разница существенна: ``0`` не ограничивает окно, ``None`` означал бы «не смотрели».
    """
    assert (
        resources.vram_peak_mb(DocumentLevel.LAYOUT_TABLES, PdfPipeline.TEXT_LAYER) == 0
    )
    assert resources.vram_peak_mb(DocumentLevel.TEXT) == 0


def test_read_usable_vram_mb_reserves_headroom(monkeypatch: pytest.MonkeyPatch) -> None:
    """``usable = total − reserve``: карта целиком прогону не отдаётся.

    Резерв — не потолок, а цена того, чтобы прогон не оказался CUDA-OOM для рабочего
    стола: дисплей и первый CUDA-контекст занимают память до воркеров.
    """
    monkeypatch.setattr(resources, "cuda_total_vram_mb", lambda: 4095)
    assert resources.read_usable_vram_mb() == 4095 - resources.VRAM_RESERVE_MB
    # Карта меньше резерва — 0 («по VRAM не помещается»), а не отрицательное число.
    monkeypatch.setattr(resources, "cuda_total_vram_mb", lambda: 100)
    assert resources.read_usable_vram_mb() == 0
    # Устройства нет — «неизвестно», а не ноль.
    monkeypatch.setattr(resources, "cuda_total_vram_mb", lambda: None)
    assert resources.read_usable_vram_mb() is None


# --------------------------------------------------------------------------- #
# resolve_mem_budget_mb — объявленный вход сильнее измерения
# --------------------------------------------------------------------------- #


def test_resolve_mem_budget_env_wins_over_available() -> None:
    assert resolve_mem_budget_mb(
        env={MEM_BUDGET_ENV: "4000"}, available_mb=HOST_BUDGET_MB
    ) == 4000


@pytest.mark.parametrize("raw", ["", "   ", "abc", "0", "-5"])
def test_resolve_mem_budget_ignores_invalid_env(raw: str) -> None:
    """Мусор в env не роняет старт: бюджет считается от ``MemAvailable − резерв``."""
    env = {MEM_BUDGET_ENV: raw}
    assert resolve_mem_budget_mb(env=env, available_mb=HOST_BUDGET_MB) == (
        HOST_BUDGET_MB - MEM_RESERVE_MB
    )


def test_resolve_mem_budget_reads_process_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """``env=None`` — это окружение процесса (пробелы вокруг числа допускаются)."""
    monkeypatch.setenv(MEM_BUDGET_ENV, " 4321 ")
    assert resolve_mem_budget_mb(available_mb=HOST_BUDGET_MB) == 4321


def test_resolve_mem_budget_unknown_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ни env, ни ``/proc/meminfo`` — ``None``, а не придуманное число."""
    _fake_proc(monkeypatch, None)
    assert resolve_mem_budget_mb(env={}, available_mb=None) is None


@pytest.mark.parametrize("available_mb", [100, MEM_RESERVE_MB - 1])
def test_resolve_mem_budget_never_negative(available_mb: int) -> None:
    """Резерв больше доступного — бюджет 0, а не отрицательное число."""
    assert resolve_mem_budget_mb(env={}, available_mb=available_mb) == 0


# --------------------------------------------------------------------------- #
# MemoryBudgetError — отказ, который можно исправить по сообщению
# --------------------------------------------------------------------------- #


def _budget_error(
    *,
    budget_mb: int | None,
    peak_mb: int | None,
    level: DocumentLevel = DocumentLevel.FULL,
) -> MemoryBudgetError:
    """Отказ с заданными числами (``workers=0`` — как его строит preflight)."""
    return MemoryBudgetError(
        budget_mb=budget_mb,
        level=level,
        peak_mb=peak_mb,
        parent_floor_mb=PARENT_FLOOR_MB,
        workers=0,
    )


def test_memory_budget_error_names_numbers() -> None:
    """Сообщение обязано называть бюджет, уровень, пик и пол родителя."""
    error = _budget_error(budget_mb=1000, peak_mb=PEAK_FULL)
    message = str(error)
    assert error.describe() == message
    assert "1000" in message
    assert "full" in message
    assert str(PEAK_FULL) in message
    assert str(PARENT_FLOOR_MB) in message
    assert "workers=0" in message
    assert "None" not in message


def test_memory_budget_error_without_numbers_does_not_print_none() -> None:
    """Неизвестный бюджет/пик — словом, а не ``None`` в операторском сообщении."""
    message = _budget_error(
        budget_mb=None, peak_mb=None, level=DocumentLevel.TEXT
    ).describe()
    assert "None" not in message
    assert message.count("неизвестен") == 2
    assert "text" in message


# --------------------------------------------------------------------------- #
# mem_budget_provenance — источник числа, а не догадка о нём
# --------------------------------------------------------------------------- #


def test_mem_budget_provenance_env_source() -> None:
    provenance = mem_budget_provenance(
        env={MEM_BUDGET_ENV: "4000"}, available_mb=HOST_BUDGET_MB
    )
    assert provenance["source"] == "env"
    assert provenance["mem_budget_mb"] == 4000
    assert provenance["available_mb"] == HOST_BUDGET_MB
    assert provenance["reserve_mb"] == MEM_RESERVE_MB
    assert provenance["parent_floor_mb"] == PARENT_FLOOR_MB


def test_mem_budget_provenance_available_source() -> None:
    provenance = mem_budget_provenance(env={}, available_mb=HOST_BUDGET_MB)
    assert provenance["source"] == "available"
    assert provenance["mem_budget_mb"] == HOST_BUDGET_MB - MEM_RESERVE_MB


def test_mem_budget_provenance_unknown_source(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_proc(monkeypatch, None)
    provenance = mem_budget_provenance(env={}, available_mb=None)
    assert provenance["source"] == "unknown"
    assert provenance["mem_budget_mb"] is None
    assert provenance["available_mb"] is None


@pytest.mark.parametrize("raw", ["abc", "0", "-5", "   "])
def test_mem_budget_provenance_rejected_env_is_not_labelled_env(raw: str) -> None:
    """Непринятый env не выдаёт себя за объявленный бюджет (провенанс не врёт).

    ``resolve_mem_budget_mb`` такой мусор игнорирует и считает от ``MemAvailable``;
    если бы метка оставалась ``env``, ``metrics.json`` утверждал бы «бюджет объявлен
    оператором» при числе из свободной памяти — ложь ровно в том сценарии, ради
    которого провенанс и пишется (оператор опечатался).
    """
    provenance = mem_budget_provenance(
        env={MEM_BUDGET_ENV: raw}, available_mb=HOST_BUDGET_MB
    )
    assert provenance["source"] == "available"
    assert provenance["mem_budget_mb"] == HOST_BUDGET_MB - MEM_RESERVE_MB


def test_mem_budget_provenance_publishes_every_peak() -> None:
    """Все четыре уровня замера попадают в метрику (``off`` пика не имеет)."""
    provenance = mem_budget_provenance(
        env={MEM_BUDGET_ENV: "1"}, available_mb=HOST_BUDGET_MB
    )
    assert provenance["docling_peak_mb"] == {
        "text": PEAK_TEXT,
        "layout": PEAK_LAYOUT,
        "layout_tables": PEAK_LAYOUT_TABLES,
        "full": PEAK_FULL,
    }


# --------------------------------------------------------------------------- #
# document_family — семейство формата по URL (без чтения байтов)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://example.test/files/report.pdf", resources.FAMILY_PDF),
        ("https://example.test/files/report.pdf?dl=1#page=2", resources.FAMILY_PDF),
        ("https://example.test/files/REPORT.PDF", resources.FAMILY_PDF),
        ("https://example.test/files/handout.docx", resources.FAMILY_OFFICE),
        ("https://example.test/slides.pptx", resources.FAMILY_OFFICE),
        ("https://example.test/sheet.xlsx", resources.FAMILY_OFFICE),
        ("https://example.test/book.epub", resources.FAMILY_EPUB),
        ("https://example.test/notes.md", resources.FAMILY_TEXT),
        ("https://example.test/readme.txt", resources.FAMILY_TEXT),
        ("https://example.test/docs/guide", resources.FAMILY_HTML),
        ("https://example.test", resources.FAMILY_HTML),
        # Член контейнера классифицируется по суффиксу **члена**, не контейнера.
        ("https://example.test/a.zip!/pdf/manual.pdf", resources.FAMILY_PDF),
        ("https://example.test/a.zip!/notes/readme.md", resources.FAMILY_TEXT),
        ("not a url", resources.FAMILY_OTHER),
        ("", resources.FAMILY_OTHER),
    ],
)
def test_document_family_by_url(url: str, expected: str) -> None:
    assert document_family(url) == expected


# --------------------------------------------------------------------------- #
# summarize_chunks_per_doc — nearest-rank, без интерполяции
# --------------------------------------------------------------------------- #


def test_summarize_chunks_per_doc_single_document() -> None:
    assert summarize_chunks_per_doc([("https://example.test/a.pdf", 7)]) == {
        "pdf": {"count": 1, "p50": 7.0, "p90": 7.0, "max": 7.0}
    }


def test_summarize_chunks_per_doc_two_documents_is_nearest_rank() -> None:
    """Два документа: p50 — нижнее наблюдённое, p90 — верхнее (не 7.0 и не 8.2)."""
    summary = summarize_chunks_per_doc(
        [("https://example.test/a.pdf", 9), ("https://example.test/b.pdf", 5)]
    )
    assert summary["pdf"] == {"count": 2, "p50": 5.0, "p90": 9.0, "max": 9.0}


def test_summarize_chunks_per_doc_ten_documents_is_nearest_rank() -> None:
    """Интерполяция дала бы 5.5/9.1 — перцентиль обязан быть наблюдённым числом."""
    documents = [(f"https://example.test/p{index}", index) for index in range(1, 11)]
    summary = summarize_chunks_per_doc(documents)
    assert summary["html"] == {"count": 10, "p50": 5.0, "p90": 9.0, "max": 10.0}


def test_summarize_chunks_per_doc_skips_absent_families() -> None:
    """Отсутствующее семейство не появляется нулями: ноль значил бы «чанков нет»."""
    summary = summarize_chunks_per_doc([("https://example.test/a.pdf", 3)])
    assert list(summary) == ["pdf"]
    assert "html" not in summary


def test_summarize_chunks_per_doc_keeps_zero_chunk_document() -> None:
    """Документ без чанков — факт (нарезка не дала ничего), он входит в ``count``."""
    summary = summarize_chunks_per_doc(
        [("https://example.test/a.pdf", 0), ("https://example.test/b.html", 4)]
    )
    assert summary["pdf"] == {"count": 1, "p50": 0.0, "p90": 0.0, "max": 0.0}
    assert summary["html"] == {"count": 1, "p50": 4.0, "p90": 4.0, "max": 4.0}


def test_summarize_chunks_per_doc_order_is_deterministic() -> None:
    """Порядок входа не влияет ни на значения, ни на порядок ключей (детерминизм)."""
    documents = [
        ("https://example.test/a.pdf", 3),
        ("https://example.test/p.html", 5),
        ("https://example.test/n.md", 1),
    ]
    first = summarize_chunks_per_doc(documents)
    second = summarize_chunks_per_doc(list(reversed(documents)))
    assert first == second
    assert list(first) == ["html", "pdf", "text"]


# --------------------------------------------------------------------------- #
# build_resources_block — блок metrics.json.resources
# --------------------------------------------------------------------------- #


def test_build_resources_block_merges_phase_keys() -> None:
    """Пик без длительности и длительность без пика — оба наблюдаемых факта."""
    block = build_resources_block(
        phase_peaks={"corpus": 1234.56},
        phase_durations={"corpus": 12.34, "evaluate": 7.04},
    )
    assert list(block["phases"]) == ["corpus", "evaluate"]
    assert block["phases"]["corpus"] == {"peak_rss_mb": 1234.6, "wall_s": 12.3}
    assert block["phases"]["evaluate"] == {"peak_rss_mb": None, "wall_s": 7.0}


def test_build_resources_block_without_observation_is_neutral() -> None:
    """Не измеряли — ``None``/``{}``, а не нули, которые кто-то прочтёт как факт."""
    assert build_resources_block() == {
        "phases": {},
        "scheduling": {
            "extract_workers": None,
            "extract_max_tasks_per_child": None,
            "source": "declared",
        },
        "chunks_per_doc": {},
        "documents": 0,
        "chunks": 0,
        "workers_peak_rss_mb": None,
        "budget": {},
        "stage_timings": {},
    }


def test_build_resources_block_keeps_observation_facts() -> None:
    observation = ResourcesObservation(
        chunks_per_doc={"pdf": {"count": 2, "p50": 5.0, "p90": 9.0, "max": 9.0}},
        documents=2,
        chunks=14,
        extract_workers=3,
        extract_max_tasks_per_child=50,
        scheduling_source="derived",
        workers_peak_rss_mb=1234.44,
    )
    block = build_resources_block(observation=observation)
    assert block["scheduling"] == {
        "extract_workers": 3,
        "extract_max_tasks_per_child": 50,
        "source": "derived",
    }
    assert block["chunks_per_doc"] == observation.chunks_per_doc
    assert (block["documents"], block["chunks"]) == (2, 14)
    assert block["workers_peak_rss_mb"] == 1234.4


def test_build_resources_block_absent_pool_peak_is_not_zero() -> None:
    """Пула не было — пика воркера нет; 0 MB был бы выдуманным измерением."""
    block = build_resources_block(observation=ResourcesObservation())
    assert block["workers_peak_rss_mb"] is None
    assert block["scheduling"]["extract_workers"] is None
    assert block["scheduling"]["source"] == "declared"


def test_build_resources_block_budget_round_trip() -> None:
    """Провенанс бюджета доезжает до метрики целиком; ``{}`` остаётся ``{}``."""
    provenance: dict[str, Any] = mem_budget_provenance(
        env={}, available_mb=HOST_BUDGET_MB
    )
    assert build_resources_block(budget=provenance)["budget"] == provenance
    assert build_resources_block(budget={})["budget"] == {}


# --------------------------------------------------------------------------- #
# read_peak_rss_mb / read_available_mb — измерение и честный фолбэк
# --------------------------------------------------------------------------- #


@LINUX_ONLY
def test_read_peak_rss_mb_positive_on_this_host() -> None:
    value = read_peak_rss_mb()
    assert isinstance(value, float)
    assert value > 0


@LINUX_ONLY
def test_read_available_mb_positive_on_this_host() -> None:
    value = read_available_mb()
    assert isinstance(value, int)
    assert value > 0


def _rusage_mb() -> float:
    """Ожидание фолбэка: ``ru_maxrss`` (Linux — KiB) в MB, с тем же округлением.

    Округление до 1 знака — часть контракта ``read_peak_rss_mb`` (метрики без
    float-мусора), поэтому ожидание округляется так же; ``approx`` с узким допуском
    остаётся, потому что пик процесса может вырасти между двумя чтениями.
    """
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)


def test_read_peak_rss_mb_falls_back_to_getrusage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Нет ``/proc/self/status`` — читается ``ru_maxrss`` того же процесса.

    ``ru_maxrss`` монотонен и растёт от собственных аллокаций теста, поэтому сравнивать
    его с более поздним чтением нельзя (окно «между двумя чтениями» — не константа):
    берётся снимок **до** вызова и проверяется, что фолбэк вернул значение внутри
    ``[до, после]``.
    """
    before = _rusage_mb()
    _fake_proc(monkeypatch, None)
    value = read_peak_rss_mb()
    assert value is not None
    assert value > 0
    assert before <= value + 0.2
    assert value <= _rusage_mb() + 0.2


def test_read_peak_rss_mb_garbage_status_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Битая строка ``VmHWM`` — не падение, а тот же фолбэк."""
    _fake_proc(monkeypatch, "VmHWM: not-a-number kB\n")
    assert read_peak_rss_mb() == pytest.approx(_rusage_mb(), abs=0.2)


def test_read_peak_rss_mb_without_any_source_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ни procfs, ни ``ru_maxrss`` — ``None``: метрика не выдумывается.

    ``resource`` импортируется модулем **опционально** (Unix-only), поэтому подменяется
    сам атрибут ``_resource``: тест не зависит от того, что модуль назвал его публично.
    """
    _fake_proc(monkeypatch, None)
    monkeypatch.setattr(
        resources,
        "_resource",
        SimpleNamespace(RUSAGE_SELF=0, getrusage=lambda who: SimpleNamespace(ru_maxrss=0)),
    )
    assert read_peak_rss_mb() is None


def test_read_peak_rss_mb_without_resource_module_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Платформа без ``resource`` (Windows) — ``None``, а не ``AttributeError``."""
    _fake_proc(monkeypatch, None)
    monkeypatch.setattr(resources, "_resource", None)
    assert read_peak_rss_mb() is None


def test_read_available_mb_missing_procfs_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Контейнер без procfs — не ошибка: бюджет задаётся только явно."""
    _fake_proc(monkeypatch, None)
    assert read_available_mb() is None


def test_read_available_mb_missing_field_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_proc(monkeypatch, "MemTotal:       8000000 kB\n")
    assert read_available_mb() is None


def test_read_available_mb_parses_kib_into_mb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``MemAvailable`` в kB переводится в MB целым, без округления вверх."""
    _fake_proc(
        monkeypatch, "MemTotal:       8000000 kB\nMemAvailable:   2048000 kB\n"
    )
    assert read_available_mb() == 2000
