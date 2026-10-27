"""F7: ось ``crawl.document_timeout_s`` доезжает до docling (P8).

Дефект был в проводке, а не в адаптере: параметр ``document_timeout`` у
:class:`~core.adapters.extract_document.DoclingDocumentExtractor` существовал и
приоритетен над таблицей уровней, но **ни один** конструктор в composition root его не
передавал — объявленный потолок документа не действовал.

Проверка — инъекцией подменённого конструктора: так видно ровно то, что передаёт
проводка (значение оси либо ``None`` = «ось не объявлена, работает таблица уровней»), и
результат не зависит от того, установлен ли docling на хосте.

Данные нейтральны (§1.3).
"""

from __future__ import annotations

from typing import Any

import pytest

from core.cli import wiring
from core.domain.enums import DocumentLevel, PdfPipeline
from core.domain.models import CrawlRunConfig


class _RecordingDocling:
    """Подменённый ``DoclingDocumentExtractor``: запоминает аргументы проводки."""

    available = False
    version = "recording"
    supports_source_path = True

    captured: Any = None

    def __init__(
        self,
        level: DocumentLevel = DocumentLevel.FULL,
        *,
        document_timeout: float | None = None,
    ) -> None:
        type(self).captured = {"level": level, "document_timeout": document_timeout}


@pytest.fixture
def recording(monkeypatch: pytest.MonkeyPatch) -> type[_RecordingDocling]:
    """Подменить docling-адаптер в composition root (без зависимости от хоста)."""
    monkeypatch.setattr(wiring, "DoclingDocumentExtractor", _RecordingDocling)
    _RecordingDocling.captured = None
    return _RecordingDocling


def test_worker_extractor_receives_declared_document_timeout(
    recording: type[_RecordingDocling],
) -> None:
    """Фабрика воркер-экстрактора передаёт объявленный таймаут в адаптер."""
    wiring.build_worker_extractor(DocumentLevel.TEXT, document_timeout=1234.0)

    assert recording.captured == {"level": DocumentLevel.TEXT, "document_timeout": 1234.0}


def test_worker_extractor_without_axis_keeps_level_table(
    recording: type[_RecordingDocling],
) -> None:
    """Ось не объявлена → ``None``: адаптер берёт per-level таблицу (в т.ч. ``full: None``)."""
    wiring.build_worker_extractor(DocumentLevel.LAYOUT_TABLES, PdfPipeline.STRUCTURED)

    assert recording.captured == {
        "level": DocumentLevel.LAYOUT_TABLES,
        "document_timeout": None,
    }


def test_ports_receive_declared_document_timeout_from_run_layer(
    recording: type[_RecordingDocling],
) -> None:
    """``build_adapters`` резолвит ось ``crawl`` (run-слой) и отдаёт её docling-адаптеру."""
    wiring.build_adapters(
        profile="local", run_crawl=CrawlRunConfig(document_timeout_s=900.0)
    )

    assert recording.captured is not None
    assert recording.captured["document_timeout"] == 900.0


def test_ports_without_declared_axis_pass_none(
    recording: type[_RecordingDocling],
) -> None:
    """Дефолт оси (1800 с) — потолок *задачи* фазы, а не конвертера: в адаптер идёт ``None``.

    Иначе прогон, который ось не объявлял, получил бы другой per-level таймаут docling
    (``text``: 900 → 1800) — то есть смену поведения без объявленного входа.
    """
    wiring.build_adapters(profile="local")

    assert recording.captured is not None
    assert recording.captured["document_timeout"] is None
