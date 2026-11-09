"""Тесты extraction-golden и A/B экстракторов (T8.7, §11.2, §13, I19, A3).

Используют фейковый ``ExtractorPort`` (без docling/trafilatura) и нейтральный
extraction-golden-фикстур. Проверяют: загрузку/валидацию golden, прогон кейсов
(expected/forbidden/min_chars), изоляцию ошибок, конструкцию ``RawPage`` (формат →
расширение/content-type), A/B-сравнение, триггер re-extract по ``extractor_version``,
``to_manifest_json`` и пустой golden. Нейтральны (§1.3).
"""

from __future__ import annotations

import base64
import importlib.util
import json
import pathlib
from pathlib import Path
from typing import Any

import pytest

from core.adapters.extract_pdf_text import PdfiumDocumentExtractor
from core.pipeline.extraction_eval import (
    ExtractionGoldenCase,
    ExtractionReport,
    compare_extractors,
    extractor_version_changed,
    load_extraction_golden,
    run_extraction_golden,
)
from core.ports.types import ExtractedDoc, RawPage
from core.util.hashing import content_hash

FIXTURES = Path(__file__).resolve().parent / "fixtures"
GOLDEN_PATH = FIXTURES / "extraction_golden.json"
GOLDEN_FILES = FIXTURES / "extraction_golden"
GENERATOR_PATH = FIXTURES / "make_extraction_golden.py"


class FakeExtractor:
    """Детерминированный фейк ``ExtractorPort``: ``case_id`` → текст/ошибка/исключение.

    ``case_id`` выводится из имени файла в ``page.url`` (последний сегмент без расширения).
    """

    def __init__(
        self,
        version: str,
        texts: dict[str, str] | None = None,
        errors: dict[str, str] | None = None,
        raises: set[str] | None = None,
    ) -> None:
        self.version = version
        self._texts = texts or {}
        self._errors = errors or {}
        self._raises = raises or set()
        self.calls: list[tuple[str, bytes, str | None]] = []

    def extract(self, page: RawPage) -> ExtractedDoc:
        case_id = page.url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        self.calls.append((case_id, _page_bytes(page), page.content_type))
        if case_id in self._raises:
            raise RuntimeError(f"boom:{case_id}")
        if case_id in self._errors:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(""),
                extractor_version=self.version,
                error=self._errors[case_id],
            )
        text = self._texts.get(case_id, "")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
        )


def _case(case_id: str, raw: bytes, *, fmt: str = "html", **kwargs) -> ExtractionGoldenCase:
    """Строит кейс с встроенными байтами (base64) — без внешних файлов."""
    return ExtractionGoldenCase(
        case_id=case_id,
        format=fmt,
        raw_b64=base64.b64encode(raw).decode("ascii"),
        **kwargs,
    )


# -- load_extraction_golden ------------------------------------------------- #


def test_load_golden_fixture() -> None:
    cases = load_extraction_golden(GOLDEN_PATH)
    assert [case.case_id for case in cases] == [
        "case_html",
        "case_pdf",
        "case_pdf_scan",
        "case_docx",
    ]
    assert [case.format for case in cases] == ["html", "pdf", "pdf", "docx"]
    html, pdf, scan, docx = cases
    # HTML-кейс остаётся встроенным; PDF/Office — реальные файлы рядом с golden.
    assert html.raw_b64 is not None and html.raw_path is None
    assert all(case.raw_b64 is None and case.raw_path is not None for case in cases[1:])
    assert all(Path(case.raw_path).is_file() for case in cases[1:] if case.raw_path)
    assert html.expected_substrings == ["Alpha heading", "beta body"]
    assert html.forbidden_substrings == ["Gamma"]
    assert html.min_chars == 10
    # PDF-кейс проверяет содержание документа, а не служебное слово структуры PDF.
    assert pdf.expected_substrings == ["distinctive alpha sentence", "omega conclusion"]
    assert "Catalog" not in pdf.expected_substrings
    assert "Catalog" in pdf.forbidden_substrings
    assert pdf.min_chars == 250
    assert scan.expect_error == "no_text_layer"
    assert docx.expected_substrings == ["unique word kumquat", "narwhal"]
    assert docx.min_chars == 80


def test_load_golden_resolves_raw_path_relative_to_file(tmp_path: Path) -> None:
    golden = tmp_path / "golden.json"
    golden.write_text(
        json.dumps([{"case_id": "c", "format": "pdf", "raw_path": "files/page.pdf"}]),
        encoding="utf-8",
    )
    (case,) = load_extraction_golden(golden)
    assert case.raw_path == str((tmp_path / "files" / "page.pdf").resolve())


def test_golden_fixtures_match_generator(tmp_path: Path) -> None:
    """Закоммиченные фикстуры совпадают с пересборкой генератором (побайтово).

    Бинарная фикстура без такой сверки устаревает молча: генератор меняют, файл — нет,
    и golden продолжает проверять старые байты. Пересобрать:
    ``.venv/bin/python tests/pipeline/fixtures/make_extraction_golden.py``.
    """
    spec = importlib.util.spec_from_file_location("make_extraction_golden", GENERATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    built = module.build_all(tmp_path)
    assert sorted(built) == ["document.docx", "document.pdf", "scan.pdf"]
    for name, data in built.items():
        committed = GOLDEN_FILES / name
        assert committed.is_file(), f"нет закоммиченной фикстуры {name}"
        assert committed.read_bytes() == data, (
            f"{name} устарел: пересоберите tests/pipeline/fixtures/make_extraction_golden.py"
        )


@pytest.mark.parametrize(
    "payload",
    [
        "{}",  # не список
        "[1]",  # элемент не объект
        '[{"format": "pdf", "raw_b64": "AA=="}]',  # нет case_id
        '[{"case_id": "", "format": "pdf", "raw_b64": "AA=="}]',  # пустой case_id
        '[{"case_id": "c", "format": "zip", "raw_b64": "AA=="}]',  # неподдерживаемый формат
        '[{"case_id": "c", "format": "pdf"}]',  # нет источника байтов
        '[{"case_id": "c", "format": "pdf", "raw_path": "a", "raw_b64": "AA=="}]',  # оба
        '[{"case_id": "c", "format": "pdf", "raw_b64": "!!!"}]',  # битый base64
        '[{"case_id": "c", "format": "pdf", "raw_b64": "AA==", "expected_substrings": 1}]',  # не список
        '[{"case_id": "c", "format": "pdf", "raw_b64": "AA==", "forbidden_substrings": [1]}]',  # не строки
        '[{"case_id": "c", "format": "pdf", "raw_b64": "AA==", "min_chars": -1}]',  # отрицательное
        '[{"case_id": "c", "format": "pdf", "raw_b64": "AA==", "min_chars": true}]',  # bool
        '[{"case_id": "c", "format": "pdf", "raw_b64": "AA==", "expect_error": 1}]',  # не строка
        '[{"case_id": "c", "format": "pdf", "raw_b64": "AA==", "expect_error": ""}]',  # пустая
        "not json",  # не JSON
    ],
)
def test_load_golden_rejects_bad_payload(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "bad.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(ValueError):
        load_extraction_golden(path)


def test_load_golden_rejects_duplicate_case_id(tmp_path: Path) -> None:
    path = tmp_path / "dup.json"
    path.write_text(
        json.dumps(
            [
                {"case_id": "c", "format": "pdf", "raw_b64": "AA=="},
                {"case_id": "c", "format": "pdf", "raw_b64": "AA=="},
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_extraction_golden(path)


# -- run_extraction_golden -------------------------------------------------- #


def test_pass_case_and_page_construction() -> None:
    raw = b"<html>Alpha</html>"
    case = _case("case_a", raw, expected_substrings=["Alpha"], min_chars=3)
    extractor = FakeExtractor("fake-1", texts={"case_a": "Alpha"})
    report = run_extraction_golden([case], extractor)
    assert isinstance(report, ExtractionReport)
    assert report.extractor_version == "fake-1"
    assert report.total == 1 and report.passed == 1
    assert report.pass_rate == 1.0
    assert report.results[0].ok is True
    assert report.results[0].chars == 5
    # RawPage собрана из байтов, формат → url-расширение и content-type.
    call_id, call_content, call_content_type = extractor.calls[0]
    assert call_id == "case_a"
    assert call_content == raw
    assert call_content_type == "text/html"


def test_pdf_format_sets_content_type() -> None:
    case = _case("case_p", b"%PDF-1.4 x", fmt="pdf", expected_substrings=["PDF"])
    extractor = FakeExtractor("fake-1", texts={"case_p": "PDF content"})
    run_extraction_golden([case], extractor)
    assert extractor.calls[0][2] == "application/pdf"


def test_missing_expected_substring_fails() -> None:
    case = _case("case_a", b"x", expected_substrings=["Alpha", "beta"])
    extractor = FakeExtractor("fake-1", texts={"case_a": "Alpha only"})
    result = run_extraction_golden([case], extractor).results[0]
    assert result.ok is False
    assert result.missing == ["beta"]
    assert result.forbidden_hits == []
    assert result.error is None


def test_forbidden_substring_fails() -> None:
    case = _case("case_a", b"x", forbidden_substrings=["Gamma"])
    extractor = FakeExtractor("fake-1", texts={"case_a": "Alpha Gamma"})
    result = run_extraction_golden([case], extractor).results[0]
    assert result.ok is False
    assert result.forbidden_hits == ["Gamma"]
    assert result.missing == []
    assert result.error is None


def test_min_chars_fails() -> None:
    case = _case("case_a", b"x", min_chars=10)
    extractor = FakeExtractor("fake-1", texts={"case_a": "short"})
    result = run_extraction_golden([case], extractor).results[0]
    assert result.ok is False
    assert result.chars == 5
    assert result.error is None


# -- реальные фикстуры: содержание, а не служебные слова --------------------- #


def _golden_case(case_id: str) -> ExtractionGoldenCase:
    (case,) = [case for case in load_extraction_golden(GOLDEN_PATH) if case.case_id == case_id]
    return case


class _DocumentPort:
    """``ExtractorPort``-обёртка документного адаптера — как композит в composition root.

    Документный порт получает уже выбранный файл (``extract_bytes``), а golden зовёт
    ``extract(page)``; обёртка передаёт байты и имя с суффиксом, по которому адаптер
    проверяет формат. Это ровно то, что делает ``_LocalExtractor``.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.version = str(getattr(inner, "version", ""))

    def extract(self, page: RawPage, route: Any = None) -> ExtractedDoc:
        filename = page.url.rsplit("/", 1)[-1]
        return self._inner.extract_bytes(_page_bytes(page), filename=filename)


def test_pdf_case_rejects_service_word_instead_of_content() -> None:
    """Старый дефект: вместо текста пришло служебное слово PDF ``Catalog`` → кейс красный."""
    pdf_case = _golden_case("case_pdf")
    extractor = FakeExtractor("fake-1", texts={"case_pdf": "Catalog"})
    result = run_extraction_golden([pdf_case], extractor).results[0]
    assert result.ok is False
    assert result.missing == ["distinctive alpha sentence", "omega conclusion"]
    assert result.forbidden_hits == ["Catalog"]


def test_pdf_case_rejects_empty_text() -> None:
    """Пустой текст на PDF-кейсе — провал, а не «прочитали и ничего не нашли»."""
    pdf_case = _golden_case("case_pdf")
    extractor = FakeExtractor("fake-1", texts={"case_pdf": ""})
    result = run_extraction_golden([pdf_case], extractor).results[0]
    assert result.ok is False
    assert result.chars == 0
    assert result.missing == ["distinctive alpha sentence", "omega conclusion"]


def test_expected_error_case_requires_that_error() -> None:
    """Негативный кейс зелёный только при ожидаемой ошибке; текст её не заменяет."""
    scan_case = _golden_case("case_pdf_scan")

    raised = FakeExtractor("fake-1", errors={"case_pdf_scan": "no_text_layer"})
    passed = run_extraction_golden([scan_case], raised).results[0]
    assert passed.ok is True
    assert passed.error == "no_text_layer"
    assert passed.expected_error == "no_text_layer"

    silent = FakeExtractor("fake-1", texts={"case_pdf_scan": "some text"})
    missed = run_extraction_golden([scan_case], silent).results[0]
    assert missed.ok is False
    assert missed.error is None
    assert missed.expected_error == "no_text_layer"

    wrong = FakeExtractor("fake-1", errors={"case_pdf_scan": "pdfium: сбой"})
    mismatched = run_extraction_golden([scan_case], wrong).results[0]
    assert mismatched.ok is False
    assert mismatched.error == "pdfium: сбой"
    assert mismatched.expected_error == "no_text_layer"


def test_golden_pdf_cases_with_real_pdfium() -> None:
    """``pypdfium2`` (базовая зависимость) читает обе PDF-фикстуры как задумано.

    Без этого теста фикстура проверяла бы только фейк: реальный механизм ``text_layer``
    обязан вернуть содержание страниц и отказать скану без слоя.
    """
    pdf_cases = [case for case in load_extraction_golden(GOLDEN_PATH) if case.format == "pdf"]
    report = run_extraction_golden(pdf_cases, _DocumentPort(PdfiumDocumentExtractor()))
    assert [result.case_id for result in report.results] == ["case_pdf", "case_pdf_scan"]
    assert report.passed == report.total == 2


def test_golden_docx_case_with_real_docling() -> None:
    """Минимальный DOCX читается офисным бэкендом docling и даёт ожидаемый текст."""
    pytest.importorskip("docling", reason="офисный бэкенд docling не установлен")
    from core.adapters.extract_document import DoclingDocumentExtractor

    docx_case = _golden_case("case_docx")
    report = run_extraction_golden([docx_case], _DocumentPort(DoclingDocumentExtractor()))
    result = report.results[0]
    assert result.ok is True, result
    assert result.error is None
    assert result.chars >= 80


def test_error_isolation_between_cases() -> None:
    cases = [
        _case("ok_case", b"x", expected_substrings=["Alpha"]),
        _case("err_case", b"x", expected_substrings=["Alpha"]),
        _case("raise_case", b"x", expected_substrings=["Alpha"]),
    ]
    extractor = FakeExtractor(
        "fake-1",
        texts={"ok_case": "Alpha"},
        errors={"err_case": "адаптер: сбой"},
        raises={"raise_case"},
    )
    report = run_extraction_golden(cases, extractor)
    assert report.total == 3
    by_id = {result.case_id: result for result in report.results}
    assert by_id["ok_case"].ok is True
    assert by_id["err_case"].ok is False
    assert by_id["err_case"].error == "адаптер: сбой"
    assert by_id["err_case"].extractor_version == "fake-1"
    assert by_id["raise_case"].ok is False
    assert by_id["raise_case"].error is not None
    assert "RuntimeError" in by_id["raise_case"].error
    assert report.passed == 1
    assert report.pass_rate == pytest.approx(1 / 3)


def test_run_is_deterministic() -> None:
    cases = [_case("case_a", b"<html>Alpha</html>", expected_substrings=["Alpha"])]
    extractor = FakeExtractor("fake-1", texts={"case_a": "Alpha"})
    first = run_extraction_golden(cases, extractor).to_manifest_json()
    second = run_extraction_golden(cases, extractor).to_manifest_json()
    assert first == second


def test_empty_golden_handled_deterministically() -> None:
    report = run_extraction_golden([], FakeExtractor("fake-1"))
    assert report.total == 0
    assert report.passed == 0
    assert report.pass_rate == 0.0
    data = json.loads(report.to_manifest_json())
    assert data["total"] == 0
    assert data["pass_rate"] == 0.0
    assert data["results"] == []


# -- compare_extractors (A/B, A3) ------------------------------------------- #


def _ab_cases() -> list[ExtractionGoldenCase]:
    return [
        _case("c1", b"x", expected_substrings=["OK"]),
        _case("c2", b"x", expected_substrings=["OK"]),
        _case("c3", b"x", expected_substrings=["OK"]),
    ]


def test_compare_extractors_agreement_and_disagreement() -> None:
    a = FakeExtractor("extract-a", texts={"c1": "OK", "c2": "OK", "c3": "nope"})
    b = FakeExtractor("extract-b", texts={"c1": "OK", "c2": "nope", "c3": "nope"})
    comparison = compare_extractors(_ab_cases(), a, b)

    assert comparison["a_version"] == "extract-a"
    assert comparison["b_version"] == "extract-b"
    assert comparison["a_pass_rate"] == pytest.approx(2 / 3)
    assert comparison["b_pass_rate"] == pytest.approx(1 / 3)
    by_id = {row["case_id"]: row for row in comparison["cases"]}
    assert by_id["c1"]["classification"] == "both_pass"
    assert by_id["c2"]["classification"] == "disagree"
    assert by_id["c3"]["classification"] == "both_fail"
    assert comparison["summary"] == {"both_pass": 1, "disagree": 1, "both_fail": 1}
    assert comparison["recommendation"]["suggested"] == "a"
    # A/B — измерение (A3): автопереключение запрещено, выбор фиксирует человек.
    assert comparison["recommendation"]["auto_switch"] is False
    assert "A3" in comparison["recommendation"]["basis"]


def test_compare_extractors_tie() -> None:
    a = FakeExtractor("extract-a", texts={"c1": "OK", "c2": "nope", "c3": "nope"})
    b = FakeExtractor("extract-b", texts={"c1": "nope", "c2": "OK", "c3": "nope"})
    comparison = compare_extractors(_ab_cases(), a, b)
    assert comparison["a_pass_rate"] == comparison["b_pass_rate"]
    assert comparison["recommendation"]["suggested"] == "tie"
    assert comparison["recommendation"]["auto_switch"] is False


# -- extractor_version_changed (триггер re-extract, §13) -------------------- #


@pytest.mark.parametrize(
    ("manifest", "current", "expected"),
    [
        ("docling-doc-2", "docling-doc-2", False),
        ("docling-doc-2", "docling-doc-3", True),
        (None, "docling-doc-2", True),
        ("docling-doc-2", None, True),
        (None, None, False),
        ("", "", False),
        (None, "", False),
    ],
)
def test_extractor_version_changed(manifest, current, expected: bool) -> None:
    assert extractor_version_changed(manifest, current) is expected


# -- to_manifest_json ------------------------------------------------------- #


def test_to_manifest_json_structure() -> None:
    cases = [
        _case("case_a", b"x", expected_substrings=["Alpha"]),
        _case("case_b", b"x", expected_substrings=["Missing"]),
    ]
    extractor = FakeExtractor("fake-1", texts={"case_a": "Alpha", "case_b": "other"})
    report = run_extraction_golden(cases, extractor)
    raw = report.to_manifest_json()
    assert raw == report.to_manifest_json()  # детерминирован
    data = json.loads(raw)
    assert data["extractor_version"] == "fake-1"
    assert data["total"] == 2
    assert data["passed"] == 1
    assert data["pass_rate"] == pytest.approx(0.5)
    assert [row["case_id"] for row in data["results"]] == ["case_a", "case_b"]
    assert data["results"][1]["missing"] == ["Missing"]


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM).

    До F5 конверсия шла внутри краула, и ``RawPage.content`` страницы был непуст. Теперь
    документ конвертируется из строки очереди: ``content`` пуст, тело лежит по
    ``source_path`` (I19, ``corpus_raw/{hash}.bin``) — ровно так его получает воркер пула.
    Фейковый экстрактор обязан читать тот же носитель, что и адаптер, иначе он проверял бы
    не контракт порта, а прежний способ доставки байтов.
    """
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""
