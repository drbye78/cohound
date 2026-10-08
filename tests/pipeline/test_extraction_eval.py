"""Тесты extraction-golden и A/B экстракторов (T8.7, §11.2, §13, I19, A3).

Используют фейковый ``ExtractorPort`` (без docling/trafilatura) и нейтральный
extraction-golden-фикстур. Проверяют: загрузку/валидацию golden, прогон кейсов
(expected/forbidden/min_chars), изоляцию ошибок, конструкцию ``RawPage`` (формат →
расширение/content-type), A/B-сравнение, триггер re-extract по ``extractor_version``,
``to_manifest_json`` и пустой golden. Нейтральны (§1.3).
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

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
        self.calls.append((case_id, page.content, page.content_type))
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
    assert [case.case_id for case in cases] == ["case_html", "case_pdf", "case_docx"]
    assert [case.format for case in cases] == ["html", "pdf", "docx"]
    assert all(case.raw_b64 is not None for case in cases)
    assert all(case.raw_path is None for case in cases)
    assert cases[0].expected_substrings == ["Alpha heading", "beta body"]
    assert cases[0].forbidden_substrings == ["Gamma"]
    assert cases[0].min_chars == 10
    assert cases[2].min_chars == 0


def test_load_golden_resolves_raw_path_relative_to_file(tmp_path: Path) -> None:
    golden = tmp_path / "golden.json"
    golden.write_text(
        json.dumps([{"case_id": "c", "format": "pdf", "raw_path": "files/page.pdf"}]),
        encoding="utf-8",
    )
    (case,) = load_extraction_golden(golden)
    assert case.raw_path == str((tmp_path / "files" / "page.pdf").resolve())


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
