"""Тесты ``hound experiment extraction`` — A/B механизмов PDF-извлечения (O38б, D10).

Что доказывается:

* команда берёт **набор** (``cohound-extraction-golden/1``) и печатает пер-кейс таблицу
  исходов под A и под B, а артефакт замера несёт ``run_id``/``golden``/``per_case``/
  ``summary`` и версию каждого механизма;
* A и B **различаются там, где должны**: кейс, который один механизм читает, а другой
  отказывает (``no_text_layer``), попадает в ``a_only``/``b_only``, а не в «оба прошли»;
* отсутствие предпосылки — **отказ** (``EXIT_USAGE``) с названной причиной: нет набора,
  0 кейсов, объявленный механизм не исполним (docling не установлен), A и B — один и тот
  же механизм. Тихий фолбэк на другой движок запрещён: измерение назвало бы механизм,
  которого не было (A3).

Реальная экстракция не выполняется: фабрика ``build_worker_extractor`` подменяется
детерминированными фейками — проверяется проводка и отчёт, а не docling/pdfium.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.extr"
PRODUCT = "p-alpha"


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_extr",
        anchor=PRODUCT,
        cohort="cohort_extr",
        products=[PRODUCT],
        package="pkg-extr",
        focus=Focus(modules=["mod"], params=["param"]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _store(tmp_path: Path) -> Path:
    db = tmp_path / "runs" / RUN_ID / "cohound.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db) as store:
        store.create_run(RUN_ID, _cfg())
    return db


def _raw(tmp_path: Path, name: str) -> Path:
    path = tmp_path / "corpus" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n")
    return path


def _golden(tmp_path: Path, cases: list[dict[str, Any]]) -> Path:
    path = tmp_path / "extraction_golden.json"
    path.write_text(
        json.dumps(
            {
                "format": "cohound-extraction-golden/1",
                "case_id": "case_extr",
                "run_id": RUN_ID,
                "status": "candidate",
                "reviewed_by": None,
                "policy": {},
                "cases": cases,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


class FakeExtractor:
    """Детерминированный ``ExtractorPort``: ``case_id`` → текст/ошибка, версия и уровень."""

    def __init__(
        self,
        version: str,
        *,
        texts: dict[str, str] | None = None,
        errors: dict[str, str] | None = None,
        pages: int | None = None,
        document_level: Any = "full",
    ) -> None:
        self.version = version
        self.document_level = document_level
        self._texts = texts or {}
        self._errors = errors or {}
        self._pages = pages

    def extract(self, page: Any) -> Any:
        from core.ports.types import ExtractedDoc

        case_id = page.url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        if case_id in self._errors:
            return ExtractedDoc(
                text="",
                content_hash="h",
                extractor_version=self.version,
                error=self._errors[case_id],
            )
        text = self._texts.get(case_id, "")
        # ``pages_converted`` по инварианту домена требует ``partial``: это счётчик
        # «прочитано N из заявленных», и без частичности он был бы счётчиком без факта.
        partial = self._pages is not None and bool(text)
        return ExtractedDoc(
            text=text,
            content_hash="h",
            extractor_version=self.version,
            partial=partial,
            partial_reason="тест: прочитана часть страниц" if partial else None,
            pages_converted=self._pages if partial else None,
        )


def _cases(tmp_path: Path) -> list[dict[str, Any]]:
    return [
        {
            "case_id": "case_a",
            "format": "pdf",
            "raw_path": str(_raw(tmp_path, "a.pdf")),
            "expected_substrings": ["alpha"],
            "min_chars": 3,
            "doc_id": "p-alpha:aaa",
        },
        {
            "case_id": "case_b",
            "format": "pdf",
            "raw_path": str(_raw(tmp_path, "b.pdf")),
            "expected_substrings": ["beta"],
            "min_chars": 3,
            "doc_id": "p-alpha:bbb",
        },
    ]


def _install_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    structured: FakeExtractor,
    text_layer: FakeExtractor,
) -> None:
    """Подмена фабрики механизмов: ``structured`` → A, ``text_layer`` → B."""
    from core.cli.commands import experiment as module

    def factory(*, level: Any = None, pdf_pipeline: Any = None, pdf_device: Any = None,
                document_timeout: Any = None) -> Any:
        del level, pdf_device, document_timeout
        return structured if str(getattr(pdf_pipeline, "value", pdf_pipeline)) == "structured" \
            else text_layer

    monkeypatch.setattr(module, "build_worker_extractor", factory)


# ---------------------------------------------------------------------------
# Измерение состоялось
# ---------------------------------------------------------------------------
def test_extraction_reports_per_case_and_writes_artifact(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Пер-кейс исходы A/B, сводка «только под одним» и артефакт замера."""
    db = _store(tmp_path)
    golden = _golden(tmp_path, _cases(tmp_path))
    artifact = tmp_path / "out" / "extraction.json"
    # A читает оба кейса; B отказывает на case_b — различие обязано быть видно.
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor(
            "docling-doc-4:full/cpu/x/y",
            texts={"case_a": "alpha text", "case_b": "beta text"},
            pages=3,
        ),
        text_layer=FakeExtractor(
            "pdfium-text-5",
            texts={"case_a": "alpha text"},
            errors={"case_b": "no_text_layer"},
        ),
    )

    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--out-json",
            str(artifact),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["run_id"] == RUN_ID
    assert payload["golden"] == str(golden)
    assert payload["golden_format"] == "cohound-extraction-golden/1"
    assert payload["golden_status"] == "candidate"
    assert payload["total"] == 2
    assert payload["extractor_version"] == {
        "a": "docling-doc-4:full/cpu/x/y",
        "b": "pdfium-text-5",
    }
    assert payload["mechanisms"]["a"]["mechanism"] == "structured"
    assert payload["mechanisms"]["b"]["mechanism"] == "text_layer"
    assert payload["mechanisms"]["a"]["level"] == "full"
    assert payload["mechanisms"]["b"]["device"] == "cpu"

    per_case = {row["case_id"]: row for row in payload["per_case"]}
    assert per_case["case_a"]["a"]["outcome"] == "ok"
    assert per_case["case_a"]["b"]["outcome"] == "ok"
    assert per_case["case_a"]["classification"] == "both_pass"
    assert per_case["case_b"]["a"]["outcome"] == "ok"
    assert per_case["case_b"]["b"]["outcome"] == "error"
    assert per_case["case_b"]["b"]["error"] == "no_text_layer"
    assert per_case["case_b"]["classification"] == "disagree"
    # Страницы — наблюдение читателя, а не выдумка.
    assert per_case["case_a"]["a"]["pages"] == 3

    assert payload["summary"]["a_passed"] == 2
    assert payload["summary"]["b_passed"] == 1
    assert payload["summary"]["a_only"] == 1
    assert payload["summary"]["b_only"] == 0
    assert payload["summary"]["both"] == 1
    assert payload["summary"]["neither"] == 0
    # A/B — измерение, а не автопереключение (A3).
    assert payload["recommendation"]["auto_switch"] is False
    assert payload["recommendation"]["suggested"] == "a"

    assert payload["artifact"] == str(artifact)
    written = json.loads(artifact.read_text(encoding="utf-8"))
    assert written["per_case"] == payload["per_case"]
    assert written["run_id"] == RUN_ID


def test_extraction_reports_missing_and_forbidden(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Исход различает «подстроки нет» и «запрещённая нашлась» — это разные дефекты."""
    db = _store(tmp_path)
    cases = _cases(tmp_path)
    cases[0]["forbidden_substrings"] = ["gamma"]
    golden = _golden(tmp_path, cases)
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x", texts={"case_a": "alpha gamma", "case_b": ""}),
        text_layer=FakeExtractor("pdfium-text-5", texts={"case_a": "alpha", "case_b": "beta"}),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--out-json",
            str(tmp_path / "o.json"),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    per_case = {row["case_id"]: row for row in payload["per_case"]}
    assert per_case["case_a"]["a"]["outcome"] == "forbidden"
    assert per_case["case_a"]["a"]["forbidden_hits"] == ["gamma"]
    assert per_case["case_b"]["a"]["outcome"] == "missing"
    assert per_case["case_b"]["a"]["missing"] == ["beta"]
    assert per_case["case_b"]["b"]["outcome"] == "ok"


def test_extraction_prints_table_without_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Человеческий вывод — таблица пер-кейс (команда обязана быть читаемой без ``--json``)."""
    db = _store(tmp_path)
    golden = _golden(tmp_path, _cases(tmp_path))
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x", texts={"case_a": "alpha", "case_b": "beta"}),
        text_layer=FakeExtractor("pdfium-text-5", texts={"case_a": "alpha"}),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--out-json",
            str(tmp_path / "o.json"),
        ]
    )
    printed = capsys.readouterr().out
    assert code == EXIT_OK
    assert "| case_id | A | B | chars A/B | pages A/B | ms A/B |" in printed
    assert "| case_b | ok | missing |" in printed
    assert "только A: 1" in printed


# ---------------------------------------------------------------------------
# Отказы: предпосылки нет — измерение не выдумывается
# ---------------------------------------------------------------------------
def test_extraction_refuses_without_golden(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Набора нет — ``EXIT_USAGE`` с названным путём, а не «0 кейсов, всё хорошо»."""
    db = _store(tmp_path)
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x"),
        text_layer=FakeExtractor("pdfium-text-5"),
    )
    missing = tmp_path / "absent.json"
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(missing),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert payload["error"] == "golden"
    assert str(missing) in payload["message"]


def test_extraction_refuses_empty_set(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """0 кейсов — отказ: пустой набор не превращается в измерение."""
    db = _store(tmp_path)
    golden = _golden(tmp_path, [])
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x"),
        text_layer=FakeExtractor("pdfium-text-5"),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["error"] == "golden"
    assert "0 кейсов" in payload["message"]


def test_extraction_refuses_unavailable_structured(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``structured`` без docling — отказ с числами: PDF ушёл бы в HTML-читатель."""
    db = _store(tmp_path)
    golden = _golden(tmp_path, _cases(tmp_path))
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("trafilatura-html-8", document_level=None),
        text_layer=FakeExtractor("pdfium-text-5"),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["error"] == "wiring"
    assert "docling" in payload["message"]
    assert "text_layer" in payload["message"]


def test_extraction_refuses_same_mechanism(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A и B — один механизм: сравнение с самим собой ничего не измеряет."""
    db = _store(tmp_path)
    golden = _golden(tmp_path, _cases(tmp_path))
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x"),
        text_layer=FakeExtractor("pdfium-text-5"),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--mechanism-b",
            "structured",
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["error"] == "usage"
    assert "один механизм" in payload["message"]


def test_extraction_default_golden_comes_from_package_sources(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``--golden`` набор берётся из ``packages/{case_id}/sources/`` (раскладка пакета)."""
    db = _store(tmp_path)
    packages = tmp_path / "packages"
    sources = packages / "case_extr" / "sources"
    sources.mkdir(parents=True)
    _golden(sources, _cases(tmp_path))
    monkeypatch.setenv("COHOUND_PACKAGES_DIR", str(packages))
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x", texts={"case_a": "alpha", "case_b": "beta"}),
        text_layer=FakeExtractor("pdfium-text-5", texts={"case_a": "alpha", "case_b": "beta"}),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--out-json",
            str(tmp_path / "o.json"),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["golden"] == str(sources / "extraction_golden.json")
    assert payload["case_id"] == "case_extr"


def test_extraction_default_artifact_lives_in_run_dir(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``--out-json`` артефакт ложится в ``runs/{run_id}/extraction/`` рядом с БД."""
    db = _store(tmp_path)
    golden = _golden(tmp_path, _cases(tmp_path))
    _install_fakes(
        monkeypatch,
        structured=FakeExtractor("docling-doc-4:x", texts={"case_a": "alpha", "case_b": "beta"}),
        text_layer=FakeExtractor("pdfium-text-5", texts={"case_a": "alpha", "case_b": "beta"}),
    )
    code = main(
        [
            "experiment",
            "extraction",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--golden",
            str(golden),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    artifact = Path(payload["artifact"])
    assert artifact.parent == db.parent / "extraction"
    assert artifact.is_file()
    assert json.loads(artifact.read_text(encoding="utf-8"))["total"] == 2
