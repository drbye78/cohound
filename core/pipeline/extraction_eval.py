"""coHound — extraction-golden и A/B экстракторов (T8.7, §11.2, §13, I19, A3).

Прогон фиксированного набора страниц (HTML/PDF/Office) против ``ExtractorPort``: для каждой
страницы проверяется наличие обязательных подстрок, отсутствие запрещённых и минимальная
длина текста. Это «нижний» golden-уровень качества экстракции (06 §6): он ловит потерю
контента (особенно PDF-таблицы) до того, как она превратится в FN в отчёте.

**Re-extract без ре-фетча (02 §B.1, I19).** Сырые байты — retention-единица; ``extractor_version``
позволяет переизвлечь текст из тех же байтов. Смена ``extractor_version`` — версионная граница
(§13): она триггерит новый extraction-прогон и, как следствие, новую ``index_build_id``-сборку
(старые сборки иммутабельны). Это же и есть A/B экстракторов.

**A/B — измерение, а не автопереключение (A3, §0).** :func:`compare_extractors` сравнивает два
экстрактора и возвращает рекомендацию с флагом ``auto_switch: False``: выбор экстрактора
фиксируется человеком/гейтом по измерению, а не предвидением; флаг документирует это в манифесте.

Модуль категорийно нейтрален (§1.3), детерминирован и не требует тяжёлых адаптеров
(docling/trafilatura) — в тестах используется фейковый ``ExtractorPort``. Единственный I/O —
загрузчик golden и чтение ``raw_path``.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from pathlib import Path

from core.ports.extractor import ExtractorPort
from core.ports.types import RawPage

__all__ = [
    "FORMAT_CONTENT_TYPES",
    "SUPPORTED_GOLDEN_FORMATS",
    "ExtractionCaseResult",
    "ExtractionGoldenCase",
    "ExtractionReport",
    "compare_extractors",
    "extractor_version_changed",
    "load_extraction_golden",
    "run_extraction_golden",
]


#: Форматы extraction-golden (02 §B.1/01 §3.5): PDF + Office Open XML + HTML.
SUPPORTED_GOLDEN_FORMATS: frozenset[str] = frozenset({"pdf", "docx", "pptx", "xlsx", "html"})

#: Content-Type по формату: детерминированная метка формата на ``RawPage`` (01 §3.5).
FORMAT_CONTENT_TYPES: dict[str, str] = {
    "pdf": "application/pdf",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "html": "text/html",
}

#: Фиксированный ``fetched_at`` для синтетических golden-страниц — детерминизм прогона.
_FIXED_FETCHED_AT = "1970-01-01T00:00:00+00:00"


@dataclass(frozen=True)
class ExtractionGoldenCase:
    """Один кейс extraction-golden: страница + ожидания по её извлечённому тексту.

    Источник байтов — ровно один из ``raw_path`` (файл на диске) или ``raw_b64`` (base64
    байтов, встроенных в golden). ``format`` определяет расширение/Content-Type синтетической
    ``RawPage`` и должен входить в :data:`SUPPORTED_GOLDEN_FORMATS`.
    """

    case_id: str
    format: str
    raw_path: str | None = None
    raw_b64: str | None = None
    expected_substrings: list[str] = field(default_factory=list)
    forbidden_substrings: list[str] = field(default_factory=list)
    min_chars: int = 0


@dataclass(frozen=True)
class ExtractionCaseResult:
    """Результат одного кейса: ``ok`` только при отсутствии ошибки и всех расхождений."""

    case_id: str
    ok: bool
    extractor_version: str
    chars: int
    missing: list[str] = field(default_factory=list)
    forbidden_hits: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class ExtractionReport:
    """Итог прогона extraction-golden по одному экстрактору."""

    extractor_version: str
    results: list[ExtractionCaseResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        """Число успешных кейсов."""
        return sum(1 for result in self.results if result.ok)

    @property
    def total(self) -> int:
        """Общее число кейсов."""
        return len(self.results)

    @property
    def pass_rate(self) -> float:
        """Доля успешных кейсов; для пустого golden — ``0.0`` (детерминированный дефолт)."""
        if not self.results:
            return 0.0
        return self.passed / len(self.results)

    def to_manifest_json(self) -> str:
        """JSON для манифеста/логов: версия экстрактора + сводка + результаты (детерминирован).

        ``sort_keys`` делает строку воспроизводимой (пригодна для хеширования/диффа).
        """
        payload = {
            "extractor_version": self.extractor_version,
            "total": self.total,
            "passed": self.passed,
            "pass_rate": round(self.pass_rate, 6),
            "results": [
                {
                    "case_id": result.case_id,
                    "ok": result.ok,
                    "extractor_version": result.extractor_version,
                    "chars": result.chars,
                    "missing": list(result.missing),
                    "forbidden_hits": list(result.forbidden_hits),
                    "error": result.error,
                }
                for result in self.results
            ],
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _decode_b64(value: str, where: str) -> bytes:
    """Строгий base64-декодер с понятной ошибкой (валидация формата golden)."""
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"{where}: некорректный base64 ({exc})") from exc


def load_extraction_golden(path: str | Path) -> list[ExtractionGoldenCase]:
    """Загружает extraction-golden из JSON-списка объектов с валидацией формы.

    Ожидаемая форма::

        [
          {
            "case_id": "case_a",
            "format": "html",
            "raw_b64": "<base64>",          # либо "raw_path": "files/page.pdf"
            "expected_substrings": ["..."],
            "forbidden_substrings": ["..."],
            "min_chars": 10
          }
        ]

    Относительный ``raw_path`` резолвится относительно каталога самого golden-файла
    (не зависит от CWD). ``raw_b64`` валидируется по синтаксису base64 уже на загрузке.

    :raises ValueError: не JSON-список, элемент не объект, ``case_id`` пуст/дублируется,
        неизвестный формат, заданы оба/ни один источник байтов, битый base64,
        списки подстрок не из строк, ``min_chars`` не целое >= 0.
    """
    golden_path = Path(path)
    raw_text = golden_path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"extraction-golden: некорректный JSON ({golden_path}): {exc}") from exc
    if not isinstance(data, list):
        raise ValueError(
            f"extraction-golden: ожидается JSON-список, получено {type(data).__name__}"
        )

    base_dir = golden_path.resolve().parent
    cases: list[ExtractionGoldenCase] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(data):
        where = f"extraction-golden[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{where}: ожидается объект, получено {type(item).__name__}")

        case_id = item.get("case_id")
        if not isinstance(case_id, str) or not case_id.strip():
            raise ValueError(f"{where}.case_id: ожидается непустая строка")
        if case_id in seen_ids:
            raise ValueError(f"{where}.case_id: дубликат {case_id!r}")
        seen_ids.add(case_id)

        fmt_raw = item.get("format")
        if not isinstance(fmt_raw, str) or fmt_raw.strip().lower() not in SUPPORTED_GOLDEN_FORMATS:
            raise ValueError(
                f"{where}.format: ожидается один из {sorted(SUPPORTED_GOLDEN_FORMATS)}, "
                f"получено {fmt_raw!r}"
            )
        fmt = fmt_raw.strip().lower()

        raw_path = item.get("raw_path")
        raw_b64 = item.get("raw_b64")
        if raw_path is not None and not isinstance(raw_path, str):
            raise ValueError(f"{where}.raw_path: ожидается строка")
        if raw_b64 is not None and not isinstance(raw_b64, str):
            raise ValueError(f"{where}.raw_b64: ожидается строка")
        path_value = raw_path if isinstance(raw_path, str) and raw_path.strip() != "" else None
        b64_value = raw_b64 if isinstance(raw_b64, str) and raw_b64.strip() != "" else None
        if path_value is not None and b64_value is not None:
            raise ValueError(f"{where}: заданы и raw_path, и raw_b64 — допустим ровно один")
        if path_value is None and b64_value is None:
            raise ValueError(f"{where}: не задан источник байтов (raw_path или raw_b64)")

        resolved_path = str((base_dir / path_value).resolve()) if path_value is not None else None
        if b64_value is not None:
            _decode_b64(b64_value, f"{where}.raw_b64")

        expected = item.get("expected_substrings", [])
        forbidden = item.get("forbidden_substrings", [])
        for name, value in (
            ("expected_substrings", expected),
            ("forbidden_substrings", forbidden),
        ):
            if not isinstance(value, list) or not all(isinstance(s, str) for s in value):
                raise ValueError(f"{where}.{name}: ожидается список строк")

        min_chars = item.get("min_chars", 0)
        if isinstance(min_chars, bool) or not isinstance(min_chars, int) or min_chars < 0:
            raise ValueError(f"{where}.min_chars: ожидается целое >= 0")

        cases.append(
            ExtractionGoldenCase(
                case_id=case_id,
                format=fmt,
                raw_path=resolved_path,
                raw_b64=b64_value,
                expected_substrings=list(expected),
                forbidden_substrings=list(forbidden),
                min_chars=min_chars,
            )
        )
    return cases


def _read_case_bytes(case: ExtractionGoldenCase) -> bytes:
    """Читает сырые байты кейса (base64 или файл); ошибка → ``ValueError``."""
    if case.raw_b64 is not None:
        return _decode_b64(case.raw_b64, f"case {case.case_id!r}")
    if case.raw_path is not None:
        try:
            return Path(case.raw_path).read_bytes()
        except OSError as exc:
            raise ValueError(
                f"case {case.case_id!r}: не удалось прочитать raw_path {case.raw_path!r}: {exc}"
            ) from exc
    raise ValueError(f"case {case.case_id!r}: не задан источник байтов")


def _build_page(case: ExtractionGoldenCase) -> RawPage:
    """Строит синтетическую ``RawPage``: формат → расширение url и Content-Type (01 §3.5)."""
    fmt = case.format.lower()
    url = f"https://example.invalid/extraction-golden/{case.case_id}.{fmt}"
    return RawPage(
        url=url,
        content=_read_case_bytes(case),
        fetched_at=_FIXED_FETCHED_AT,
        content_type=FORMAT_CONTENT_TYPES.get(fmt),
    )


def _result_error(case_id: str, *, version: str, error: str, chars: int = 0) -> ExtractionCaseResult:
    return ExtractionCaseResult(
        case_id=case_id,
        ok=False,
        extractor_version=version,
        chars=chars,
        error=error,
    )


def _run_case(case: ExtractionGoldenCase, extractor: ExtractorPort) -> ExtractionCaseResult:
    """Прогоняет один кейс с изоляцией ошибок: любой сбой → результат с ``error``, не исключение."""
    fallback_version = str(getattr(extractor, "version", "") or "")

    try:
        page = _build_page(case)
    except Exception as exc:  # noqa: BLE001 — изоляция кейса: битый источник байтов
        return _result_error(
            case.case_id, version=fallback_version, error=f"raw: {type(exc).__name__}: {exc}"
        )

    try:
        extracted = extractor.extract(page)
    except Exception as exc:  # noqa: BLE001 — изоляция кейса: адаптер бросил
        return _result_error(
            case.case_id, version=fallback_version, error=f"extract: {type(exc).__name__}: {exc}"
        )

    if extracted is None:
        return _result_error(
            case.case_id, version=fallback_version, error="extract: пустой ExtractedDoc"
        )

    version = str(getattr(extracted, "extractor_version", "") or fallback_version)
    text = str(getattr(extracted, "text", "") or "")

    error = getattr(extracted, "error", None)
    if error:
        return _result_error(case.case_id, version=version, error=str(error), chars=len(text))

    missing = [needle for needle in case.expected_substrings if needle not in text]
    forbidden_hits = [needle for needle in case.forbidden_substrings if needle in text]
    chars = len(text)
    ok = not missing and not forbidden_hits and chars >= case.min_chars
    return ExtractionCaseResult(
        case_id=case.case_id,
        ok=ok,
        extractor_version=version,
        chars=chars,
        missing=missing,
        forbidden_hits=forbidden_hits,
        error=None,
    )


def run_extraction_golden(
    cases: list[ExtractionGoldenCase],
    extractor: ExtractorPort,
) -> ExtractionReport:
    """Прогоняет extraction-golden по одному экстрактору (детерминирован, изоляция кейсов).

    Для каждого кейса строится ``RawPage`` (байты из ``raw_b64``/``raw_path``, формат — из
    ``format``), вызывается ``extractor.extract(page)`` и проверяются ``expected_substrings``/
    ``forbidden_substrings``/``min_chars``. Ошибка одного кейса не прерывает прогон.
    """
    return ExtractionReport(
        extractor_version=str(getattr(extractor, "version", "") or ""),
        results=[_run_case(case, extractor) for case in cases],
    )


def compare_extractors(
    cases: list[ExtractionGoldenCase],
    a: ExtractorPort,
    b: ExtractorPort,
) -> dict:
    """A/B-сравнение двух экстракторов на одном golden (T8.7, A3).

    Возвращает словарь с версиями/``pass_rate`` обоих, пер-кейс классификацией
    (``both_pass`` / ``both_fail`` / ``disagree``), сводкой и **рекомендацией измерения**:
    ``recommendation.auto_switch`` всегда ``False`` — A/B это измерение (A3), выбор
    фиксируется человеком/гейтом, а не автопереключением реализации.
    """
    report_a = run_extraction_golden(cases, a)
    report_b = run_extraction_golden(cases, b)
    results_b = {result.case_id: result for result in report_b.results}

    cases_out: list[dict] = []
    summary = {"both_pass": 0, "both_fail": 0, "disagree": 0}
    for result_a in report_a.results:
        result_b = results_b.get(result_a.case_id)
        b_ok = bool(result_b.ok) if result_b is not None else False
        if result_a.ok and b_ok:
            classification = "both_pass"
        elif not result_a.ok and not b_ok:
            classification = "both_fail"
        else:
            classification = "disagree"
        summary[classification] += 1
        cases_out.append(
            {
                "case_id": result_a.case_id,
                "a_ok": result_a.ok,
                "b_ok": b_ok,
                "classification": classification,
            }
        )

    if report_a.pass_rate > report_b.pass_rate:
        suggested = "a"
    elif report_b.pass_rate > report_a.pass_rate:
        suggested = "b"
    else:
        suggested = "tie"

    return {
        "a_version": report_a.extractor_version,
        "b_version": report_b.extractor_version,
        "a_pass_rate": report_a.pass_rate,
        "b_pass_rate": report_b.pass_rate,
        "cases": cases_out,
        "summary": summary,
        "recommendation": {
            "suggested": suggested,
            "auto_switch": False,
            "basis": (
                "A/B — измерение (A3): выбор экстрактора фиксируется человеком/гейтом "
                "по extraction-golden, а не автопереключением; смена extractor_version "
                "триггерит новый прогон и новую index_build_id (§13)"
            ),
        },
    }


def extractor_version_changed(
    manifest_version: str | None,
    current_version: str | None,
) -> bool:
    """Триггер re-extract: ``True``, когда версия экстрактора изменилась (§13, I19).

    Смена ``extractor_version`` — версионная граница: она означает новый extraction-прогон
    из тех же ``corpus_raw``-байтов (без ре-фетча, 02 §B.1) и новую ``index_build_id``-сборку.
    ``None`` и ``""`` трактуются как «версия не задана» и между собой не различаются.
    """
    return str(manifest_version or "") != str(current_version or "")
