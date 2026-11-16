"""Тесты прибора длительности извлечения (`scripts/measure_extract_duration.py`, O30).

Прибор отвечает на вопрос «какой таймаут документа следует из распределения». Ошибка здесь
не падает, а выдаёт правдоподобное число, поэтому проверяются свойства, поломка которых
меняет вывод:

* **стратификация**: выборка обязана покрыть все страты, иначе p95 описывает длинные
  документы (или короткие) под именем корпуса;
* **детерминизм отбора**: один seed — одна выборка, иначе число не воспроизводится;
* **прогрев исключён**: первый документ несёт загрузку весов, и таймаут, выведенный из него,
  был бы таймаутом на «первый документ»;
* **перцентиль по рангу, не интерполяцией**: при малом n интерполяция выдаёт значения,
  которых в данных нет.

Сеть, GPU, docling и БД не нужны: проверяются чистые функции и разбор аргументов.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_extract_duration.py"
    spec = importlib.util.spec_from_file_location("measure_extract_duration", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_extract_duration"] = module
    spec.loader.exec_module(module)
    return module


duration = _load_module()


def _doc(page_count: int, name: str = "") -> dict:
    return {"path": f"/x/{name or page_count}.bin", "page_count": page_count}


# ---------------------------------------------------------------- стратификация


def test_stratified_sample_covers_every_stratum() -> None:
    """Иначе p95 описывал бы длинные документы под именем корпуса."""
    candidates = [_doc(n, f"d{n}") for n in (1, 5, 9, 20, 30, 50, 70, 120, 200, 400)]
    chosen = duration._stratified(candidates, 8, 0)
    assert {item["stratum"] for item in chosen} == {"short", "medium", "long", "huge"}
    counts: dict[str, int] = {}
    for item in chosen:
        counts[item["stratum"]] = counts.get(item["stratum"], 0) + 1
    assert set(counts.values()) == {2}


def test_stratified_sample_is_deterministic_by_seed() -> None:
    """Один seed — одна выборка: иначе опубликованное число не воспроизводится."""
    candidates = [_doc(n, f"d{n}") for n in range(1, 200)]
    first = [item["path"] for item in duration._stratified(candidates, 12, 7)]
    second = [item["path"] for item in duration._stratified(candidates, 12, 7)]
    other = [item["path"] for item in duration._stratified(candidates, 12, 8)]
    assert first == second
    assert first != other


def test_stratified_sample_does_not_take_the_first_documents() -> None:
    """Порядок в базе — это продукт, а не корпус: «первые N» мерили бы один продукт."""
    candidates = [_doc(200, f"a{i}") for i in range(50)]
    chosen = duration._stratified(candidates, 4, 0)
    assert len(chosen) == 4
    assert [item["path"] for item in chosen] != [f"/x/a{i}.bin" for i in range(4)]


def test_stratified_sample_fills_the_quota_from_a_skewed_corpus() -> None:
    """Пустая страта не съедает квоту: на стенде 149 из 155 документов — long/huge."""
    candidates = [_doc(200, f"d{i}") for i in range(30)]
    chosen = duration._stratified(candidates, 20, 0)
    assert len(chosen) == 20
    assert {item["stratum"] for item in chosen} == {"huge"}


def test_stratified_sample_never_exceeds_the_candidates() -> None:
    """Просили больше, чем есть, — берём сколько есть, без дублей."""
    chosen = duration._stratified([_doc(5, "a"), _doc(60, "b")], 20, 0)
    assert len(chosen) == 2
    assert len({item["path"] for item in chosen}) == 2


def test_stratified_sample_keeps_empty_strata_harmless() -> None:
    """Нет длинных документов — выборка из того, что есть, а не отказ."""
    chosen = duration._stratified([_doc(3), _doc(4)], 8, 0)
    assert len(chosen) == 2
    assert {item["stratum"] for item in chosen} == {"short"}


# ---------------------------------------------------------------- перцентили


def test_percentile_uses_ranks_not_interpolation() -> None:
    """Значение перцентиля обязано быть одним из наблюдённых."""
    values = [1.0, 2.0, 3.0, 4.0, 225.0]
    for fraction in (0.5, 0.95, 0.99):
        assert duration._percentile(values, fraction) in values
    assert duration._percentile(values, 0.5) == 3.0
    assert duration._percentile(values, 0.99) == 225.0


def test_percentile_of_single_value_is_that_value() -> None:
    assert duration._percentile([7.5], 0.95) == 7.5


def test_percentile_of_empty_is_an_error_not_a_zero() -> None:
    """Ноль выглядел бы замером; пусто — это отсутствие данных."""
    with pytest.raises(ValueError):
        duration._percentile([], 0.5)


def test_bootstrap_ci_brackets_the_percentile() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    low, high = duration._bootstrap_ci(values, 0.5, bootstrap=200, seed=0)
    assert low <= duration._percentile(values, 0.5) <= high
    assert low <= high


def test_bootstrap_ci_is_deterministic_by_seed() -> None:
    values = [float(n) for n in range(1, 21)]
    assert duration._bootstrap_ci(values, 0.95, bootstrap=100, seed=3) == (
        duration._bootstrap_ci(values, 0.95, bootstrap=100, seed=3)
    )


# ---------------------------------------------------------------- сводка


def test_summary_separates_document_and_page_price() -> None:
    """Дорогой документ и дорогая страница — разные вопросы; смешивать их нельзя."""
    summary = duration._summarize(
        [{"page_count": 10, "seconds": 10.0}, {"page_count": 100, "seconds": 100.0}],
        bootstrap=50,
        seed=0,
    )
    assert summary["documents"] == 2 and summary["pages"] == 110
    assert summary["document"]["p50"] == 10.0
    assert summary["page"]["p50"] == 1.0


def test_summary_counts_pages_from_the_sample_not_from_claims() -> None:
    rows = [{"page_count": 3, "seconds": 1.0}, {"page_count": 4, "seconds": 2.0}]
    assert duration._summarize(rows, bootstrap=10, seed=0)["pages"] == 7


# ---------------------------------------------------------------- аргументы


def test_defaults_declare_level_mechanism_and_sample() -> None:
    args = duration._parse_args(["--db", "state.db"])
    assert args.level == "full"
    assert args.mechanism == "structured"
    assert args.device == "cpu"
    assert args.sample == 20
    assert args.seed == 0
    assert args.bootstrap == 1000
    assert args.dry_run is False


def test_mechanism_is_a_closed_set() -> None:
    """Третьего механизма извлечения PDF не существует — прибор не должен его изобретать."""
    with pytest.raises(SystemExit):
        duration._parse_args(["--db", "s.db", "--mechanism", "hybrid"])


def test_device_is_a_closed_set_without_auto() -> None:
    """``auto`` нет: место вычисления — объявленное условие (R5)."""
    with pytest.raises(SystemExit):
        duration._parse_args(["--db", "s.db", "--device", "auto"])


def test_sample_zero_means_all() -> None:
    """``0`` — «все», а не «ни одного»."""
    assert duration._parse_args(["--db", "s.db", "--sample", "0"]).sample == 0


def test_missing_db_is_a_usage_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        duration.main(["--db", str(tmp_path / "nope.db")])
    assert "state.db" in str(exc.value)


# ---------------------------------------------------------------- прогон целиком


def _fake_measure(seconds: float, *, error: str | None = None):
    """Подмена дочернего замера: сеть, docling и GPU не нужны."""

    def _inner(item, *, mechanism, level, device, timeout):
        row = {**item, "seconds": seconds, "peak_rss_mb": 100.0, "startup_s": 20.0}
        row["error"] = error
        return row

    return _inner


def test_journal_keeps_the_data_of_a_killed_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Убитый прогон обязан оставить данные: 16.11.2026 OOM убил замер на 15-м из 20.

    Проверяется не «журнал пишется», а то, что из него **восстанавливается сводка** без
    повторного измерения.
    """
    import json

    out = tmp_path / "o.json"
    monkeypatch.setattr(
        duration,
        "_candidates",
        lambda db: [{"path": f"/x/{n}.bin", "page_count": n} for n in (5, 20, 60, 200)],
    )
    monkeypatch.setattr(duration, "_measure_one", _fake_measure(10.0))
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    assert duration.main(["--db", str(db), "--sample", "0", "--out-json", str(out)]) == 0

    journal = out.with_suffix(".jsonl")
    assert journal.is_file()
    assert len(journal.read_text().splitlines()) == 4

    # Сводка из журнала — без измерения вовсе.
    def _forbidden(*a, **k):
        raise AssertionError("--from-journal не должен ничего мерить")

    monkeypatch.setattr(duration, "_measure_one", _forbidden)
    rebuilt = tmp_path / "rebuilt.json"
    assert duration.main(
        ["--db", str(db), "--from-journal", str(journal), "--out-json", str(rebuilt)]
    ) == 0
    payload = json.loads(rebuilt.read_text())
    assert payload["conditions"]["source"] == "journal"
    assert payload["summary"]["documents"] == 4
    assert payload["summary"]["pages"] == 285


def test_journal_records_a_row_before_the_next_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Строка пишется **после** замера, а не пачкой в конце: иначе убийство теряет всё."""

    def _killed(item, *, mechanism, level, device, timeout):
        raise KeyboardInterrupt

    out = tmp_path / "o.json"
    monkeypatch.setattr(
        duration, "_candidates", lambda db: [{"path": "/x/1.bin", "page_count": 5}]
    )
    monkeypatch.setattr(duration, "_measure_one", _fake_measure(1.0))
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    assert duration.main(["--db", str(db), "--sample", "0", "--out-json", str(out)]) == 0
    journal = out.with_suffix(".jsonl")
    assert len(journal.read_text().splitlines()) == 1
    monkeypatch.setattr(duration, "_measure_one", _killed)


def test_failed_extraction_is_counted_not_silently_dropped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Отказ извлечения — тоже факт: он входит в отчёт числом, а не исчезает."""
    import json

    out = tmp_path / "o.json"
    monkeypatch.setattr(
        duration,
        "_candidates",
        lambda db: [{"path": f"/x/{n}.bin", "page_count": n} for n in (5, 10, 15, 20)],
    )
    monkeypatch.setattr(duration, "_measure_one", _fake_measure(1.0, error="OOM"))
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    # Ни одного измерения — код 1, но отчёт с отказами **записан**: результат переживает прогон.
    assert duration.main(["--db", str(db), "--sample", "0", "--out-json", str(out)]) == 1
    payload = json.loads(out.read_text())
    assert payload["conditions"]["failed"] == 4
    assert payload["summary"] is None
    assert len(payload["failed_documents"]) == 4


def test_no_documents_is_a_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Пустой корпус — отказ с кодом 2, а не пустое распределение."""
    monkeypatch.setattr(duration, "_candidates", lambda db: [])
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    assert duration.main(["--db", str(db)]) == 2


def test_dry_run_does_not_measure_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--dry-run`` — только отбор: сборка docling стоит десятки секунд и гигабайты."""

    def _forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("--dry-run не должен ничего мерить")

    monkeypatch.setattr(
        duration,
        "_candidates",
        lambda db: [{"path": str(tmp_path / "a.bin"), "page_count": 5}],
    )
    monkeypatch.setattr(duration, "_measure_one", _forbidden)
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    assert duration.main(["--db", str(db), "--dry-run"]) == 0


def test_child_prints_one_machine_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Родитель берёт **последнюю** строку с префиксом: болтовня в stdout ломает разбор."""
    import json

    class _Doc:
        text = "x" * 10

    class _Extractor:
        def extract_path(self, path: str, filename_hint: str = "") -> _Doc:
            return _Doc()

    monkeypatch.setattr(duration, "_build_extractor", lambda *a, **k: _Extractor())
    pdf = tmp_path / "a.bin"
    pdf.write_bytes(b"%PDF-1.4 fake")
    assert duration.main(["--child", "--pdf", str(pdf)]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert len(lines) == 1
    payload = json.loads(lines[0][len(duration._CHILD_PREFIX) :])
    assert payload["chars"] == 10
    assert payload["error"] is None


def test_child_failure_is_a_fact_not_a_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Отказ ребёнка печатается строкой с ошибкой: родитель запишет его, а не упадёт."""

    class _Extractor:
        def extract_path(self, path: str, filename_hint: str = "") -> object:
            raise RuntimeError("сломалось")

    monkeypatch.setattr(duration, "_build_extractor", lambda *a, **k: _Extractor())
    pdf = tmp_path / "a.bin"
    pdf.write_bytes(b"%PDF-1.4 fake")
    assert duration.main(["--child", "--pdf", str(pdf)]) == 0
    import json

    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.strip())
    payload = json.loads(line[len(duration._CHILD_PREFIX) :])
    assert "RuntimeError" in payload["error"]


def test_measure_one_treats_a_child_without_the_machine_line_as_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ребёнок упал до замера — отказ документа, а не «документ стоит 0 секунд»."""
    import subprocess

    class _Completed:
        returncode = 137  # SIGKILL: OOM
        stdout = "Killed\n"
        stderr = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _Completed())
    row = duration._measure_one(
        {"path": "/x/1.bin", "page_count": 118},
        mechanism="structured",
        level="full",
        device="cuda",
        timeout=60.0,
    )
    assert row["error"]
    assert "137" in row["error"]


def test_measure_one_timeout_is_an_error_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    def _boom(*args: object, **kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd="x", timeout=5)

    monkeypatch.setattr(subprocess, "run", _boom)
    row = duration._measure_one(
        {"path": "/x/1.bin", "page_count": 10},
        mechanism="structured",
        level="full",
        device="cpu",
        timeout=5.0,
    )
    assert "timeout" in row["error"]
