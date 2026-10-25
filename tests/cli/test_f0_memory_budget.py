"""F0: preflight бюджета памяти экстракции — отказ на входе, а не OOM на документе.

Проверяется §3.3 стратегии производственной архитектуры
(``docs/architecture/11-production-architecture-strategy.md``):

* бюджет памяти резолвится **в composition root** (``core.cli.wiring``), а не читается
  пайплайном из окружения;
* окно экстракции выводится из бюджета (``derived``) и при нуле воркеров прогон
  **отказывает** с ``EXIT_USAGE`` и числами в сообщении — заведомо невозможную
  конфигурацию нельзя начинать;
* явно объявленная ось ``crawl.extract_workers`` сильнее бюджета (воля оператора): отказа
  нет, но расхождение видно предупреждением с числами;
* ``document_level=off`` и ``split`` — не повод отказывать: в первом случае воркеры не
  нужны, во втором они живут на edge со своим бюджетом.

Сквозные проверки используют фикстуру ``tests/fixtures/e2e`` (реальный прогон
останавливается на ``g1`` до сети/LLM): отказ обязан случиться **до** исполнения фаз.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from core.cli.errors import EXIT_USAGE
from core.cli.main import EXIT_BLOCKED, main
from core.cli.wiring import (
    preflight_extraction_budget,
    resolve_extraction_budget,
)
from core.domain.enums import DocumentLevel
from core.domain.models import CrawlRunConfig
from core.pipeline.resources import (
    DOCLING_PEAK_MB,
    MEM_BUDGET_ENV,
    PARENT_FLOOR_MB,
    MemoryBudgetError,
    workers_from_budget,
)

E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"
ROOT = "https://p1.test/docs"

#: Полный уровень: 3107 MB на документ. Бюджет 1000 MB не помещает ни одного воркера
#: (1000 − 500 пола < 3107), то есть конфигурация заведомо невозможна.
UNFITTABLE_BUDGET = 1000


def _preflight(
    *,
    run_crawl: CrawlRunConfig | None = None,
    mem_budget_mb: int | None = UNFITTABLE_BUDGET,
    derive: bool = True,
    roots: list[str] | None = None,
) -> tuple[MemoryBudgetError | None, tuple[str, ...]]:
    return preflight_extraction_budget(
        roots=roots if roots is not None else [ROOT],
        deploy=None,
        run_crawl=run_crawl,
        profiles={},
        mem_budget_mb=mem_budget_mb,
        derive_extract_workers=derive,
    )


# --------------------------------------------------------------------------- #
# Preflight: отказ несёт числа, а не «не помещается»
# --------------------------------------------------------------------------- #

def test_preflight_refuses_unfittable_level_and_names_numbers() -> None:
    """``full`` на бюджете 1000 MB → отказ с бюджетом, уровнем, пиком, полом и воркерами."""
    error, warnings = _preflight()
    assert isinstance(error, MemoryBudgetError)
    assert warnings == ()
    assert error.budget_mb == UNFITTABLE_BUDGET
    assert error.level is DocumentLevel.FULL
    assert error.peak_mb == DOCLING_PEAK_MB[DocumentLevel.FULL]
    assert error.parent_floor_mb == PARENT_FLOOR_MB
    assert error.workers == 0
    message = error.describe()
    for expected in (
        f"budget={UNFITTABLE_BUDGET} MB",
        f"level={DocumentLevel.FULL.value}",
        f"level peak={DOCLING_PEAK_MB[DocumentLevel.FULL]} MB",
        f"parent floor={PARENT_FLOOR_MB} MB",
        "workers=0",
        MEM_BUDGET_ENV,
    ):
        assert expected in message, message


def test_preflight_uses_resolved_level_not_default() -> None:
    """Уровень берётся **разрешённый** (run-слой), а не дефолтный ``full``.

    ``text`` помещается в 2000 MB (1 воркер), поэтому отказа нет; на 1000 MB — отказ,
    и сообщение называет именно ``text`` с его пиком, а не ``full``.
    """
    assert workers_from_budget(DocumentLevel.TEXT, 2000) == 1
    ok, warnings = _preflight(
        run_crawl=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        mem_budget_mb=2000,
    )
    assert ok is None
    assert warnings == ()

    error, _ = _preflight(
        run_crawl=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        mem_budget_mb=UNFITTABLE_BUDGET,
    )
    assert isinstance(error, MemoryBudgetError)
    assert error.level is DocumentLevel.TEXT
    assert error.peak_mb == DOCLING_PEAK_MB[DocumentLevel.TEXT]
    assert f"level peak={DOCLING_PEAK_MB[DocumentLevel.TEXT]} MB" in error.describe()


def test_preflight_does_not_refuse_declared_extract_workers() -> None:
    """Явная ось ``extract_workers`` сильнее бюджета: предупреждение с числами, не отказ."""
    error, warnings = _preflight(run_crawl=CrawlRunConfig(extract_workers=2))
    assert error is None
    assert len(warnings) == 1
    warning = warnings[0]
    assert "crawl.extract_workers: 2" in warning
    assert f"{UNFITTABLE_BUDGET} MB" in warning
    assert "помещают 0" in warning
    assert f"peak={DOCLING_PEAK_MB[DocumentLevel.FULL]} MB" in warning
    assert MEM_BUDGET_ENV in warning


def test_preflight_does_not_refuse_level_off() -> None:
    """``off`` — документной экстракции нет: ноль воркеров здесь не «не помещается»."""
    error, warnings = _preflight(
        run_crawl=CrawlRunConfig(document_level=DocumentLevel.OFF)
    )
    assert error is None
    assert warnings == ()


def test_preflight_is_noop_when_window_is_not_derived() -> None:
    """``derive_extract_workers=False`` (``split``/инъецированные порты) — preflight молчит.

    Под ``split`` воркеры живут на edge с собственным бюджетом: ядро не вправе отказывать
    прогону по памяти своего хоста.
    """
    error, warnings = _preflight(derive=False)
    assert error is None
    assert warnings == ()


def test_preflight_without_budget_does_not_refuse() -> None:
    """Бюджет неизвестен — выводить нечего: отказа нет (значение не выдумывается)."""
    error, warnings = _preflight(mem_budget_mb=None)
    assert error is None
    assert warnings == ()


# --------------------------------------------------------------------------- #
# Резолв бюджета: одна точка чтения, флаг зависит от профиля зон
# --------------------------------------------------------------------------- #

def test_resolve_budget_prefers_declared_env() -> None:
    """Явный ``COHOUND_MEM_BUDGET_MB`` сильнее измерения свободной памяти."""
    budget, derive = resolve_extraction_budget(
        "local", env={MEM_BUDGET_ENV: "4096"}, available_mb=65536
    )
    assert (budget, derive) == (4096, True)


def test_resolve_budget_split_does_not_derive_window() -> None:
    """``split`` — окно ядра не выводится: воркеры на edge, бюджет чужой машины."""
    budget, derive = resolve_extraction_budget(
        "split", env={MEM_BUDGET_ENV: "4096"}, available_mb=65536
    )
    assert budget == 4096
    assert derive is False


def test_resolve_budget_ignores_measured_peaks_for_injected_ports() -> None:
    """Инъецированные порты — не тот стек экстракции: окно из бюджета не выводится (A3)."""
    _, derive = resolve_extraction_budget(
        "local", env={MEM_BUDGET_ENV: "4096"}, injected_ports=True
    )
    assert derive is False


# --------------------------------------------------------------------------- #
# Сквозь CLI: отказ до исполнения фаз
# --------------------------------------------------------------------------- #

def _argv(runs_dir: Path, run_id: str, package: Path, urls: Path | None = None) -> list[str]:
    argv = [
        "run",
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--runs-dir",
        str(runs_dir),
        "--run-id",
        run_id,
        "--json",
    ]
    if urls is not None:
        argv.extend(["--urls", str(urls)])
    return argv


def _urls_file(tmp_path: Path) -> Path:
    path = tmp_path / "corpus_urls.yaml"
    path.write_text('p1:\n  - "https://p1.test/docs"\n', encoding="utf-8")
    return path


def test_run_refuses_unfittable_budget_before_phases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``run`` на невыполнимом бюджете → ``EXIT_USAGE``, числа в выводе, фаз не было.

    Отказ — это **вход**, а не сбой исполнения: прогон, который заведомо не помещается,
    не должен доходить до первого документа и падать OOM-killer'ом без трейсбека.
    """
    monkeypatch.setenv(MEM_BUDGET_ENV, str(UNFITTABLE_BUDGET))
    runs_dir = tmp_path / "runs"
    code = main(
        _argv(runs_dir, "f0-budget-refused", E2E_FIXTURES, _urls_file(tmp_path))
    )
    assert code == EXIT_USAGE
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert f"budget={UNFITTABLE_BUDGET} MB" in output
    assert f"level peak={DOCLING_PEAK_MB[DocumentLevel.FULL]} MB" in output
    # Ни одна фаза не исполнялась: журналов фаз (``logs/{phase}.jsonl``) нет — отказ
    # случился до сборки фаз. Каталог ``logs/`` при этом существует намеренно: отказ
    # тоже обязан оставить улику, поэтому ``console.log`` ставится до проверки бюджета
    # (иначе причина отказа жила бы только в консоли, которую никто не сохранит).
    logs = runs_dir / "f0-budget-refused" / "logs"
    assert not list(logs.glob("*.jsonl")), "журнал фазы при отказе по бюджету"
    console = logs / "console.log"
    assert console.exists(), "отказ по бюджету не оставил улики в console.log"
    assert f"budget={UNFITTABLE_BUDGET} MB" in console.read_text(encoding="utf-8")


def test_run_proceeds_when_extract_workers_is_declared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Объявленная ось ``extract_workers`` — не отказ, а видимое предупреждение с числами."""
    monkeypatch.setenv(MEM_BUDGET_ENV, str(UNFITTABLE_BUDGET))
    package = tmp_path / "package"
    shutil.copytree(
        E2E_FIXTURES,
        package,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    run_yaml = package / "run.yaml"
    run_yaml.write_text(
        run_yaml.read_text(encoding="utf-8")
        + "\ncrawl:\n  extract_workers: 2\n",
        encoding="utf-8",
    )
    runs_dir = tmp_path / "runs"
    code = main(_argv(runs_dir, "f0-budget-declared", package, _urls_file(tmp_path)))
    # Гейт G1 остаётся человеческим (S9): дальше префлайта прогон дошёл — отказа по
    # бюджету нет, исполнение по воле оператора.
    assert code == EXIT_BLOCKED
    payload = capsys.readouterr().out
    assert "crawl.extract_workers: 2" in payload
    assert f"{UNFITTABLE_BUDGET} MB" in payload
