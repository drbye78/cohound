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
from core.config.deploy import CrawlConfig, DeployConfig
from core.domain.enums import (
    DocumentLevel,
    FrozenSource,
    PdfDevice,
    PdfPipeline,
    RefreshPolicy,
    WorkClass,
)
from core.domain.models import CrawlRunConfig
from core.pipeline.resources import (
    DOCLING_PEAK_MB,
    MEM_BUDGET_ENV,
    MODEL_FREE_PEAK_MB,
    PARENT_FLOOR_MB,
    MemoryBudgetError,
    workers_for_class,
    workers_from_budget,
)

# 09 §4.4: ``state/{package_id}/`` — точка ресурса **пакета**, и с файловым локом она
# эксклюзивна. Часть тестов гоняет ``run`` по фикстуре ``tests/fixtures/e2e`` прямо из
# репозитория, поэтому без изоляции все тесты и все xdist-воркеры делят
# ``<repo>/state/e2e/.lock`` и падают по очереди с «пакет уже используется» — флак
# параллелизма, а не регрессия. Раскладку ``state`` модуль не утверждает, поэтому
# фикстура предмет проверки не подменяет.
pytestmark = pytest.mark.usefixtures("isolated_state_dir")

E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"
ROOT = "https://p1.test/docs"

#: Полный уровень: 4833 MB на документ. Бюджет 1000 MB не помещает ни одного воркера
#: (1000 − 500 пола < 4833), то есть конфигурация заведомо невозможна.
UNFITTABLE_BUDGET = 1000


def _preflight(
    *,
    run_crawl: CrawlRunConfig | None = None,
    mem_budget_mb: int | None = UNFITTABLE_BUDGET,
    derive: bool = True,
    roots: list[str] | None = None,
    refresh_policy: RefreshPolicy | str | None = None,
    frozen_source: FrozenSource | None = None,
    state: object | None = None,
    products: list[str] | None = None,
) -> tuple[MemoryBudgetError | None, tuple[str, ...]]:
    return preflight_extraction_budget(
        roots=roots if roots is not None else [ROOT],
        deploy=None,
        run_crawl=run_crawl,
        profiles={},
        mem_budget_mb=mem_budget_mb,
        derive_extract_workers=derive,
        refresh_policy=refresh_policy,
        frozen_source=frozen_source,
        state=state,
        products=products,
    )


# --------------------------------------------------------------------------- #
# Состояние пакета для счёта документов к пересборке
# --------------------------------------------------------------------------- #


def _state_store(tmp_path: Path) -> object:
    """Пустое durable-состояние пакета (``state.db``) для счёта пересборки."""
    from core.store.state_store import SqliteStateStore

    return SqliteStateStore(tmp_path / "state.db")


def _doc_version(
    state: object,
    product_id: str,
    *,
    no: int,
    chunker_version: str | None,
    with_chunks: bool = True,
    current: bool = True,
) -> None:
    """Кладёт активную версию документа; ``with_chunks`` — есть ли у неё chunk-set.

    ``current=True`` — версия является текущей для URL (``url_state``), то есть именно её
    увидит проход без сети; ``False`` — историческая версия того же документа, которую
    прогон не читает.
    """
    from core.ports.state import ChunkRow, DocumentVersionRow, UrlStateRow

    dv_id = f"dv_{product_id}_{no}"
    state.upsert_document_version(  # type: ignore[attr-defined]
        DocumentVersionRow(
            doc_version_id=dv_id,
            doc_id=f"{product_id}:h{no}",
            product_id=product_id,
            url=f"https://{product_id}.test/{no}",
            content_hash=f"h{no}",
            extractor_version="extract-1",
            level="full",
            raw_path=f"/raw/{no}.bin",
            chunker_version=chunker_version,
            first_seen_run_id="run-1",
            last_seen_run_id="run-1",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            status="active",
        )
    )
    if current:
        state.upsert_url_state(  # type: ignore[attr-defined]
            UrlStateRow(
                product_id=product_id,
                url=f"https://{product_id}.test/{no}",
                normalized_url=f"https://{product_id}.test/{no}",
                doc_version_id=dv_id,
            )
        )
    if not with_chunks:
        return
    state.put_chunk_text(f"c{no}", f"текст {no}")  # type: ignore[attr-defined]
    state.replace_doc_chunks(  # type: ignore[attr-defined]
        dv_id,
        [
            ChunkRow(
                chunk_id=f"{dv_id}:0",
                doc_version_id=dv_id,
                product_id=product_id,
                chunk_no=0,
                content_hash=f"c{no}",
                chunker_version=chunker_version or "unknown",
            )
        ],
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

    Отказа нет, но предупреждение о **сумме** окон классов есть, и оно тоже обязано
    называть разрешённый уровень: ``text`` стоит 855 MB на PDF-документ, а не 4833
    (``full``). Иначе проверка «взят разрешённый уровень» перестала бы ловить подмену
    ровно там, где уровень впервые стал виден числом.
    """
    assert workers_from_budget(DocumentLevel.TEXT, 2000) == 1
    ok, warnings = _preflight(
        run_crawl=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        mem_budget_mb=2000,
    )
    assert ok is None
    assert len(warnings) == 1, warnings
    assert f"pdf_structured 1×{DOCLING_PEAK_MB[DocumentLevel.TEXT]}" in warnings[0]
    assert f"1×{DOCLING_PEAK_MB[DocumentLevel.FULL]}" not in warnings[0]

    error, _ = _preflight(
        run_crawl=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        mem_budget_mb=UNFITTABLE_BUDGET,
    )
    assert isinstance(error, MemoryBudgetError)
    assert error.level is DocumentLevel.TEXT
    assert error.peak_mb == DOCLING_PEAK_MB[DocumentLevel.TEXT]
    assert f"level peak={DOCLING_PEAK_MB[DocumentLevel.TEXT]} MB" in error.describe()


def test_preflight_warns_when_sum_of_class_windows_exceeds_budget() -> None:
    """Сумма окон по классам — предупреждение с числами, а не отказ (A3).

    Окно каждого класса считается так, будто класс в корпусе один, поэтому сумма
    ``window × peak`` может превысить бюджет при том, что каждое окно по отдельности
    помещается. Это не повод отказывать: потребитель ведёт общий счётчик памяти и
    откладывает старты (``class_skipped_for_budget``), то есть сумма держится **по
    построению**. Предупреждение нужно, чтобы упор в бюджет узла был виден до прогона:
    иначе «полосы разошлись по памяти» читалось бы как «полосы узкие».

    Числа в сообщении берутся из тех же :func:`workers_for_class`, что и у потребителя,
    поэтому здесь проверяется их наличие и порядок величины, а не переписанная формула.
    """
    # Бюджет стенда 07.11.2026 ≈ 4.5 GB, уровень ``layout_tables``: PDF помещается
    # (3679 ≤ 4000), значит отказа быть не должно — и ровно поэтому сумма видна.
    budget = 4500
    level = DocumentLevel.LAYOUT_TABLES
    error, warnings = _preflight(
        run_crawl=CrawlRunConfig(document_level=level), mem_budget_mb=budget
    )
    assert error is None
    assert len(warnings) == 1, warnings
    warning = warnings[0]
    model_free, _ = workers_for_class(
        WorkClass.MODEL_FREE, level, PdfPipeline.STRUCTURED, budget
    )
    pdf_peak = DOCLING_PEAK_MB[level]
    # Имена классов и оба слагаемых — в тексте: без них предупреждение не проверяемо.
    assert f"model_free {model_free}×{MODEL_FREE_PEAK_MB}" in warning
    assert f"pdf_structured 1×{pdf_peak}" in warning
    assert f"{budget - PARENT_FLOOR_MB} MB" in warning
    # Сумма названа числом и она действительно больше доступного бюджета.
    total = model_free * MODEL_FREE_PEAK_MB + pdf_peak
    assert f"= {total} MB" in warning
    assert total > budget - PARENT_FLOOR_MB
    # Отказом это не является, и токен метрики назван — куда смотреть после прогона.
    assert "class_skipped_for_budget" in warning
    assert "отказом это не является" in warning


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


def test_preflight_skips_offline_reuse_only_on_a_measured_zero(
    tmp_path: Path,
) -> None:
    """``offline`` + все chunk-set годны → пропуск **с числом**, а не по слову режима.

    Пропуск законен ровно тогда, когда пересобирать нечего: счёт по состоянию — «0 из M».
    Прежняя посылка «при ``offline`` документы не читаются» ложна (негодный chunk-set
    пересобирается из сохранённых байтов), поэтому пропуск теперь опирается на факт.
    """
    from core.adapters.chunker_family import composite_chunker_version

    state = _state_store(tmp_path)
    current = composite_chunker_version()
    for no in range(3):
        _doc_version(state, "p1", no=no, chunker_version=current)

    error, warnings = _preflight(refresh_policy=RefreshPolicy.OFFLINE, state=state)
    assert error is None
    # Не молчание: «не проверял» названо, иначе отсутствие отказа неотличимо от проверки.
    assert len(warnings) == 1, warnings
    assert "из чанков" in warnings[0]
    assert "offline" in warnings[0]
    assert "0 из 3" in warnings[0], warnings[0]


def test_preflight_applies_when_offline_rebuild_is_unavoidable(
    tmp_path: Path,
) -> None:
    """``offline`` + N негодных наборов → гейт применяется и называет N, уровень и пик.

    Измерено 22–23.11.2026 на стенде: прогон дошёл до ``VmHWM`` 4313 MB при
    ``MemAvailable`` ~1.6 GB, а гейт молчал — верил ``refresh``.
    """
    from core.adapters.chunker_family import composite_chunker_version

    state = _state_store(tmp_path)
    current = composite_chunker_version()
    _doc_version(state, "p1", no=0, chunker_version=current)
    _doc_version(state, "p1", no=1, chunker_version="md-heading-4+text-para-2")
    _doc_version(state, "p1", no=2, chunker_version=current, with_chunks=False)

    error, warnings = _preflight(refresh_policy=RefreshPolicy.OFFLINE, state=state)
    assert isinstance(error, MemoryBudgetError)
    assert "2 из 3" in warnings[0], warnings[0]
    assert "пересобраны из сохранённых байтов" in warnings[0]
    assert "offline" in warnings[0]
    window = " ".join(warnings)
    assert f"level={DocumentLevel.FULL.value}" in window, window
    assert f"пик={DOCLING_PEAK_MB[DocumentLevel.FULL]} MB" in window, window
    assert f"бюджет {UNFITTABLE_BUDGET} MB" in window, window
    assert "окно 0" in window, window
    # Отказ остаётся отказом и несёт те же числа (describe), а не «не помещается».
    assert f"budget={UNFITTABLE_BUDGET} MB" in error.describe()


def test_preflight_offline_rebuild_passes_when_window_fits(
    tmp_path: Path,
) -> None:
    """Пересборка есть, но окно помещается → гейт применён и **не** отказывает.

    Гейт — не потолок: если документы помещаются, работа идёт, а факт пересборки и числа
    окна видны предупреждением.
    """
    state = _state_store(tmp_path)
    _doc_version(state, "p1", no=0, chunker_version="md-heading-4+text-para-2")

    # full = 4833 MB + пол 500 MB: 6000 MB помещает ровно одного воркера.
    error, warnings = _preflight(
        refresh_policy=RefreshPolicy.OFFLINE,
        mem_budget_mb=6000,
        state=state,
    )
    assert error is None
    assert "1 из 1" in warnings[0], warnings
    assert "окно 1" in " ".join(warnings)


def test_preflight_offline_without_state_warns_with_numbers_and_does_not_refuse() -> None:
    """Состояние недоступно → предупреждение с числами формулы, **не** отказ.

    Отказ без факта был бы ложным предохранителем: он запрещал бы работу, которой может
    не быть, и вынуждал бы завышать бюджет (объявлять ложь) или менять ось измерения.
    """
    error, warnings = _preflight(refresh_policy=RefreshPolicy.OFFLINE, state=None)
    assert error is None
    text = " ".join(warnings)
    assert "из чанков" in text
    assert "offline" in text
    assert "НЕ проверено" in text, text
    assert "недоступно" in text, text
    assert f"level={DocumentLevel.FULL.value}" in text, text
    assert f"пик={DOCLING_PEAK_MB[DocumentLevel.FULL]} MB" in text, text
    assert f"бюджет {UNFITTABLE_BUDGET} MB" in text, text
    assert "окно 0" in text, text


def test_preflight_does_not_refuse_frozen_chunks() -> None:
    """``frozen`` + ``frozen_source=chunks`` — тот же корпус из чанков, тот же счёт."""
    error, _ = _preflight(
        refresh_policy=RefreshPolicy.FROZEN, frozen_source=FrozenSource.CHUNKS
    )
    assert error is None


def test_preflight_still_refuses_frozen_bytes() -> None:
    """``frozen`` + ``bytes`` — байты пере-извлекаются: отказ остаётся в силе.

    Это граница правки: пропуск привязан к ``corpus_source=CHUNKS``, а не к слову
    «frozen». Иначе объявленный источник менял бы исполнение гейта.
    """
    error, _ = _preflight(
        refresh_policy=RefreshPolicy.FROZEN, frozen_source=FrozenSource.BYTES
    )
    assert error is not None


def test_preflight_counts_current_documents_not_version_history(tmp_path: Path) -> None:
    """Единица счёта — текущая версия URL, а не строка истории ``document_version``.

    Замена документа по контенту не вытесняет прежнюю версию (она остаётся ``active``),
    поэтому счёт по ``document_version`` назвал бы работой то, что прогон не читает.
    Измерено 23.11.2026 на стенде: 17 858 активных версий против **2 057** URL с негодным
    набором — разница в 8.7×, и это была бы ложь в тексте гейта.
    """
    from core.adapters.chunker_family import composite_chunker_version

    state = _state_store(tmp_path)
    current = composite_chunker_version()
    _doc_version(state, "p1", no=0, chunker_version=current)
    # Прежняя версия того же документа: статус ``active``, но URL на неё не указывает.
    _doc_version(
        state, "p1", no=1, chunker_version="md-heading-4+text-para-2", current=False
    )

    error, warnings = _preflight(refresh_policy=RefreshPolicy.OFFLINE, state=state)
    assert error is None
    assert "0 из 1" in warnings[0], warnings[0]


def test_preflight_offline_without_budget_names_count_and_no_window(
    tmp_path: Path,
) -> None:
    """Бюджета нет — окно не выводится (число не выдумывается), но пересборка названа.

    ``mem_budget_mb=None`` — «бюджет неизвестен», а не «окно 0»: печатать окно здесь
    значило бы выдать отсутствие факта за измерение (A3).
    """
    state = _state_store(tmp_path)
    _doc_version(state, "p1", no=0, chunker_version="md-heading-4+text-para-2")

    error, warnings = _preflight(
        refresh_policy=RefreshPolicy.OFFLINE, mem_budget_mb=None, state=state
    )
    assert error is None
    assert "1 из 1" in warnings[0], warnings
    assert not any("окно" in warning for warning in warnings), warnings


def test_preflight_rebuild_count_is_scoped_to_run_products(tmp_path: Path) -> None:
    """Счёт пересборки — только по продуктам прогона: состояние живёт дольше прогона.

    Негодные наборы продукта вне области (срез ``--only-product``, выбывший продукт) не
    должны давать число, по которому гейт откажет в работе, которой не будет.
    """
    from core.adapters.chunker_family import composite_chunker_version

    state = _state_store(tmp_path)
    current = composite_chunker_version()
    _doc_version(state, "p1", no=0, chunker_version=current)
    _doc_version(state, "other", no=1, chunker_version="md-heading-4+text-para-2")

    error, warnings = _preflight(
        refresh_policy=RefreshPolicy.OFFLINE, state=state, products=["p1"]
    )
    assert error is None
    assert "0 из 1" in warnings[0], warnings[0]

    # Тот же прогон без сужения области видит негодный набор и применяет гейт.
    error_wide, warnings_wide = _preflight(
        refresh_policy=RefreshPolicy.OFFLINE, state=state
    )
    assert isinstance(error_wide, MemoryBudgetError)
    assert "1 из 2" in warnings_wide[0], warnings_wide[0]


def test_preflight_refresh_is_unchanged_by_state(tmp_path: Path) -> None:
    """``refresh``/``auto`` — прежний отказ независимо от содержимого состояния.

    Регрессия: счёт пересборки относится только к корпусу из чанков; при сетевом
    проходе документы читаются по построению, и состояние на решение не влияет.
    """
    from core.adapters.chunker_family import composite_chunker_version

    state = _state_store(tmp_path)
    _doc_version(state, "p1", no=0, chunker_version=composite_chunker_version())

    for policy in (RefreshPolicy.REFRESH, RefreshPolicy.AUTO):
        error, warnings = _preflight(refresh_policy=policy, state=state)
        assert isinstance(error, MemoryBudgetError), policy
        assert warnings == (), policy


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


def test_run_offline_with_empty_state_counts_zero_and_does_not_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Сквозь ``run``: ``--refresh offline`` на пустом состоянии — «0 из 0», не отказ.

    Проверяется проводка ``state``/``products`` из composition root: счёт идёт по
    открытому состоянию прогона, и пустое состояние — это измеренный ноль («пересборка не
    требуется»), а не повод отказать в заведомо не помещающемся бюджете.
    """
    monkeypatch.setenv(MEM_BUDGET_ENV, str(UNFITTABLE_BUDGET))
    runs_dir = tmp_path / "runs"
    argv = _argv(runs_dir, "f0-offline-empty-state", E2E_FIXTURES, _urls_file(tmp_path))
    argv.extend(["--refresh", "offline"])
    code = main(argv)
    # G1/G4 остаются человеческими: дальше preflight'а бюджета прогон дошёл — отказа по
    # памяти нет, несмотря на заведомо не помещающийся бюджет.
    assert code == EXIT_BLOCKED
    payload = capsys.readouterr().out
    assert "из чанков" in payload, payload
    assert "0 из 0" in payload, payload


# --------------------------------------------------------------------------- #
# P9: cuda без устройства — отказ на входе, а не тихий CPU
# --------------------------------------------------------------------------- #


def test_cuda_device_without_gpu_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``crawl.pdf_device=cuda`` при недоступном CUDA — ``WiringError`` с числами (R5).

    Тихий CPU-фолбэк запрещён: ``extractor_version``/``extractor_stack`` назвали бы GPU,
    на котором прогон не исполнялся. Сообщение обязано нести факты (версия torch, факт
    ``cuda_available``, объём карты), иначе оператор не может ни понять отказ, ни
    исправить вход.
    """
    from core.cli import wiring

    monkeypatch.setattr(wiring, "cuda_available", lambda: False)
    monkeypatch.setattr(wiring, "cuda_total_vram_mb", lambda: None)

    with pytest.raises(wiring.WiringError) as excinfo:
        wiring.preflight_pdf_device(
            deploy=DeployConfig(crawl=CrawlConfig(pdf_device=PdfDevice.CUDA))
        )
    text = str(excinfo.value)
    assert "crawl.pdf_device=cuda" in text
    assert "cuda_available=False" in text
    assert "device_vram_mb=нет устройства" in text
    assert "torch=" in text


def test_cpu_device_never_requires_cuda(monkeypatch: pytest.MonkeyPatch) -> None:
    """``cpu`` — штатный режим: доступность CUDA не проверяется вовсе.

    torch при этом **не импортируется** (ось не должна стоить памяти сама по себе):
    проверка «а вдруг GPU есть» на каждый прогон была бы той же ошибкой, что чтение
    устройства из окружения.
    """
    from core.cli import wiring

    def _must_not_probe() -> bool:
        raise AssertionError("под cpu доступность CUDA не спрашивается")

    monkeypatch.setattr(wiring, "cuda_available", _must_not_probe)
    device = wiring.preflight_pdf_device(
        deploy=DeployConfig(crawl=CrawlConfig(pdf_device=PdfDevice.CPU))
    )
    assert device is PdfDevice.CPU


def test_cuda_device_is_accepted_when_gpu_is_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ось исполни́ма — прогон получает объявленное устройство, а не «что найдётся»."""
    from core.cli import wiring

    monkeypatch.setattr(wiring, "cuda_available", lambda: True)
    assert (
        wiring.preflight_pdf_device(
            deploy=DeployConfig(crawl=CrawlConfig(pdf_device=PdfDevice.CUDA))
        )
        is PdfDevice.CUDA
    )
