"""Частичное извлечение на границе корпуса: признак → ``CorpusDoc`` → покрытие (I2/I2c).

Проверяется дефект класса «тихая потеря»: документ, прочитанный на 7 страницах из 2763,
обязан быть виден и в корпусе, и в покрытии, иначе молчание непрочитанных страниц
читается как «вендор не заявил», а затем публикуется как ``❌``.

Тесты офлайн: SQLite в памяти, краулер/экстрактор/чанкер — структурные заглушки.
Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    CatalogRow,
    Cell,
    CellState,
    Cohort,
    CohortEntry,
    CorpusCoverage,
    CrawlItemStatus,
    CrawlSessionItem,
    DenialStrength,
    EvidenceScope,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
    Status,
)
from core.domain.invariants import check_absence_coverage
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    PARTIAL_REASON_UNSTATED,
    build_corpus,
    derive_corpus_coverage,
)
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-partial"
FIXTURES = Path(__file__).parent / "fixtures"
TS = "2026-01-01T00:00:00+00:00"
URL = "https://docs.test/p1/manual"
URL_SECOND = "https://docs.test/p1/other"

# --------------------------------------------------------------------------- #
# Заглушки портов (структурные, как в соседних тестах фазы)
# --------------------------------------------------------------------------- #

class _Crawler:
    """Краулер: ``url → bytes`` (страница всегда отдаётся успешно)."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: object = None) -> RawPage:
        self.calls.append(url)
        return RawPage(url=url, content=self.pages[url], fetched_at=TS, lang="en")

class _Extractor:
    """Экстрактор: обычный текст; для ``partial_urls`` — частичное извлечение.

    Частичность — ровно тот факт, который сообщает docling на ``PARTIAL_SUCCESS``:
    текст прочитанных страниц есть, и вместе с ним статус и счётчики.
    """

    version = "extractor-1"

    def __init__(self, *, partial_urls: set[str] | None = None) -> None:
        self.partial_urls = set(partial_urls or ())

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        text = page.content.decode("utf-8", errors="replace")
        if page.url in self.partial_urls:
            return ExtractedDoc(
                text=text,
                content_hash=content_hash(page.content),
                extractor_version=self.version,
                page_count=2763,
                partial=True,
                partial_reason=(
                    "docling: статус конвертации PARTIAL_SUCCESS, получено страниц 7 из "
                    "2763; уровень full: обещана структура целого документа — получен "
                    "текст части"
                ),
                pages_converted=7,
            )
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(page.content),
            extractor_version=self.version,
            page_count=1,
        )

class _Chunker:
    """Чанкер: один чанк на документ."""

    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
        ]

def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]

def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )

def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store

def _build(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    partial_urls: set[str] | None = None,
) -> tuple[object, SqliteStore]:
    store = _store()
    result = build_corpus(
        _plan(),
        _Crawler(pages),
        _Extractor(partial_urls=partial_urls),
        _Chunker(),
        store,
        urls={"p1": sorted(pages)},
        raw_root=tmp_path,
        run_id=RUN_ID,
        out_dir=tmp_path / "runs",
    )
    return result, store

# --------------------------------------------------------------------------- #
# Деривация покрытия (чистая функция)
# --------------------------------------------------------------------------- #

def _item(url: str, status: CrawlItemStatus) -> CrawlSessionItem:
    return CrawlSessionItem(
        session_id="crawl:run", product_id="p1", url=url, status=status
    )

def test_partial_extraction_makes_coverage_incomplete() -> None:
    """Частичный документ — блокирующий класс ``partial_extraction``, а не «прочитано всё»."""
    coverage = derive_corpus_coverage(
        product_id="p1",
        items=[_item(URL, CrawlItemStatus.FETCHED)],
        truncated=False,
        computed_at=TS,
        partial_docs=[URL],
    )

    assert coverage.complete is False
    assert coverage.observed_sources == coverage.expected_sources == 1
    assert coverage.blocked == {"partial_extraction": 1}
    assert coverage.blocking_failures == {"partial_extraction": 1}
    assert coverage.partial_blocks == {"docs.test": {"partial_extraction": 1}}
    assert coverage.unreadable_origins == []

def test_coverage_without_partial_documents_is_unchanged() -> None:
    """Нет частичных документов — прежнее поведение: полный корпус остаётся полным."""
    coverage = derive_corpus_coverage(
        product_id="p1",
        items=[
            _item(URL, CrawlItemStatus.FETCHED),
            _item(URL_SECOND, CrawlItemStatus.FETCHED),
        ],
        truncated=False,
        computed_at=TS,
    )

    assert coverage.complete is True
    assert coverage.blocked == {}
    assert coverage.blocking_failures == {}

# --------------------------------------------------------------------------- #
# Сквозной путь: извлечение → corpus_docs → corpus_coverage
# --------------------------------------------------------------------------- #

def test_partial_document_reaches_corpus_and_coverage(tmp_path: Path) -> None:
    """Признак экстрактора виден в ``corpus_docs`` и делает охват неполным."""
    result, store = _build(
        tmp_path,
        pages={URL: b"read only part of the manual"},
        partial_urls={URL},
    )
    try:
        docs = {doc.doc_id: doc for doc in store.iter_corpus_docs(RUN_ID)}
        assert len(docs) == 1
        doc = next(iter(docs.values()))
        assert doc.partial is True
        assert "PARTIAL_SUCCESS" in (doc.partial_reason or "")
        assert (doc.pages_converted, doc.page_count) == (7, 2763)
        assert doc.extracted_chars == len("read only part of the manual")

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.blocked == {"partial_extraction": 1}

        # Операторский канал: строка продукта называет частичность, а не только
        # «документов 1».
        notes = [entry for entry in result.log_entries if isinstance(entry, str)]
        assert any("частично" in note for note in notes), notes
    finally:
        store.close()

def test_full_document_keeps_coverage_complete(tmp_path: Path) -> None:
    """Документ без частичности не блокирует корпус зря (регресс-защита)."""
    result, store = _build(tmp_path, pages={URL: b"whole document"})
    try:
        doc = next(iter(store.iter_corpus_docs(RUN_ID)))
        assert doc.partial is False
        assert doc.partial_reason is None
        assert doc.pages_converted is None

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is True
        assert "partial_extraction" not in coverage.blocked

        notes = [entry for entry in result.log_entries if isinstance(entry, str)]
        assert not any("частично" in note for note in notes), notes
    finally:
        store.close()

def test_partial_without_stated_reason_still_marks_the_corpus(tmp_path: Path) -> None:
    """Экстрактор не назвал причину — документ всё равно помечен, а не молчит.

    Инвариант ``CorpusDoc`` требует причины, поэтому «причина не сообщена» записывается
    как **факт отсутствия** провенанса: терять текст из-за незаполненного поля нельзя,
    а выдавать документ за полный — тем более.
    """
    class _SilentPartialExtractor(_Extractor):
        def extract(
            self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
        ) -> ExtractedDoc:
            return ExtractedDoc(
                text="текст части",
                content_hash=content_hash(page.content),
                extractor_version=self.version,
                partial=True,
                page_count=10,
                pages_converted=3,
            )

    store = _store()
    try:
        build_corpus(
            _plan(),
            _Crawler({URL: b"bytes"}),
            _SilentPartialExtractor(),
            _Chunker(),
            store,
            urls={"p1": [URL]},
            raw_root=tmp_path,
            run_id=RUN_ID,
            out_dir=tmp_path / "runs",
        )
        doc = next(iter(store.iter_corpus_docs(RUN_ID)))
        assert doc.partial is True
        assert doc.partial_reason == PARTIAL_REASON_UNSTATED
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None and coverage.complete is False
    finally:
        store.close()

# --------------------------------------------------------------------------- #
# I2c: неполный (частичный) корпус не даёт молчаливого ❌
# --------------------------------------------------------------------------- #

def _partial_coverage() -> CorpusCoverage:
    return derive_corpus_coverage(
        product_id="p1",
        items=[_item(URL, CrawlItemStatus.FETCHED)],
        truncated=False,
        computed_at=TS,
        partial_docs=[URL],
    )

def _absent_cell(scope: EvidenceScope) -> Cell:
    """``❌``-ячейка, собранная в обход валидатора (проверяется чистый инвариант)."""
    return Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.EXPLICIT,
        evidence_scope=scope,
    )

def test_partial_coverage_downgrades_undocumented_absent_to_unknown() -> None:
    """Механизм ✗ механики: неполный охват делает класс отрицания ``unobserved``.

    Это первый барьер (до I2c): оценщик, не заявивший класса отрицания, получает
    ``unobserved`` — значит ``❌`` без свидетельства вендора на таком корпусе вообще не
    собирается. Проверяется именно деривация класса, а не текст сообщения.
    """
    from core.pipeline.evaluate import _coverage_vector, _denial_basis

    coverage = _partial_coverage()

    assert _denial_basis(_coverage_vector(coverage), coverage) == "unobserved"
    # Полный корпус даёт «вендор не заявил» — прежнее поведение не изменилось.
    full = derive_corpus_coverage(
        product_id="p1",
        items=[_item(URL, CrawlItemStatus.FETCHED)],
        truncated=False,
        computed_at=TS,
    )
    assert _denial_basis(_coverage_vector(full), full) == "not_stated"

def test_domain_rejects_absent_cell_with_unobserved_denial() -> None:
    """I2: ``❌`` с классом ``unobserved`` домен не собирает — публикация невозможна."""
    with pytest.raises(ValueError):
        Cell(
            run_id=RUN_ID,
            product_id="p1",
            param_id="param_a",
            state=CellState.EVALUATED,
            status=Status.ABSENT,
            absence_rule=AbsenceRuleType.EXPLICIT,
            evidence_scope=EvidenceScope(
                denial_strength=DenialStrength.UNOBSERVED,
                reason="охват неполон: частичное извлечение",
            ),
        )

def test_i2c_blocks_bounded_absent_on_partially_read_corpus() -> None:
    """``bounded`` без принятия риска на частичном корпусе — нарушение I2c.

    Объект строится через ``model_construct``: модель ``EvidenceScope`` такую комбинацию
    не собирает (двойная защита), а проверяется здесь именно чистый инвариант I2c.
    """
    scope = EvidenceScope.model_construct(
        denial_strength=DenialStrength.BOUNDED,
        reason="конечный справочник объявлен каталогом",
        enumerated=True,
        scope_acceptance=None,
    )
    cell = _absent_cell(scope)
    violations = check_absence_coverage(cell, _partial_coverage())

    codes = {violation.code for violation in violations}
    assert "I2c" in codes
    assert any("partial_extraction" in violation.message for violation in violations)

def test_i2c_accepts_bounded_absent_with_risk_acceptance() -> None:
    """Человеческое принятие риска (``scope_acceptance``) — единственный путь к ``❌``."""
    cell = _absent_cell(
        EvidenceScope(
            denial_strength=DenialStrength.BOUNDED,
            reason="конечный справочник объявлен каталогом",
            enumerated=True,
            scope_acceptance="analyst",
        )
    )
    assert check_absence_coverage(cell, _partial_coverage()) == []

def test_i2c_still_rejects_unobserved_denial_on_partial_corpus() -> None:
    """Класс ``unobserved`` (вывод из неполного охвата) ``❌`` не даёт ни при чём."""
    cell = _absent_cell(
        EvidenceScope(denial_strength=DenialStrength.UNOBSERVED, reason="охват неполон")
    )
    violations = check_absence_coverage(cell, _partial_coverage())

    assert {violation.code for violation in violations} == {"I2c"}

def test_explicit_vendor_denial_outlives_partial_coverage() -> None:
    """Дословное отрицание вендора — свидетельство, а не вывод из полноты охвата.

    Это **не** послабление: ``explicit`` опирается на цитату вендора, поэтому
    непрочитанные страницы его не отменяют. Разделение классов — Вариант 2 (§2.3/I2c).
    """
    cell = _absent_cell(
        EvidenceScope(
            denial_strength=DenialStrength.EXPLICIT,
            citation_quote="возможность не поддерживается",
            reason="дословный отказ",
        )
    )
    assert check_absence_coverage(cell, _partial_coverage()) == []

def test_partial_coverage_cannot_be_declared_complete() -> None:
    """Модель не даёт объявить охват полным, если частичность записана в ``blocked``."""
    coverage = _partial_coverage()
    assert coverage.complete is False
    # И подтверждение обратного направления: полный корпус без частичных — цел.
    assert derive_corpus_coverage(
        product_id="p1",
        items=[_item(URL, CrawlItemStatus.FETCHED)],
        truncated=False,
        computed_at=TS,
    ).complete is True
