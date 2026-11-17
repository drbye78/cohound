"""План 27 §4/§8: reference-корпус — сборка, манифест, детерминированный отбор цитаты.

Проверяется то, что делает факт проверяемым: цитата — **дословная подстрока** текста
чанка (I22-дисциплина), хеш берётся у чанка (I19), а «не нашли» при неполном охвате не
превращается в отсутствие (та же дисциплина, что I2/I2c).
"""

from __future__ import annotations

from pathlib import Path

from core.domain.enums import FactReview, FactStatus, Importance, ParamType
from core.domain.models import CatalogRow
from core.pipeline.reference_corpus import (
    ChunkText,
    ReferenceCorpusLayout,
    ReferenceManifest,
    derive_facts,
    parameter_terms,
    read_manifest,
    reference_build_id,
    select_quote,
    write_manifest,
)


def _param(param_id: str = "p1") -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title="JSONB indexing",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="supports jsonb storage with expression indexing",
        concept_terms={"en": ["jsonb"]},
    )


# ---------------------------------------------------------------------------
# Идентичность и раскладка
# ---------------------------------------------------------------------------


def test_build_id_is_content_addressed() -> None:
    """Один текст — одна сборка; другой текст — другой ``build_id`` (§4)."""
    first = reference_build_id("postgresql", "18", "digest-a")
    assert first == reference_build_id("postgresql", "18", "digest-a")
    assert first != reference_build_id("postgresql", "18", "digest-b")
    assert first.startswith("ref_postgresql_18_")
    assert len(first.rsplit("_", 1)[-1]) == 8


def test_layout_puts_corpus_outside_runs(tmp_path: Path) -> None:
    layout = ReferenceCorpusLayout(
        root=tmp_path / "reference" / "pkg" / "postgresql" / "18",
        package_id="pkg",
        product_id="postgresql",
        version="18",
    ).ensure()
    assert layout.db_path.parent == layout.root
    assert "runs" not in str(layout.db_path)
    assert layout.map_dir("map_1").parent == layout.maps_dir


def test_manifest_round_trip(tmp_path: Path) -> None:
    manifest = ReferenceManifest(
        schema="reference-corpus/1",
        build_id="ref_postgresql_18_deadbeef",
        package_id="pkg",
        product_id="postgresql",
        version="18",
        content_digest="deadbeef",
        docs=3,
        chunks=42,
        extractor_version="trafilatura-html-7",
        chunker_version="policy-6",
        observed_at="2026-11-17T00:00:00+00:00",
        roots=["https://www.postgresql.org/docs/18/"],
    )
    path = write_manifest(tmp_path / "manifest.json", manifest)
    assert read_manifest(path) == manifest
    assert read_manifest(tmp_path / "missing.json") is None


# ---------------------------------------------------------------------------
# Отбор цитаты (§8.1 шаг 7a)
# ---------------------------------------------------------------------------


def test_quote_is_always_a_substring_of_the_chunk() -> None:
    text = "Intro line.\nThe database supports jsonb storage with expression indexing.\nTail."
    quote = select_quote(text, ["jsonb"])
    assert quote is not None
    assert quote in text
    assert quote.startswith("The database supports")


def test_quote_missing_term_returns_none() -> None:
    assert select_quote("nothing relevant here", ["jsonb"]) is None
    assert select_quote("", ["jsonb"]) is None


def test_parameter_terms_skip_short_words_and_keep_order() -> None:
    terms = parameter_terms(_param())
    assert "jsonb" in terms
    assert all(len(term) >= 4 for term in terms)
    assert terms == parameter_terms(_param())


# ---------------------------------------------------------------------------
# Факты (§8.1 шаги 8–9, §8.3)
# ---------------------------------------------------------------------------


def _chunks() -> list[ChunkText]:
    return [
        ChunkText(
            chunk_id="c1",
            doc_id="d1",
            url="https://www.postgresql.org/docs/18/datatype-json.html",
            text="Preamble.\nThe database supports jsonb storage with expression indexing.\n",
            content_hash="b" * 64,
        ),
        ChunkText(
            chunk_id="c2",
            doc_id="d2",
            url="https://www.postgresql.org/docs/18/other.html",
            text="Unrelated page about backups.",
            content_hash="c" * 64,
        ),
    ]


def test_derive_facts_present_carries_quote_hash_url_and_pending_review() -> None:
    facts = derive_facts(
        baseline_id="upstream-pg",
        version="18",
        chunks=_chunks(),
        params=[_param()],
        corpus_build_id="ref_postgresql_18_deadbeef",
        observed_at="2026-11-17T00:00:00+00:00",
    )
    assert len(facts) == 1
    fact = facts[0]
    assert fact.status is FactStatus.PRESENT
    assert fact.quote and fact.quote in _chunks()[0].text
    assert fact.content_hash == "b" * 64
    assert fact.url == "https://www.postgresql.org/docs/18/datatype-json.html"
    # Прибор не подписывает за человека: наследует только `accepted`.
    assert fact.review is FactReview.PENDING
    assert fact.corpus_build_id == "ref_postgresql_18_deadbeef"


def test_derive_facts_incomplete_coverage_gives_unclear_not_absent() -> None:
    """Неполнота охвата не даёт отсутствия (§8.3, та же дисциплина, что I2/I2c)."""
    param = CatalogRow(
        param_id="p2",
        title="Streaming replication slots",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="physical replication slot streaming",
    )
    facts = derive_facts(
        baseline_id="upstream-pg",
        version="18",
        chunks=_chunks(),
        params=[param],
        corpus_build_id="ref",
        observed_at="2026-11-17T00:00:00+00:00",
    )
    assert facts[0].status is FactStatus.UNCLEAR
    assert facts[0].note == "no_place_found; coverage_incomplete"
    assert facts[0].quote is None


def test_derive_facts_absent_only_with_complete_coverage() -> None:
    param = CatalogRow(
        param_id="p2",
        title="Streaming replication slots",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="physical replication slot streaming",
    )
    facts = derive_facts(
        baseline_id="upstream-pg",
        version="18",
        chunks=_chunks(),
        params=[param],
        corpus_build_id="ref",
        observed_at="2026-11-17T00:00:00+00:00",
        coverage_complete=True,
    )
    assert facts[0].status is FactStatus.ABSENT
    assert facts[0].note == "no_place_found"
