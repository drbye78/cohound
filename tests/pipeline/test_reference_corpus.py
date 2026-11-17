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
    build_id = "ref_postgresql_18_deadbeef"
    # Сборка — каталог внутри версии: две сборки одной версии сосуществуют.
    assert layout.db_path(build_id) == layout.build_dir(build_id) / "reference.db"
    assert layout.db_path(build_id).parent.parent == layout.builds_dir
    assert layout.manifest_path(build_id) == layout.build_dir(build_id) / "manifest.json"
    assert "runs" not in str(layout.db_path(build_id))
    assert layout.map_dir(build_id, "map_1").parent == layout.maps_dir(build_id)
    # Байты общие для сборок версии, а не для одной сборки.
    assert layout.raw_dir == layout.root / "raw"
    assert layout.current_path == layout.root / "current.json"
    assert layout.lock_path == layout.root / ".lock"


def test_manifest_round_trip(tmp_path: Path) -> None:
    manifest = ReferenceManifest(
        schema="reference-corpus/2",
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
        complete=False,
        incomplete_reasons=["truncated:max_documents"],
        skipped={"duplicate_text": 2},
        errors=["pkg: https://x/fail: fetch: Timeout: таймаут"],
    )
    path = write_manifest(tmp_path / "manifest.json", manifest)
    assert read_manifest(path) == manifest
    assert read_manifest(tmp_path / "missing.json") is None


def test_manifest_old_schema_reads_with_incomplete_provenance() -> None:
    """Манифест ``/1`` читается, но «полнота» не выдумывается: ``complete=False``.

    Старый формат поля не нёс, значит факта о полноте нет; ``complete=True`` по умолчанию
    разрешил бы ``absent`` без носителя (I2/I2c).
    """
    manifest = ReferenceManifest.from_dict(
        {
            "schema": "reference-corpus/1",
            "build_id": "ref_x_1_aaaaaaaa",
            "content_digest": "aaaaaaaa",
            "docs": 1,
            "chunks": 1,
        }
    )
    assert manifest.complete is False
    assert manifest.incomplete_reasons == []
    assert manifest.skipped == {}
    assert manifest.errors == []


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


def test_parameter_terms_ignore_rule_text() -> None:
    """Слова процедуры проверки — не термины поиска (замер 18.11.2026).

    ``rule`` у каталога ``ru-pg-dbms`` — служебная формулировка («…явно указанного
    значения в исходной comparative table…»), и слово ``table`` находилось в таблице
    стандарта SQL: параметр «Разработчик» получал ``present`` с цитатой
    ``| X040 |  | Basic table mapping |  |``. 16 из 49 ``present`` были такими.
    """
    param = _param().model_copy(update={"rule": "supports table mapping and provenance"})
    terms = parameter_terms(param)
    assert "table" not in terms
    assert "mapping" not in terms
    assert "provenance" not in terms
    assert "jsonb" in terms


def test_parameter_terms_empty_when_only_rule_matches() -> None:
    """Нет терминов — пустой список, а не «поищем по процедуре»."""
    param = _param().model_copy(
        update={"title": "QL", "rule": "supports table mapping", "concept_terms": {}}
    )
    assert parameter_terms(param) == []


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
