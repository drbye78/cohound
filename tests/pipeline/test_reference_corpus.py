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
    CHUNK_ID_SCHEME,
    MAX_QUOTE_CHARS,
    ChunkText,
    ReferenceCorpusLayout,
    ReferenceManifest,
    coverage_from_manifest,
    derive_facts,
    parameter_phrases,
    parameter_terms,
    read_artifact_dates,
    read_manifest,
    reference_build_id,
    reference_content_digest,
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


def _chunk(hash_value: str) -> ChunkText:
    """Чанк с заданным хешем текста: отпечатку нужен только он."""
    return ChunkText(
        chunk_id="c1", doc_id="d1", url=None, text="x", content_hash=hash_value
    )


def test_content_digest_sees_the_address_scheme() -> None:
    """Байт-идентичный текст с другим адресом чанка — другая сборка.

    Дефект, ради которого схема входит в отпечаток (19.11.2026): формат адреса сменили
    (``{doc}#{no}`` → ``product:doc:chunk_no``), текст остался тем же, ``content_digest``
    совпал, ``build_id`` совпал — и пересборка ответила «уже была, ничего не перезаписано»,
    оставив в каталоге БД с неразбираемым ``chunk_id``. Разные адреса — разные сборки.
    """
    chunks = [_chunk("a" * 64), _chunk("b" * 64)]
    old = reference_content_digest(chunks, address_scheme="product:doc#chunk_no")
    new = reference_content_digest(chunks, address_scheme=CHUNK_ID_SCHEME)
    assert old != new
    # Без объявленной схемы отпечаток считается по одним текстам — прежнее поведение.
    assert reference_content_digest(chunks) == reference_content_digest(chunks)
    assert reference_content_digest(chunks) != new


def test_content_digest_ignores_chunk_order() -> None:
    """Перестановка чанков измерение не меняет — и со схемой тоже."""
    chunks = [_chunk("a" * 64), _chunk("b" * 64)]
    forward = reference_content_digest(chunks, address_scheme=CHUNK_ID_SCHEME)
    backward = reference_content_digest(
        list(reversed(chunks)), address_scheme=CHUNK_ID_SCHEME
    )
    assert forward == backward


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


def test_long_sentence_quote_stays_a_substring() -> None:
    """Длинное предложение: обрезка не делает цитату не-подстрокой.

    До 18.11.2026 обрезка добавляла «…», и ``select_quote`` отбрасывал собственный
    результат проверкой подстрочности — термин был в тексте, а ответ был «не нашли».
    Замер на PG 18: таких потерь 0 из 181, но класс дефекта — молчаливое «в корпусе
    нет информации», поэтому проверка живёт здесь, а не в отчёте.
    """
    long_sentence = "x " * (MAX_QUOTE_CHARS // 2 + 40) + "jsonb storage here."
    quote = select_quote(long_sentence, ["jsonb storage"])
    assert quote is not None, "термин в тексте есть — «не нашли» было бы ложным ответом"
    assert quote in long_sentence
    assert len(quote) <= MAX_QUOTE_CHARS


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


def test_parameter_terms_honour_declared_query_lang() -> None:
    """Язык терминов — объявленное условие, а не «все языки подряд» (замер 19.11.2026).

    При ``all`` термины всех языков склеиваются и режутся по ``MAX_TERMS``; на английском
    reference-корпусе русские термины занимали бюджет, и ``present`` было 37 против 42 при
    ``en`` (``ru`` — 13). Здесь проверяется механика: объявленный язык остаётся
    единственным, ``all`` сохраняет прежнее поведение.
    """
    param = _param().model_copy(
        update={
            "title": "Точка восстановления",
            "concept_terms": {
                "ru": ["точка восстановления", "резервная копия"],
                "en": ["point-in-time recovery", "backup"],
            },
        }
    )
    english = parameter_terms(param, query_lang="en")
    russian = parameter_terms(param, query_lang="ru")
    everything = parameter_terms(param, query_lang="all")
    # Заголовок — всегда первый термин: он на языке каталога и называет параметр.
    assert english[0] == russian[0] == everything[0]
    assert "recovery" in english and "backup" in english
    assert "резервная" in russian
    # Слова заголовка входят всегда (он называет параметр), поэтому чужой язык проверяется
    # по терминам ``concept_terms``, а не по всему списку: «восстановления» даёт заголовок.
    assert "recovery" not in russian, "объявленный язык остаётся единственным"
    assert "резервная" not in english
    # ``all`` — прежнее поведение: термины обоих языков в одном списке.
    assert "recovery" in everything and "резервная" in everything
    # Неизвестный язык — пустой набор терминов языка, а не тихий откат к ``all``.
    assert "recovery" not in parameter_terms(param, query_lang="de")


def test_parameter_terms_empty_when_only_rule_matches() -> None:
    """Нет терминов — пустой список, а не «поищем по процедуре»."""
    param = _param().model_copy(
        update={"title": "QL", "rule": "supports table mapping", "concept_terms": {}}
    )
    assert parameter_terms(param) == []


# ---------------------------------------------------------------------------
# Фразы зачёта (§8.1 шаг 7a): запрос — слова, зачёт — термин целиком
# ---------------------------------------------------------------------------


def test_parameter_phrases_keep_multiterm_whole() -> None:
    """Многословный термин — **одна** фраза, а не три слова.

    Замер 18.11.2026: разбиение на слова давало 165 ``present`` против 37 при зачёте по
    фразе — ``point-in-time recovery`` совпадало по любому из трёх слов.
    """
    param = _param().model_copy(
        update={"concept_terms": {"en": ["point-in-time recovery"]}}
    )
    assert parameter_phrases(param) == ["point-in-time recovery"]
    # А запрос по-прежнему строится из слов: BM25 токенизирует всё равно.
    spaced = _param().model_copy(
        update={"title": "", "concept_terms": {"en": ["incremental backup"]}}
    )
    assert parameter_phrases(spaced) == ["incremental backup"]
    assert parameter_terms(spaced) == ["incremental", "backup"]


def test_parameter_phrases_fall_back_to_title() -> None:
    """Авторских терминов нет — фразой становится заголовок (он называет параметр)."""
    param = _param().model_copy(
        update={"concept_terms": {}, "title": "Streaming replication"}
    )
    assert parameter_phrases(param) == ["Streaming replication"]


def test_parameter_phrases_empty_without_terms_and_title() -> None:
    """Ни терминов, ни заголовка — пустой список: засчитывать нечего."""
    param = _param().model_copy(update={"concept_terms": {}, "title": ""})
    assert parameter_phrases(param) == []


def test_parameter_phrases_order_is_deterministic() -> None:
    """Порядок — от ключей языков, а не от порядка вставки в словарь (R7)."""
    first = _param().model_copy(
        update={
            "concept_terms": {
                "ru": ["резервное копирование", "backup"],
                "en": ["backup", "backup"],
            }
        }
    )
    second = _param().model_copy(
        update={
            "concept_terms": {
                "en": ["backup", "backup"],
                "ru": ["резервное копирование", "backup"],
            }
        }
    )
    # `en` < `ru`; внутри языка — авторский порядок; дубликаты (в т.ч. межъязыковые) убраны.
    assert parameter_phrases(first) == ["backup", "резервное копирование"]
    assert parameter_phrases(second) == parameter_phrases(first)


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


def test_derive_facts_scores_phrase_not_single_word() -> None:
    """Зачёт по фразе: слова по отдельности ``present`` не дают (замер 18.11.2026).

    Слово в предложении — не утверждение о параметре: ``incremental`` и ``backup``
    встречаются в документации предка по отдельности постоянно, а параметр называется
    фразой ``incremental backup``.
    """
    param = CatalogRow(
        param_id="p3",
        title="Incremental backup",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="incremental backup mode",
        concept_terms={"en": ["incremental backup"]},
    )

    def _fact(text: str) -> FactStatus:
        chunk = ChunkText(
            chunk_id="c1",
            doc_id="d1",
            url="https://www.postgresql.org/docs/18/backup.html",
            text=text,
            content_hash="d" * 64,
        )
        return derive_facts(
            baseline_id="upstream-pg",
            version="18",
            chunks=[chunk],
            params=[param],
            corpus_build_id="ref_postgresql_18_deadbeef",
            observed_at="2026-11-17T00:00:00+00:00",
        )[0].status

    assert (
        _fact("An incremental backup is not the same as a full backup.")
        is FactStatus.PRESENT
    )
    # Слова есть, фразы нет — это не находка.
    assert _fact("The backup was incremental.") is FactStatus.UNCLEAR


# ---------------------------------------------------------------------------
# Даты наблюдений и покрытие reference-сборки (I2c/I2d для карты, §9)
# ---------------------------------------------------------------------------


def _reference_db_with_artifacts(tmp_path: Path, rows: list[tuple[str, str, str | None]]) -> Path:
    """Минимальная reference-БД: ``corpus_artifact`` + ``corpus_doc_version``."""
    import sqlite3

    db = tmp_path / "reference.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE corpus_artifact (run_id TEXT, product_id TEXT, url_key TEXT,"
        " doc_version_id TEXT, observed_at TEXT, observed_how TEXT, capture_id TEXT,"
        " root_url TEXT)"
    )
    conn.execute(
        "CREATE TABLE corpus_doc_version (doc_version_id TEXT PRIMARY KEY,"
        " product_id TEXT, doc_id TEXT)"
    )
    for version, doc_id, observed_at in rows:
        conn.execute(
            "INSERT INTO corpus_artifact VALUES ('b','postgresql',?,?,?,'live','c','r')",
            (version, version, observed_at),
        )
        conn.execute(
            "INSERT INTO corpus_doc_version VALUES (?, 'postgresql', ?)", (version, doc_id)
        )
    conn.commit()
    conn.close()
    return db


def test_artifact_dates_empty_table_is_an_index_not_none(tmp_path: Path) -> None:
    """Пустая таблица — «наблюдений не записано», а не «читать нечем» (I2d понижает `❌`).

    Замер 18.11.2026: карта PG 18 опубликовала `❌` по цитате без записи наблюдения —
    ровно потому, что пустой ответ было легко принять за «проверка неприменима».
    """
    db = _reference_db_with_artifacts(tmp_path, [])
    dates = read_artifact_dates(db, "postgresql")
    assert dates is not None
    assert dates.by_doc_version == {}
    assert dates.by_doc == {}


def test_artifact_dates_map_version_and_doc_to_observation(tmp_path: Path) -> None:
    db = _reference_db_with_artifacts(
        tmp_path,
        [
            ("dv_1", "postgresql:aaa", "2026-11-17T22:06:31+00:00"),
            ("dv_2", "postgresql:aaa", "2026-11-18T10:00:00+00:00"),
        ],
    )
    dates = read_artifact_dates(db, "postgresql")
    assert dates.by_doc_version["dv_1"] == "2026-11-17T22:06:31+00:00"
    # Один документ в двух версиях — самая ранняя дата (слабая опора, не «последняя»).
    assert dates.by_doc["postgresql:aaa"] == "2026-11-17T22:06:31+00:00"


def test_coverage_from_manifest_marks_roots_complete_with_date() -> None:
    manifest = ReferenceManifest.from_dict(
        {
            "build_id": "ref_postgresql_18_x",
            "product_id": "postgresql",
            "version": "18",
            "roots": ["https://www.postgresql.org/docs/18/"],
            "complete": True,
            "observed_at": "2026-11-17T22:06:31+00:00",
            "docs": 1136,
            "chunks": 9763,
        }
    )
    coverage = coverage_from_manifest(manifest, "postgresql")
    assert coverage.complete is True
    assert [root.root_url for root in coverage.roots] == ["https://www.postgresql.org/docs/18/"]
    assert coverage.roots[0].observed_at == "2026-11-17T22:06:31+00:00"
    assert coverage.roots[0].complete is True


def test_coverage_from_absent_manifest_is_not_claimed() -> None:
    """Манифеста нет — полнота не заявлена, а не «полно»: `bounded` не публикуется."""
    coverage = coverage_from_manifest(None, "postgresql")
    assert coverage.complete is False
    assert coverage.roots == []
