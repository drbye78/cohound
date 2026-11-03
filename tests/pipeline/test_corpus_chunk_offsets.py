"""L1b-2b: смещения куска **доезжают** до ``corpus_chunks`` и переживают reuse.

Гейт поймал дефект проводки: поля ``Chunk.char_start``/``char_end`` были объявлены,
инвариант ``CorpusChunk`` их проверял, DDL ``corpus_chunks`` их знал, но на **штатном**
пути ингеста кандидат был 5-кортежем, ``_accept_chunk_candidates`` строил
``CorpusChunk`` без смещений, а ``chunk_meta`` не имела под них колонок. В результате у
всех обычных HTML-страниц в БД лежал ``NULL``, а документ, переиспользованный из
chunk-set, терял провенанс куска. Юнит-round-trip стора такой дефект не ловит: он
проверяет хранилище, а не проводку.

Здесь проверяется именно проводка, на реальных адаптерах нарезки:

* штатный путь (``finish_one`` → ``write_document_state``): смещения в ``corpus_chunks``,
  и ``doc_text[char_start:char_end] == text`` для каждой строки — по нормализованному
  тексту документа, а не по «похожему»;
* reuse: второй прогон переиспользует chunk-set (``304``), экстрактор и чанкер не
  вызываются, числа те же — и в ``corpus_chunks``, и в ``state.db`` (``chunk_meta``);
* абзацный чанкер (``text-para-4``) несёт **точные** смещения: ``doc_text[start:end] ==
  text`` для каждой строки, а не ``NULL`` (``NULL`` вернулся бы к состоянию «кусок не
  срез» и потерял бы провенанс, который теперь есть).

Нейтральные данные (§1.3): ``p1``, ``docs.test``, локальные ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
import itertools
import json
from pathlib import Path
from typing import Any

import pytest

from core.adapters.chunker_markdown import MarkdownChunkerPort
from core.adapters.chunker_text import TextChunkerPort
from core.adapters.md_ast import parse_markdown
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.ports.types import ExtractedDoc, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-chunk-offsets"
URL = "https://docs.test/page"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ETAG = 'W/"offsets-1"'
LAST_MODIFIED = "Wed, 21 Oct 2026 07:28:00 GMT"

#: Markdown-документ: заведомо больше одного куска при бюджете по умолчанию (1500).
MARKDOWN_DOC = (
    "# Раздел первый\n\n"
    "## Подраздел\n\n"
    + "\n\n".join(
        f"Абзац {index}: " + "слово " * 40 for index in range(20)
    )
    + "\n"
)


# --------------------------------------------------------------------------- #
# Харнесс: план, прогон, порты-заглушки
# --------------------------------------------------------------------------- #


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
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


def _make_run(store: SqliteStore) -> None:
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _Crawler:
    """Краулер ``url -> bytes``: сеть не нужна, валидаторы — для ``304``."""

    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes], *, not_modified: bool = False) -> None:
        self.pages = dict(pages)
        self.not_modified = not_modified
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls.append(url)
        confirmed = (
            self.not_modified and plan.if_none_match is not None and plan.if_none_match == ETAG
        )
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            lang="ru",
            status_code=304 if confirmed else 200,
            etag=ETAG,
            last_modified=LAST_MODIFIED,
            not_modified=confirmed,
        )


class _Extractor:
    """Экстрактор «страница = markdown-текст» (без адаптеров, A1)."""

    version = "extractor-1"

    def __init__(self, text: str) -> None:
        self.text = text
        self.calls = 0

    def extract(self, page: RawPage, route: Any = None) -> ExtractedDoc:
        self.calls += 1
        return ExtractedDoc(
            text=self.text,
            content_hash=content_hash(self.text),
            extractor_version=self.version,
        )


class _NoExtraction(_Extractor):
    """Экстрактор, который обязан **не** вызываться при переиспользовании."""

    def __init__(self) -> None:
        super().__init__("")

    def extract(self, page: RawPage, route: Any = None) -> ExtractedDoc:
        raise AssertionError("переиспользование не должно пере-извлекать документ")


class _NoChunking:
    """Чанкер, который обязан **не** вызываться при переиспользовании.

    ``version`` — версия настоящего markdown-чанкера: версия входит в ключ
    переиспользования, и «другая версия» означала бы честный отказ от reuse (повторная
    нарезка), а не проверку того, что чанкер не позвали.
    """

    version = MarkdownChunkerPort.version

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Any]:
        raise AssertionError("переиспользование не должно пере-нарезать документ")


def _run(tmp_path: Path, store: SqliteStore, **kwargs: Any) -> Any:
    return build_corpus(
        _plan(),
        kwargs.pop("crawler", _Crawler({URL: b"page"})),
        kwargs.pop("extractor", _Extractor(MARKDOWN_DOC)),
        kwargs.pop("chunker", MarkdownChunkerPort()),
        store,
        urls={"p1": [URL]},
        raw_root=tmp_path,
        run_id=RUN_ID,
        **kwargs,
    )


def _chunk_rows(store: SqliteStore) -> list[dict[str, Any]]:
    """Строки ``corpus_chunks`` из **run-БД** — то, что увидит оценщик."""
    return [
        dict(row)
        for row in store.conn.execute(
            "SELECT chunk_no, text, char_start, char_end FROM corpus_chunks"
            " ORDER BY chunk_no"
        )
    ]


# --------------------------------------------------------------------------- #
# 1. Штатный путь: смещения доезжают до corpus_chunks
# --------------------------------------------------------------------------- #


def test_offsets_reach_corpus_chunks_on_the_regular_ingest_path(
    tmp_path: Path,
) -> None:
    """``finish_one`` пишет смещения в ``corpus_chunks``, и срез по ним равен тексту.

    Читается **БД**, а не объект чанка: дефект был именно в проводке, и проверка
    «в памяти всё хорошо» его не поймала бы. Нормализованный текст документа берётся
    тем же способом, что и у чанкера (``parse_markdown(...).text``), поэтому сверка
    буквальная: ``doc_text[char_start:char_end] == text``.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _run(tmp_path, store)
        assert result.status == "ok", (result.status, result.errors)
        rows = _chunk_rows(store)
    finally:
        store.close()

    assert len(rows) > 2, "документ обязан разрезаться, иначе кейс пуст"
    doc_text = parse_markdown(MARKDOWN_DOC).text
    for row in rows:
        assert row["char_start"] is not None and row["char_end"] is not None, row
        assert doc_text[row["char_start"] : row["char_end"]] == row["text"]
    # P1 через БД: тексты строк — тот же документ, без потерь и перекрытий.
    assert "".join(row["text"] for row in rows) == doc_text


def test_stored_chunk_text_keeps_its_edge_whitespace(
    tmp_path: Path,
) -> None:
    """Края куска принадлежат срезу: ``corpus_chunks.text`` не обрезан (``policy-3``).

    До ``policy-3`` ``CorpusChunk`` наследовал ``str_strip_whitespace=True``: текст в БД
    обрезался, хотя ``content_hash`` считался по необрезанному срезу, и
    ``doc_text[char_start:char_end]`` переставал совпадать с ``text``. Здесь это
    проверяется на строке из БД, а не на объекте чанкера.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _run(tmp_path, store)
        rows = _chunk_rows(store)
    finally:
        store.close()

    assert any(row["text"] != row["text"].strip() for row in rows), (
        "нарезка не дала кусков с краевым пробелом — тест не проверяет хранение",
    )
    doc_text = parse_markdown(MARKDOWN_DOC).text
    for row in rows:
        assert doc_text[row["char_start"] : row["char_end"]] == row["text"]


# --------------------------------------------------------------------------- #
# 2. Reuse: chunk-set возвращает тот же провенанс
# --------------------------------------------------------------------------- #


def test_reused_chunk_set_keeps_offsets_in_run_db_and_state(
    tmp_path: Path,
) -> None:
    """Второй прогон переиспользует chunk-set → те же числа в ``corpus_chunks`` и ``state.db``.

    Проверяется оба носителя: run-БД (что увидит оценщик) и durable ``chunk_meta`` (что
    переживёт прогон). Именно второй путь терял смещения: ``ReusedChunk`` их не нёс,
    ``chunk_meta`` не хранила, и документ из повторного прогона отличался от первого.
    """
    with SqliteStateStore(":memory:") as state:
        first_store = SqliteStore(":memory:")
        _make_run(first_store)
        try:
            first = _run(
                tmp_path,
                first_store,
                state=state,
                product_modes={"p1": "incremental"},
                crawler=_Crawler({URL: b"page"}),
            )
            assert first.status == "ok", (first.status, first.errors)
            first_rows = _chunk_rows(first_store)
        finally:
            first_store.close()

        versions = state.iter_document_versions("p1")
        assert len(versions) == 1
        dv_id = versions[0].doc_version_id
        first_meta = state.iter_doc_chunks(dv_id)

        second_store = SqliteStore(":memory:")
        _make_run(second_store)
        try:
            second = _run(
                tmp_path,
                second_store,
                state=state,
                product_modes={"p1": "incremental"},
                crawler=_Crawler({URL: b"page"}, not_modified=True),
                extractor=_NoExtraction(),
                chunker=_NoChunking(),
            )
            assert second.status == "ok", (second.status, second.errors)
            second_rows = _chunk_rows(second_store)
        finally:
            second_store.close()

        second_meta = state.iter_doc_chunks(dv_id)

    expected = [
        (row["chunk_no"], row["text"], row["char_start"], row["char_end"])
        for row in first_rows
    ]
    assert expected, "первый прогон не записал чанков"
    assert all(start is not None and end is not None for _, _, start, end in expected)
    assert [
        (row["chunk_no"], row["text"], row["char_start"], row["char_end"])
        for row in second_rows
    ] == expected
    # durable-подложка: провенанс переживает прогон, а не только живёт в его БД.
    assert [
        (row.chunk_no, row.char_start, row.char_end) for row in first_meta
    ] == [(row.chunk_no, row.char_start, row.char_end) for row in second_meta]
    assert all(row.char_start is not None and row.char_end is not None for row in second_meta)


# --------------------------------------------------------------------------- #
# 3. Абзацный чанкер: точные срезы доезжают до БД
# --------------------------------------------------------------------------- #


def test_paragraph_chunker_offsets_are_exact_slices(tmp_path: Path) -> None:
    """``text-para-4``: кусок — точный срез, и это видно в ``corpus_chunks``.

    Прежняя политика (``text-para-3``) обрезала края и склеивала предложения, поэтому
    смещения были ``NULL``. Точный срез без ``strip()`` снимает это ограничение: ``NULL``
    теперь означал бы потерю провенанса, а неверное смещение — ложь. Проверяется и
    провенанс строки (``doc_text[start:end] == text``), и покрытие без дыр.
    """
    plain = "\n\n".join(f"Абзац {index}. " + "слово " * 30 for index in range(6))
    with SqliteStateStore(":memory:") as state:
        store = SqliteStore(":memory:")
        _make_run(store)
        try:
            result = _run(
                tmp_path,
                store,
                extractor=_Extractor(plain),
                chunker=TextChunkerPort(),
                state=state,
                product_modes={"p1": "incremental"},
            )
            assert result.status == "ok", (result.status, result.errors)
            rows = _chunk_rows(store)
        finally:
            store.close()

        versions = state.iter_document_versions("p1")
        assert len(versions) == 1
        meta = state.iter_doc_chunks(versions[0].doc_version_id)

    assert rows, "абзацный чанкер не дал чанков — кейс пуст"
    assert "".join(row["text"] for row in rows) == plain, "P1: склейка ≠ вход"
    for row in rows:
        start, end = row["char_start"], row["char_end"]
        assert start is not None and end is not None, row
        assert end - start == len(row["text"])
        assert plain[start:end] == row["text"], "P4: text ≠ срез"
    assert rows[0]["char_start"] == 0
    assert rows[-1]["char_end"] == len(plain)
    for previous, following in itertools.pairwise(rows):
        assert previous["char_end"] == following["char_start"], "покрытие с дырой"
    assert meta and all(
        row.char_start is not None and row.char_end is not None for row in meta
    )


def test_legacy_chunk_meta_row_with_null_offsets_reads_as_none(
    tmp_path: Path,
) -> None:
    """``NULL`` из ``chunk_meta`` читается как ``None`` — и переиспользование не выдумывает ``0``.

    Строка, записанная до появления колонок (или абзацным чанкером), обязана вернуться
    в наблюдение как «смещений нет»: ``0`` подменил бы неизвестное известным.
    """
    with SqliteStateStore(":memory:") as state:
        store = SqliteStore(":memory:")
        _make_run(store)
        try:
            _run(
                tmp_path,
                store,
                extractor=_Extractor(MARKDOWN_DOC),
                chunker=MarkdownChunkerPort(),
                state=state,
                product_modes={"p1": "incremental"},
            )
        finally:
            store.close()

        versions = state.iter_document_versions("p1")
        assert len(versions) == 1
        dv_id = versions[0].doc_version_id
        # Эмуляция строки без провенанса: колонки есть, значения — NULL.
        state._conn.execute(  # noqa: SLF001 - прямой откат к до-L1b-2b состоянию строки
            "UPDATE chunk_meta SET char_start = NULL, char_end = NULL "
            "WHERE doc_version_id = ?",
            (dv_id,),
        )
        state._conn.commit()  # noqa: SLF001
        restored = state.iter_doc_chunks(dv_id)
        assert restored
        assert all(row.char_start is None and row.char_end is None for row in restored)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__]))