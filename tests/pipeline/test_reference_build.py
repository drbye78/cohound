"""План 27 §4/§8.1: сборка reference-корпуса — раскладка, идентичность, провенанс.

Проверяется то, что делает наследование проверяемым: сборка адресуется **содержимым**
(два прогона на тех же байтах — один ``build_id`` и ``reused``), scope не расширяется
молча, а неполнота прохода видна машинными причинами (``absent`` допустим только при
``complete=True``, I2/I2c-дисциплина).

Сеть не трогается: краулер/экстрактор/чанкер — фейковые порты. Писатель — настоящий
``SqliteStore``: проверяется, что БД сборки открывается тем же гейтом схемы, что и
корпус когорты, и что в неё лёг именно собранный текст.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from core.domain.enums import ExtractionRoute
from core.pipeline.reference_build import (
    ReferenceBuildLimits,
    ReferenceBuildUnavailable,
    build_reference_corpus,
)
from core.pipeline.reference_corpus import (
    ReferenceBuildAmbiguous,
    ReferenceBuildMissing,
    ReferenceCorpusLayout,
    list_builds,
    read_current_build,
    resolve_build,
    write_current_build,
)
from core.ports.types import Chunk, ExtractedDoc, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

ROOT = "https://docs.example.test/guide/"


# ---------------------------------------------------------------------------
# Фейковые порты (без сети)
# ---------------------------------------------------------------------------


class FakeCrawler:
    """Отдаёт заданные байты по URL; ``fail`` — URL, на которых бросает."""

    version = "fake-crawler-1"

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        content_type: str | None = "text/html",
        fail: tuple[str, ...] = (),
    ) -> None:
        self.pages = pages
        self.content_type = content_type
        self.fail = set(fail)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        if url in self.fail:
            raise RuntimeError("сеть недоступна")
        if url not in self.pages:
            raise KeyError(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at="2026-11-17T00:00:00+00:00",
            content_type=self.content_type,
            status_code=200,
        )


class FakeExtractor:
    """Детерминированный текст: по умолчанию — сами байты; ``errors`` — сбой извлечения."""

    version = "fake-extractor-1"

    def __init__(
        self,
        texts: dict[str, str] | None = None,
        *,
        errors: tuple[str, ...] = (),
    ) -> None:
        self.texts = texts or {}
        self.errors = set(errors)

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        if page.url in self.errors:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(""),
                extractor_version=self.version,
                error="читатель сломался",
            )
        text = self.texts.get(page.url, page.content.decode("utf-8"))
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
        )


class FakeChunker:
    """1–2 куска: режет по первой строке, чтобы смещения были точным срезом."""

    version = "fake-chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        text = doc.text
        cut = text.find("\n")
        spans = [(0, len(text))] if cut <= 0 else [(0, cut + 1), (cut + 1, len(text))]
        return [
            Chunk(
                chunk_id=f"{document_id}#{no}",
                chunk_no=no,
                text=text[start:end],
                heading_path="",
                context_prefix="",
                content_hash=content_hash(text[start:end])[:16],
                char_start=start,
                char_end=end,
            )
            for no, (start, end) in enumerate(spans)
        ]


class FakeRobots:
    """Отказ по объявленному набору URL (политика не читает сеть)."""

    def __init__(self, denied: tuple[str, ...]) -> None:
        self.denied = set(denied)

    def allowed(self, url: str) -> bool:
        return url not in self.denied


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _layout(tmp_path: Path) -> ReferenceCorpusLayout:
    return ReferenceCorpusLayout(
        root=tmp_path / "reference" / "pkg" / "upstream" / "18",
        package_id="pkg",
        product_id="upstream",
        version="18",
    )


def _build(
    layout: ReferenceCorpusLayout,
    pages: dict[str, bytes],
    *,
    roots: tuple[str, ...] = (ROOT,),
    limits: ReferenceBuildLimits | None = None,
    extractor: FakeExtractor | None = None,
    robots: Any | None = None,
    fail: tuple[str, ...] = (),
    content_type: str | None = "text/html",
) -> Any:
    return build_reference_corpus(
        layout=layout,
        roots=list(roots),
        crawler=FakeCrawler(pages, content_type=content_type, fail=fail),
        extractor=extractor or FakeExtractor(),
        chunker=FakeChunker(),
        limits=limits,
        robots=robots,
        sleep=lambda _seconds: None,
        store_factory=SqliteStore,
    )


def _rows(db_path: Path) -> tuple[int, int]:
    """``(docs, chunks)`` прямо из БД сборки — сверка с опубликованными числами."""
    conn = sqlite3.connect(db_path)
    try:
        docs = conn.execute("SELECT COUNT(*) FROM corpus_docs").fetchone()[0]
        chunks = conn.execute("SELECT COUNT(*) FROM corpus_chunks").fetchone()[0]
    finally:
        conn.close()
    return int(docs), int(chunks)


def _page(*links: str, body: str = "Текст страницы.") -> bytes:
    anchors = "".join(f'<a href="{link}">x</a>' for link in links)
    return f"<html><body><p>{body}</p>{anchors}</body></html>".encode()


# ---------------------------------------------------------------------------
# Сборка: раскладка, идентичность, публикация
# ---------------------------------------------------------------------------


def test_build_creates_db_and_manifest_with_matching_counts(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    pages = {
        ROOT: _page("/guide/next.html"),
        "https://docs.example.test/guide/next.html": _page(body="Вторая страница."),
    }
    result = _build(layout, pages, limits=ReferenceBuildLimits(max_depth=1))

    assert result.db_path == layout.db_path(result.build_id)
    assert result.db_path.is_file()
    assert layout.manifest_path(result.build_id).is_file()
    # Сборка живёт в builds/{build_id}/, а не в каталоге версии.
    assert result.db_path.parent.parent == layout.builds_dir

    docs, chunks = _rows(result.db_path)
    assert (docs, chunks) == (result.docs, result.chunks) == (2, 2)

    manifest = json.loads(layout.manifest_path(result.build_id).read_text("utf-8"))
    assert manifest["build_id"] == result.build_id
    assert manifest["docs"] == 2
    assert manifest["chunks"] == 2
    assert manifest["complete"] is True
    assert manifest["incomplete_reasons"] == []
    assert manifest["roots"] == [ROOT]
    assert manifest["extractor_version"] == "fake-extractor-1"
    assert manifest["chunker_version"] == "fake-chunker-1"

    # Чанки адресованы сборке: build_id чанка — идентичность reference-корпуса.
    conn = sqlite3.connect(result.db_path)
    try:
        build_ids = {row[0] for row in conn.execute("SELECT build_id FROM corpus_chunks")}
    finally:
        conn.close()
    assert build_ids == {result.build_id}


def test_build_id_is_content_addressed_and_second_run_is_reused(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    pages = {ROOT: _page(body="Единственная страница.")}

    first = _build(layout, pages)
    assert first.reused is False
    db_before = first.db_path.stat()
    manifest_before = layout.manifest_path(first.build_id).stat()

    second = _build(layout, pages)
    assert second.build_id == first.build_id
    assert second.reused is True
    # Ничего не перезаписано: тот же файл (inode) и та же метка записи.
    db_after = first.db_path.stat()
    manifest_after = layout.manifest_path(first.build_id).stat()
    assert (db_after.st_ino, db_after.st_mtime_ns) == (
        db_before.st_ino,
        db_before.st_mtime_ns,
    )
    assert (manifest_after.st_ino, manifest_after.st_mtime_ns) == (
        manifest_before.st_ino,
        manifest_before.st_mtime_ns,
    )
    # Переиспользованная сборка описывается опубликованным манифестом, а не новым проходом.
    assert (second.docs, second.chunks) == (first.docs, first.chunks)
    assert read_current_build(layout) == first.build_id


def test_different_text_gives_new_build_and_keeps_the_old_one(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    first = _build(layout, {ROOT: _page(body="Первый текст.")})

    second = _build(layout, {ROOT: _page(body="Другой текст.")})
    assert second.build_id != first.build_id
    assert second.reused is False
    # Старая сборка цела и по-прежнему читается.
    assert first.db_path.is_file()
    assert _rows(first.db_path) == (1, 1)
    assert set(list_builds(layout)) == {first.build_id, second.build_id}
    assert read_current_build(layout) == second.build_id


def test_staging_directory_is_not_left_behind(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _build(layout, {ROOT: _page()})
    leftovers = [p.name for p in layout.builds_dir.iterdir() if p.name.startswith(".")]
    assert leftovers == []


# ---------------------------------------------------------------------------
# Scope, robots, лимиты, дедуп
# ---------------------------------------------------------------------------


def test_scope_keeps_foreign_host_and_undeclared_subdomain_out(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    pages = {
        ROOT: _page(
            "https://other.test/page.html",
            "https://edu.docs.example.test/page.html",
            "/guide/inside.html",
        ),
        "https://docs.example.test/guide/inside.html": _page(body="Своя страница."),
        # Байты «чужих» страниц положены намеренно: если scope протечёт, они попадут.
        "https://other.test/page.html": _page(body="Чужое."),
        "https://edu.docs.example.test/page.html": _page(body="Поддомен."),
    }
    crawler = FakeCrawler(pages)
    result = build_reference_corpus(
        layout=layout,
        roots=[ROOT],
        crawler=crawler,
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
        limits=ReferenceBuildLimits(max_depth=1),
        sleep=lambda _seconds: None,
        store_factory=SqliteStore,
    )

    assert result.docs == 2
    assert crawler.calls == [
        ROOT,
        "https://docs.example.test/guide/inside.html",
    ]
    assert result.skipped["out_of_scope"] == 2
    # Вне объявленного пространства — не потеря: полнота прохода не страдает.
    assert result.complete is True


def test_robots_refusal_is_counted_and_document_absent(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    second = "https://docs.example.test/guide/secret.html"
    pages = {ROOT: _page("/guide/secret.html"), second: _page(body="Секрет.")}
    result = _build(
        layout,
        pages,
        limits=ReferenceBuildLimits(max_depth=1),
        robots=FakeRobots((second,)),
    )

    assert result.docs == 1
    assert result.skipped["robots"] == 1
    # Запрет сайта — это непрочитанная часть пространства, а не «корпус исчерпан».
    assert result.complete is False
    assert "skipped:robots" in result.manifest.incomplete_reasons


def test_max_documents_truncates_and_marks_incomplete(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    pages = {
        ROOT: _page("/guide/a.html", "/guide/b.html"),
        "https://docs.example.test/guide/a.html": _page(body="A."),
        "https://docs.example.test/guide/b.html": _page(body="B."),
    }
    result = _build(
        layout,
        pages,
        limits=ReferenceBuildLimits(max_depth=1, max_documents=1),
    )

    assert result.docs == 1
    assert result.complete is False
    assert result.truncated == ["max_documents"]
    assert "truncated:max_documents" in result.manifest.incomplete_reasons
    # Числа опубликованы и совпадают с БД: «осталось N» видно, а не спрятано.
    assert _rows(result.db_path) == (1, 1)


def test_extract_error_is_visible_and_excluded(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    broken = "https://docs.example.test/guide/broken.html"
    pages = {ROOT: _page("/guide/broken.html"), broken: _page(body="Сломанное.")}
    result = _build(
        layout,
        pages,
        limits=ReferenceBuildLimits(max_depth=1),
        extractor=FakeExtractor(errors=(broken,)),
    )

    assert result.docs == 1
    assert result.skipped["extract_error"] == 1
    assert result.complete is False
    assert any(broken in error for error in result.errors)
    assert "skipped:extract_error" in result.manifest.incomplete_reasons


def test_fetch_error_is_visible(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    pages = {ROOT: _page()}
    result = _build(layout, pages, fail=(ROOT,))

    assert result.docs == 0
    assert result.skipped["fetch_error"] == 1
    assert result.complete is False
    assert any("fetch" in error for error in result.errors)


def test_duplicate_text_yields_one_document(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    same = "Одинаковый текст."
    pages = {
        ROOT: _page("/guide/copy.html", body=same),
        "https://docs.example.test/guide/copy.html": _page(body=same),
    }
    # Текст задаётся экстрактором явно: две страницы с разной разметкой, но одним текстом
    # — ровно тот случай, ради которого дедуп идёт по ``content_hash`` текста, а не байтов.
    extractor = FakeExtractor(
        texts={
            ROOT: same,
            "https://docs.example.test/guide/copy.html": same,
        }
    )
    result = _build(
        layout, pages, limits=ReferenceBuildLimits(max_depth=1), extractor=extractor
    )

    assert result.docs == 1
    assert result.skipped["duplicate_text"] == 1
    # Тот же текст — не потеря материала: пространство прочитано.
    assert result.complete is True


def test_retention_is_content_addressed_bytes(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    body = "Байты для retention."
    pages = {ROOT: _page(body=body)}
    result = _build(layout, pages)

    raw = layout.raw_dir / f"{content_hash(pages[ROOT])}.bin"
    assert raw.is_file()
    assert raw.read_bytes() == pages[ROOT]
    assert raw.parent == layout.raw_dir
    assert result.docs == 1


def test_not_a_page_is_skipped_not_extracted(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    result = _build(
        layout,
        {ROOT: b"\x89PNG\r\n\x1a\n"},
        content_type="image/png",
    )
    assert result.docs == 0
    assert result.skipped["not_a_page"] == 1
    assert result.complete is True


def test_rejected_root_is_named_not_silent(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    result = _build(
        layout,
        {ROOT: _page()},
        roots=("http://127.0.0.1/internal",),
    )
    assert result.docs == 0
    assert result.skipped["root_rejected"] == 1
    assert result.complete is False
    assert any("root_rejected" in error for error in result.errors)


def test_missing_store_factory_is_a_visible_refusal(tmp_path: Path) -> None:
    """Слой pipeline не импортирует core.store: без фабрики — отказ, не сборка «в памяти»."""
    layout = _layout(tmp_path)
    with pytest.raises(ReferenceBuildUnavailable):
        build_reference_corpus(
            layout=layout,
            roots=[ROOT],
            crawler=FakeCrawler({ROOT: _page()}),
            extractor=FakeExtractor(),
            chunker=FakeChunker(),
        )


# ---------------------------------------------------------------------------
# Выбор сборки: явная > current > единственная > отказ
# ---------------------------------------------------------------------------


def _fake_build(layout: ReferenceCorpusLayout, build_id: str) -> None:
    """Кладёт каталог с манифестом (без БД): выбор сборки смотрит на манифест."""
    path = layout.manifest_path(build_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": "reference-corpus/2",
                "build_id": build_id,
                "content_digest": build_id.rsplit("_", 1)[-1],
                "docs": 1,
                "chunks": 1,
            }
        ),
        encoding="utf-8",
    )


def test_resolve_build_prefers_explicit_then_current(tmp_path: Path) -> None:
    layout = _layout(tmp_path).ensure()
    _fake_build(layout, "ref_upstream_18_aaaaaaaa")
    _fake_build(layout, "ref_upstream_18_bbbbbbbb")

    with pytest.raises(ReferenceBuildAmbiguous) as excinfo:
        resolve_build(layout)
    assert excinfo.value.builds == [
        "ref_upstream_18_aaaaaaaa",
        "ref_upstream_18_bbbbbbbb",
    ]

    write_current_build(layout, "ref_upstream_18_bbbbbbbb")
    assert resolve_build(layout) == "ref_upstream_18_bbbbbbbb"
    # Явная сборка побеждает указатель.
    assert resolve_build(layout, explicit="ref_upstream_18_aaaaaaaa") == (
        "ref_upstream_18_aaaaaaaa"
    )


def test_resolve_build_falls_back_to_single_build(tmp_path: Path) -> None:
    layout = _layout(tmp_path).ensure()
    _fake_build(layout, "ref_upstream_18_aaaaaaaa")
    assert resolve_build(layout) == "ref_upstream_18_aaaaaaaa"


def test_resolve_build_refuses_unknown_and_missing(tmp_path: Path) -> None:
    layout = _layout(tmp_path).ensure()
    with pytest.raises(ReferenceBuildMissing):
        resolve_build(layout)

    _fake_build(layout, "ref_upstream_18_aaaaaaaa")
    with pytest.raises(ReferenceBuildMissing) as excinfo:
        resolve_build(layout, explicit="ref_upstream_18_ffffffff")
    # Отказ действенный: перечисляет имеющиеся сборки.
    assert "ref_upstream_18_aaaaaaaa" in str(excinfo.value)


def test_resolve_build_refuses_current_pointing_nowhere(tmp_path: Path) -> None:
    layout = _layout(tmp_path).ensure()
    _fake_build(layout, "ref_upstream_18_aaaaaaaa")
    write_current_build(layout, "ref_upstream_18_deadbeef")
    with pytest.raises(ReferenceBuildMissing):
        resolve_build(layout)


def test_default_depth_follows_links_inside_the_declared_root(tmp_path: Path) -> None:
    """Дефолт сборки — дерево под объявленным корнем, а не одна его страница.

    Пин на дефект: пока ``max_depth`` по умолчанию был ``0`` (наследие ``CrawlLimits``,
    где ноль означает «только seeds оператора»), сборка документации читала **одну**
    страницу и объявляла ``complete=True``. Для reference-корпуса это ложное исчерпание:
    ``recon`` получил бы право публиковать ``absent`` по одной странице руководства
    (I2/I2c). Граница пространства здесь — path-prefix корня (``docs_roots``), а не
    глубина.
    """
    layout = _layout(tmp_path)
    pages = {
        ROOT: _page("/guide/a.html", "/guide/b.html"),
        "https://docs.example.test/guide/a.html": _page(body="A."),
        "https://docs.example.test/guide/b.html": _page(body="B."),
    }
    result = _build(layout, pages)

    assert result.docs == 3
    assert result.complete is True
    assert result.truncated == []
    assert _rows(result.db_path) == (3, 3)


def test_explicit_depth_zero_is_declared_but_marks_incomplete(tmp_path: Path) -> None:
    """``max_depth=0`` — явное «только корни», и цена этого видна, а не скрыта.

    Ссылки извлекаются **всегда** (иначе frontier не увидел бы, что обход упёрся в
    границу), поэтому нулевая глубина даёт ``truncated:max_depth`` и ``complete=False``:
    корпус из одной страницы не аттестует исчерпание документации, и ``absent`` по нему
    не публикуется.
    """
    layout = _layout(tmp_path)
    pages = {
        ROOT: _page("/guide/a.html"),
        "https://docs.example.test/guide/a.html": _page(body="A."),
    }
    result = _build(layout, pages, limits=ReferenceBuildLimits(max_depth=0))

    assert result.docs == 1
    assert result.complete is False
    assert result.truncated == ["max_depth"]
    assert "truncated:max_depth" in result.manifest.incomplete_reasons
    # Счётчик отвечает на свой вопрос: сколько корней упёрлось в границу.
    assert result.skipped["depth"] == 1
