"""Файловый staging допущенного артефакта в ``core/adapters/crawl_http.py`` (P6).

Сеть не используется: внедряется мини-дублёр ``httpx`` с потоковым телом. Проверяются:
(1) байт-идентичность страницы ≤ ``max_response_bytes`` при плане, **допускающем**
staging (никакого файла, тот же ``content``/``content_hash``); (2) 30 MiB артефакт не
попадает в RAM целиком — тело пишется в staging-файл, ``content`` пуст, ``head`` — первые
8 KiB, URL-кэш не отравляется пустым телом; (3) переключение на файл при неизвестном
``Content-Length``; (4) потолки: страница свыше ``max_response_bytes`` по-прежнему
``payload_too_large``, артефакт свыше ``max_stream_bytes`` — тоже, staging-файл удалён;
(5) P6b: документ (``pdf``/office) — тоже терминальный артефакт (файл, не RAM), а страница
и ``.rtf`` — нет. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import hashlib
import tracemalloc
from pathlib import Path
from typing import Any, Iterator

import pytest

from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import CrawlPayloadTooLarge
from core.ports.types import FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.document_formats import BINARY_SNIFF_BYTES
from core.util.hashing import content_hash

MIB = 1024 * 1024
PAGE_URL = "https://example.com/page.html"
ZIP_URL = "https://example.com/files/bundle.zip"
DOWNLOAD_URL = "https://example.com/download?id=7"
ZIP_MAGIC = b"PK\x03\x04"


def _pattern(size: int, seed: int = 0) -> bytes:
    """Детерминированное тело заданного размера (без обращения к сети).

    Период 251 (простое) — конкатенация чанков с ``seed`` = смещение даёт ровно
    непрерывную последовательность, поэтому потоковый результат сравним с цельным.
    """
    cycle = bytes(range(251))
    offset = seed % 251
    return (cycle * ((size + offset) // 251 + 1))[offset : offset + size]


class _StreamResponse:
    """Дублёр потокового ответа ``httpx``: тело отдаётся чанками, целиком не хранится."""

    def __init__(
        self,
        *,
        url: str,
        total: int,
        chunk_size: int,
        content_type: str | None,
        content_length: int | None,
        magic: bytes = b"",
    ) -> None:
        self.url = url
        self.status_code = 200
        self.headers: dict[str, str] = {}
        if content_type:
            self.headers["Content-Type"] = content_type
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)
        self._total = total
        self._chunk_size = chunk_size
        self._magic = magic

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self) -> Iterator[bytes]:
        """Поток: первый чанк несёт магию, дальше — детерминированные блоки."""
        sent = 0
        while sent < self._total:
            size = min(self._chunk_size, self._total - sent)
            chunk = _pattern(size, sent)
            if sent == 0 and self._magic:
                chunk = self._magic + chunk[len(self._magic) :]
            sent += size
            yield chunk

    def __enter__(self) -> "_StreamResponse":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


class _FakeHttpx:
    def __init__(self, response: _StreamResponse) -> None:
        self.response = response
        self.calls: list[str] = []

    def stream(self, method: str, url: str, **_kwargs: Any) -> _StreamResponse:
        assert method == "GET"
        self.calls.append(url)
        return self.response


def _crawler(
    monkeypatch: pytest.MonkeyPatch,
    response: _StreamResponse,
    *,
    cache: ContentAddressedCache | None = None,
) -> tuple[HttpCrawler, _FakeHttpx]:
    fake = _FakeHttpx(response)
    monkeypatch.setattr(HttpCrawler, "_import_http", staticmethod(lambda: fake))
    return HttpCrawler(cache=cache), fake


def _staging_plan(tmp_path: Path, *, cap: int, ceiling: int) -> FetchPlan:
    return FetchPlan(
        max_response_bytes=cap,
        staging_dir=str(tmp_path / "stage"),
        max_stream_bytes=ceiling,
        attachment_formats=("zip",),
    )


# ---------------------------------------------------------------------------
# (1) Байт-идентичность страницы при staging-способном плане
# ---------------------------------------------------------------------------


def test_small_page_is_byte_identical_with_staging_capable_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    total = MIB + 32
    plain, _ = _crawler(
        monkeypatch,
        _StreamResponse(
            url=PAGE_URL,
            total=total,
            chunk_size=256 * 1024,
            content_type="text/html; charset=utf-8",
            content_length=total,
        ),
    )
    plain_page = plain.fetch(PAGE_URL, FetchPlan(max_response_bytes=20 * MIB))

    staging, _ = _crawler(
        monkeypatch,
        _StreamResponse(
            url=PAGE_URL,
            total=total,
            chunk_size=256 * 1024,
            content_type="text/html; charset=utf-8",
            content_length=total,
        ),
    )
    staged_page = staging.fetch(
        PAGE_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=256 * MIB)
    )

    assert isinstance(staged_page, RawPage)
    # Страница (не артефакт) при любом плане идёт прежним путём: тело в памяти.
    assert staged_page.content == _pattern(total) == plain_page.content
    assert staged_page.staged_path is None
    assert staged_page.head is None
    assert content_hash(staged_page.content) == content_hash(plain_page.content)
    assert not (tmp_path / "stage").exists() or not any(
        (tmp_path / "stage").iterdir()
    )


# ---------------------------------------------------------------------------
# (2) 30 MiB артефакт: тело не в RAM, файл на диске, кэш не отравлен
# ---------------------------------------------------------------------------


def test_large_artifact_is_streamed_to_staging_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    total = 30 * MIB
    cache = ContentAddressedCache(tmp_path / "cache")
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=ZIP_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/zip",
            content_length=total,
            magic=ZIP_MAGIC,
        ),
        cache=cache,
    )

    tracemalloc.start()
    try:
        page = crawler.fetch(
            ZIP_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=256 * MIB)
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert page.content == b""
    assert page.staged_path is not None
    staged = Path(page.staged_path)
    assert staged.is_file()
    assert staged.stat().st_size == total
    # Голова — ровно первые 8 KiB (сниф без чтения файла), магия контейнера на месте.
    assert page.head is not None
    assert len(page.head) == BINARY_SNIFF_BYTES
    assert page.head.startswith(ZIP_MAGIC)
    assert page.head == ZIP_MAGIC + _pattern(BINARY_SNIFF_BYTES)[len(ZIP_MAGIC) :]
    # Файл содержит ровно те байты, что пришли в потоке (ни потерь, ни дублей).
    expected_hash = content_hash(ZIP_MAGIC + _pattern(total)[len(ZIP_MAGIC) :])
    file_hash = hashlib.sha256()
    with staged.open("rb") as handle:
        for block in iter(lambda: handle.read(MIB), b""):
            file_hash.update(block)
    assert file_hash.hexdigest() == expected_hash
    # Тело целиком в RAM не попадало: пик куда ниже 30 MiB (и ниже in-memory потолка).
    assert peak < 8 * MIB
    # Кэш не отравлен пустым телом: источник истины — retention, не URL-кэш.
    assert cache.get_bytes("http", http_key(ZIP_URL)) is None


# ---------------------------------------------------------------------------
# (3) Неизвестный Content-Length: переключение на файл по факту превышения cap
# ---------------------------------------------------------------------------


def test_unknown_length_switches_to_file_after_memory_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=ZIP_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/zip",
            content_length=None,
            magic=ZIP_MAGIC,
        ),
    )

    tracemalloc.start()
    try:
        page = crawler.fetch(ZIP_URL, _staging_plan(tmp_path, cap=2 * MIB, ceiling=64 * MIB))
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert page.content == b""
    assert page.staged_path is not None
    assert Path(page.staged_path).stat().st_size == total
    # Пик ограничен cap + один чанк, а не всем телом.
    assert peak < 10 * MIB


# ---------------------------------------------------------------------------
# (4) Класс по Content-Type (суффикс-less download) — тоже артефакт
# ---------------------------------------------------------------------------


def test_suffixless_download_is_staged_by_content_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    total = 4 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=DOWNLOAD_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/zip",
            content_length=total,
            magic=ZIP_MAGIC,
        ),
    )

    page = crawler.fetch(
        DOWNLOAD_URL, _staging_plan(tmp_path, cap=2 * MIB, ceiling=64 * MIB)
    )

    assert page.content == b""
    assert page.staged_path is not None
    assert Path(page.staged_path).stat().st_size == total


# ---------------------------------------------------------------------------
# (5) Потолки: страница и артефакт режутся по своим осям, файл удаляется
# ---------------------------------------------------------------------------


def test_page_over_memory_cap_still_rejected_without_staging_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=PAGE_URL,
            total=total,
            chunk_size=MIB,
            content_type="text/html",
            content_length=total,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(PAGE_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=256 * MIB))

    stage = tmp_path / "stage"
    assert not stage.exists() or not any(stage.iterdir())


def test_artifact_over_stream_ceiling_is_rejected_and_file_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Неизвестная длина: файл уже создан, но потолок артефакта пробит — файл снят."""
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=ZIP_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/zip",
            content_length=None,
            magic=ZIP_MAGIC,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(ZIP_URL, _staging_plan(tmp_path, cap=2 * MIB, ceiling=16 * MIB))

    stage = tmp_path / "stage"
    assert not stage.exists() or not any(stage.iterdir())


def test_known_length_over_stream_ceiling_is_rejected_before_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=ZIP_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/zip",
            content_length=total,
            magic=ZIP_MAGIC,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(ZIP_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=16 * MIB))

    stage = tmp_path / "stage"
    assert not stage.exists() or not any(stage.iterdir())


def test_declared_formats_do_not_admit_undeclared_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``.7z`` при заявленном только ``zip`` — страница: свой потолок, без файла."""
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url="https://example.com/files/bundle.7z",
            total=total,
            chunk_size=MIB,
            content_type="application/x-7z-compressed",
            content_length=total,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(
            "https://example.com/files/bundle.7z",
            _staging_plan(tmp_path, cap=20 * MIB, ceiling=256 * MIB),
        )


# ---------------------------------------------------------------------------
# (6) P6b: документ — тоже терминальный артефакт (pdf/office, не только контейнер)
# ---------------------------------------------------------------------------


PDF_URL = "https://example.com/files/manual.pdf"
DOCX_URL = "https://example.com/files/spec.docx"
RTF_URL = "https://example.com/files/memo.rtf"
PDF_MAGIC = b"%PDF-1.7\n"


def test_pdf_over_page_cap_streams_to_staging_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """40 MiB PDF (> страничных 20 MiB, < 250 MiB) идёт в файл, а не в RAM.

    План заявляет **только контейнер** ``zip``: документная половина класса от
    ``attachment_formats`` не зависит (совпадает с реестром экстрактора), поэтому PDF
    допускается и без «контейнерного» разрешения.
    """
    total = 40 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=PDF_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/pdf",
            content_length=total,
            magic=PDF_MAGIC,
        ),
    )

    tracemalloc.start()
    try:
        page = crawler.fetch(PDF_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=250 * MIB))
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert page.content == b""
    assert page.staged_path is not None
    staged = Path(page.staged_path)
    assert staged.is_file() and staged.stat().st_size == total
    assert page.head is not None and page.head.startswith(PDF_MAGIC)
    # Тело целиком в RAM не попадало (пик куда ниже страничного потолка).
    assert peak < 16 * MIB


def test_office_document_streams_by_suffix_without_content_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``.docx`` с неинформативным ``Content-Type`` — класс по суффиксу, файл на диске."""
    total = 25 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=DOCX_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/octet-stream",
            content_length=total,
            magic=ZIP_MAGIC,
        ),
    )

    page = crawler.fetch(DOCX_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=250 * MIB))

    assert page.content == b""
    assert page.staged_path is not None
    assert Path(page.staged_path).stat().st_size == total


def test_html_page_over_cap_is_still_payload_too_large(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Страница не получает артефактный потолок: HTML сверх 20 MiB — прежний отказ."""
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=PAGE_URL,
            total=total,
            chunk_size=MIB,
            content_type="text/html; charset=utf-8",
            content_length=total,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(PAGE_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=250 * MIB))

    stage = tmp_path / "stage"
    assert not stage.exists() or not any(stage.iterdir())


def test_rtf_is_not_admitted_as_terminal_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RTF ждёт ``striprtf``: читателя в реестре нет, допуск дал бы ``parser_failed``."""
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=RTF_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/rtf",
            content_length=total,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(RTF_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=250 * MIB))


def test_pdf_over_artifact_ceiling_is_rejected_and_file_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Потолок артефакта применяется и к документам: 30 MiB > 16 MiB — отказ, файла нет."""
    total = 30 * MIB
    crawler, _fake = _crawler(
        monkeypatch,
        _StreamResponse(
            url=PDF_URL,
            total=total,
            chunk_size=MIB,
            content_type="application/pdf",
            content_length=total,
            magic=PDF_MAGIC,
        ),
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(PDF_URL, _staging_plan(tmp_path, cap=20 * MIB, ceiling=16 * MIB))

    stage = tmp_path / "stage"
    assert not stage.exists() or not any(stage.iterdir())
