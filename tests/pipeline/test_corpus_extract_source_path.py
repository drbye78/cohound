"""``_run_extract``: retained-файл читается по пути, а не целиком в RAM воркера.

Замеренный дефект: воркер делал ``Path(raw_path).read_bytes()`` — весь документ (PDF на
сотни МБ, член архива на 125 МБ) поднимался в RSS, затем адаптер писал его во **второй**
временный файл и docling читал обратно. Здесь закреплено:

1. ``_run_extract`` **не** вызывает ``Path.read_bytes`` (запрет через monkeypatch): тело
   документа не копируется в RAM воркера;
2. ``RawPage`` несёт ``source_path`` и пустой ``content`` — контракт порта аддитивен
   (``RawPage`` без ``source_path`` ведёт себя как прежде);
3. ключ ``extract_cache`` остаётся хешем retained-байтов и совпадает с родительской
   пробой ``_extract_cache_key`` (``raw_path.stem``) — I19 не ослаблен потоковым чтением;
4. отсутствие retained-файла — видимая ошибка (``FileNotFoundError``), а не тихий пустой
   текст: пул переводит её в ``parser_failed`` как и раньше.

Данные категорийно-нейтральны (§1.3); адаптеры не импортируются.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.domain.enums import DocumentLevel
from core.pipeline.corpus import (
    _ExtractRequest,
    _extract_cache_key,
    _run_extract,
)
from core.pipeline.extract_cache import ExtractionCache
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchMode, RawPage
from core.util.hashing import content_hash

#: Размер «крупного» документа: заметно больше буфера потокового хеша (1 MiB), чтобы
#: ``read_bytes`` (если бы он остался) гарантированно выделил сопоставимый буфер.
LARGE_SIZE = 8 * 1024 * 1024

class _PageRecordingExtractor:
    """Фейк-экстрактор: запоминает ``RawPage`` и отвечает по размеру файла (без чтения).

    ``Path.read_bytes`` в фейке не используется намеренно: тест запрещает его глобально,
    а адаптер в реальности читает файл сам (docling по пути).
    """

    version = "fake-extractor-1"

    def __init__(self) -> None:
        self.pages: list[RawPage] = []
        self.routes: list[ExtractionRoute] = []

    @property
    def calls(self) -> int:
        return len(self.pages)

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.pages.append(page)
        self.routes.append(route)
        size = Path(page.source_path).stat().st_size if page.source_path else len(page.content)
        text = f"size={size}"
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
            page_count=1,
        )

def _request(raw_path: Path, **overrides: object) -> _ExtractRequest:
    fields: dict[str, object] = {
        "raw_path": str(raw_path),
        "url": "https://docs.example.test/file.pdf",
        "fetched_at": "2026-10-22T00:00:00+00:00",
        "fetch_mode": FetchMode.HTTP,
        "lang": "en",
        "status_code": 200,
        "content_type": "application/pdf",
        "engine": "http",
        "route": ExtractionRoute.DOCUMENT,
        "level": DocumentLevel.LAYOUT_TABLES,
    }
    fields.update(overrides)
    return _ExtractRequest(**fields)  # type: ignore[arg-type]

def _retained(tmp_path: Path, data: bytes) -> Path:
    """Раскладка retention: ``{sha256(bytes)}.bin`` (I19)."""
    path = tmp_path / f"{content_hash(data)}.bin"
    path.write_bytes(data)
    return path

# --------------------------------------------------------------------------- #
# 1. Тело документа не поднимается в RAM
# --------------------------------------------------------------------------- #

def test_run_extract_does_not_read_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = b"%PDF-1.7\n" + b"\0" * LARGE_SIZE
    path = _retained(tmp_path, data)

    def _forbidden(self: Path) -> bytes:
        raise AssertionError(
            "_run_extract не должен читать retained-файл целиком: тело идёт по source_path"
        )

    monkeypatch.setattr(Path, "read_bytes", _forbidden)
    extractor = _PageRecordingExtractor()

    doc = _run_extract(extractor, _request(path))  # type: ignore[arg-type]

    assert doc.error is None
    assert doc.text == f"size={len(data)}"
    assert extractor.calls == 1
    page = extractor.pages[0]
    # Контракт порта: путь задан, байты не передаются.
    assert page.source_path == str(path)
    assert page.content == b""
    # Метаданные задания доехали без изменений (семантика извлечения не меняется).
    assert page.url == "https://docs.example.test/file.pdf"
    assert page.content_type == "application/pdf"
    assert page.status_code == 200
    assert page.lang == "en"

def test_run_extract_passes_route_through(tmp_path: Path) -> None:
    data = b"%PDF-1.7 route"
    path = _retained(tmp_path, data)
    extractor = _PageRecordingExtractor()

    _run_extract(extractor, _request(path, route=ExtractionRoute.AUTO))  # type: ignore[arg-type]

    assert extractor.routes == [ExtractionRoute.AUTO]

# --------------------------------------------------------------------------- #
# 2. Ключ кэша экстракции не изменился
# --------------------------------------------------------------------------- #

def test_run_extract_cache_key_is_hash_of_retained_bytes(tmp_path: Path) -> None:
    """Потоковый хеш даёт ровно тот же ключ, что и ``content_hash(байты)``."""
    data = b"%PDF-1.7 cache-key" + b"\0" * 4096
    path = _retained(tmp_path, data)
    cache_root = tmp_path / "extract_cache"
    extractor = _PageRecordingExtractor()
    request = _request(path, cache_root=str(cache_root))  # type: ignore[arg-type]

    first = _run_extract(extractor, request)
    second = _run_extract(extractor, request)

    # Второй вызов — хит кэша: экстрактор не звался повторно.
    assert extractor.calls == 1
    assert second == first
    # Ключ родителя (``raw_path.stem``) == хеш retained-байтов == ключ воркера.
    assert _extract_cache_key(path) == content_hash(data)
    cached = ExtractionCache(cache_root).get(
        content_hash(data), extractor.version, DocumentLevel.LAYOUT_TABLES.value
    )
    assert cached == first

def test_run_extract_cache_key_survives_empty_file(tmp_path: Path) -> None:
    """Пустой retained-файл: ключ — хеш пустоты, а не хеш незаполненного ``content``."""
    path = _retained(tmp_path, b"")
    cache_root = tmp_path / "extract_cache"
    extractor = _PageRecordingExtractor()

    _run_extract(extractor, _request(path, cache_root=str(cache_root)))  # type: ignore[arg-type]

    assert _extract_cache_key(path) == content_hash(b"")
    assert ExtractionCache(cache_root).get(
        content_hash(b""), extractor.version, DocumentLevel.LAYOUT_TABLES.value
    ) is not None

# --------------------------------------------------------------------------- #
# 3. Пропавший retained-файл — видимая ошибка
# --------------------------------------------------------------------------- #

def test_run_extract_missing_file_raises(tmp_path: Path) -> None:
    """Прежнее поведение сохранено: исключение (пул → ``parser_failed``), не пустой текст."""
    extractor = _PageRecordingExtractor()

    with pytest.raises(FileNotFoundError):
        _run_extract(extractor, _request(tmp_path / "нет-такого.bin"))  # type: ignore[arg-type]

    assert extractor.calls == 0

# --------------------------------------------------------------------------- #
# 4. Аддитивность поля: RawPage без source_path — прежний контракт
# --------------------------------------------------------------------------- #

def test_raw_page_without_source_path_is_unchanged() -> None:
    page = RawPage(url="https://docs.example.test/a", content=b"<html>x</html>", fetched_at="")

    assert page.source_path is None
    assert page.content == b"<html>x</html>"
