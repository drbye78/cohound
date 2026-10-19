"""Тесты межпрогонного кэша экстракции (``core/pipeline/extract_cache.py``).

Проверяют: round-trip ``ExtractedDoc``; промахи по неизвестному ключу и по несовпадению
``extractor_version``/``level`` (именно промах, не ошибка); отказ кэшировать неуспешные
извлечения; битый/несовместимый конверт → промах со счётчиком; атомарность (``.tmp`` не
обслуживается); вытеснение по бюджету (детерминированно, oldest-first); детерминизм двух
экземпляров над одним корнем; защиту от path traversal.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from core.domain import DocumentLevel
from core.pipeline.extract_cache import ENVELOPE_FORMAT, ExtractionCache
from core.ports.types import ExtractedDoc
from core.util.hashing import content_hash

RAW_A = content_hash("retained-bytes-a")
RAW_B = content_hash("retained-bytes-b")
RAW_C = content_hash("retained-bytes-c")


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _doc(
    text: str,
    *,
    version: str = "ext-1",
    pages: int | None = 1,
    error: str | None = None,
) -> ExtractedDoc:
    """Успешный (по умолчанию) ``ExtractedDoc``: ``content_hash`` — хеш **текста**."""
    return ExtractedDoc(
        text=text,
        content_hash=content_hash(text),
        extractor_version=version,
        page_count=pages,
        error=error,
    )


def _entry(root: Path, raw_hash: str) -> Path:
    """Ожидаемый путь записи (``{root}/{hash[:2]}/{hash}.json``)."""
    return root / raw_hash[:2] / f"{raw_hash}.json"


def _total_bytes(root: Path) -> int:
    """Суммарный размер записей ``*.json`` под корнем."""
    return sum(path.stat().st_size for path in root.rglob("*.json"))


def _write_envelope(path: Path, **overrides: object) -> None:
    """Пишет валидный по форме конверт с переопределениями (для негативных тестов)."""
    envelope: dict[str, object] = {
        "format": ENVELOPE_FORMAT,
        "content_hash": RAW_A,
        "extractor_version": "ext-1",
        "level": "full",
        "text": "hello",
        "page_count": 1,
        "doc_content_hash": content_hash("hello"),
    }
    envelope.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope), encoding="utf-8")


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_round_trip_returns_equal_document(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")
    doc = _doc("извлечённый текст", pages=7)

    cache.put(doc, level="full", content_hash=RAW_A)
    restored = cache.get(RAW_A, "ext-1", "full")

    assert restored == doc
    assert restored is not None and restored.error is None


def test_round_trip_accepts_document_level_enum(tmp_path: Path) -> None:
    """``level`` может быть ``DocumentLevel`` (StrEnum) — ключ нормализуется в строку."""
    cache = ExtractionCache(tmp_path / "cache")
    doc = _doc("pdf text")

    cache.put(doc, level=DocumentLevel.FULL, content_hash=RAW_A)

    assert cache.get(RAW_A, "ext-1", DocumentLevel.FULL) == doc
    assert cache.get(RAW_A, "ext-1", "full") == doc


def test_round_trip_keeps_text_hash_separate_from_key(tmp_path: Path) -> None:
    """Ключ (хеш сырья) и ``doc_content_hash`` (хеш текста) — разные величины."""
    cache = ExtractionCache(tmp_path / "cache")
    doc = _doc("same text")
    cache.put(doc, level="full", content_hash=RAW_A)

    envelope = json.loads(_entry(tmp_path / "cache", RAW_A).read_text(encoding="utf-8"))
    assert envelope["content_hash"] == RAW_A
    assert envelope["doc_content_hash"] == content_hash("same text")
    assert envelope["content_hash"] != envelope["doc_content_hash"]


def test_put_without_explicit_key_uses_doc_hash(tmp_path: Path) -> None:
    """Совместимость сигнатуры: без ключа запись адресуется ``doc.content_hash``."""
    cache = ExtractionCache(tmp_path / "cache")
    doc = _doc("fallback")

    cache.put(doc, level="full")

    assert cache.get(doc.content_hash, "ext-1", "full") == doc


def test_get_is_repeatable(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")
    doc = _doc("repeatable")
    cache.put(doc, level="full", content_hash=RAW_A)

    assert cache.get(RAW_A, "ext-1", "full") == cache.get(RAW_A, "ext-1", "full")
    assert cache.stats()["hits"] == 2


# ---------------------------------------------------------------------------
# Промахи (не ошибки)
# ---------------------------------------------------------------------------


def test_miss_on_unknown_hash(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")
    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["misses"] == 1
    assert cache.stats()["corrupt"] == 0


def test_miss_on_extractor_version_mismatch(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")
    cache.put(_doc("v1"), level="full", content_hash=RAW_A)

    assert cache.get(RAW_A, "ext-2", "full") is None  # промах, не исключение
    assert cache.get(RAW_A, "ext-1", "full") is not None


def test_miss_on_level_mismatch(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")
    cache.put(_doc("full level"), level="full", content_hash=RAW_A)

    assert cache.get(RAW_A, "ext-1", "layout_tables") is None
    assert cache.get(RAW_A, "ext-1", "full") is not None


def test_stale_envelope_with_other_key_is_miss(tmp_path: Path) -> None:
    """Конверт лежит по пути RAW_A, но объявляет RAW_B → не обслуживается."""
    root = tmp_path / "cache"
    _write_envelope(_entry(root, RAW_A), content_hash=RAW_B)

    cache = ExtractionCache(root)
    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["misses"] == 1
    assert cache.stats()["corrupt"] == 0


# ---------------------------------------------------------------------------
# Кэшируются только успешные извлечения
# ---------------------------------------------------------------------------


def test_error_document_not_stored(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")

    cache.put(_doc("partial", error="parser crashed"), level="full", content_hash=RAW_A)

    assert not _entry(tmp_path / "cache", RAW_A).exists()
    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["writes"] == 0


def test_empty_and_whitespace_text_not_stored(tmp_path: Path) -> None:
    cache = ExtractionCache(tmp_path / "cache")

    cache.put(_doc(""), level="full", content_hash=RAW_A)
    cache.put(_doc("   \n\t"), level="full", content_hash=RAW_B)

    assert not _entry(tmp_path / "cache", RAW_A).exists()
    assert not _entry(tmp_path / "cache", RAW_B).exists()
    assert cache.stats()["writes"] == 0


# ---------------------------------------------------------------------------
# Битые конверты
# ---------------------------------------------------------------------------


def test_corrupt_json_is_miss_and_counted(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    path = _entry(root, RAW_A)
    path.parent.mkdir(parents=True)
    path.write_text("{not valid json", encoding="utf-8")

    cache = ExtractionCache(root)

    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["corrupt"] == 1
    assert cache.stats()["misses"] == 1


def test_non_mapping_json_is_corrupt(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    path = _entry(root, RAW_A)
    path.parent.mkdir(parents=True)
    path.write_text("[]", encoding="utf-8")

    cache = ExtractionCache(root)

    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["corrupt"] == 1


def test_undecodable_bytes_are_corrupt(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    path = _entry(root, RAW_A)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"\xff\xfe\x00broken")

    cache = ExtractionCache(root)

    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["corrupt"] == 1


def test_unknown_envelope_format_is_miss(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    _write_envelope(_entry(root, RAW_A), format=ENVELOPE_FORMAT + 1)

    cache = ExtractionCache(root)

    assert cache.get(RAW_A, "ext-1", "full") is None
    assert cache.stats()["corrupt"] == 1


# ---------------------------------------------------------------------------
# Атомарность
# ---------------------------------------------------------------------------


def test_tmp_leftover_is_not_served(tmp_path: Path) -> None:
    """Оставшийся ``.tmp`` не является записью: он не читается и не мешает ``put``."""
    root = tmp_path / "cache"
    shard = root / RAW_A[:2]
    shard.mkdir(parents=True)
    leftover = shard / f".{RAW_A}.dead.tmp"
    leftover.write_text(
        json.dumps(
            {
                "format": ENVELOPE_FORMAT,
                "content_hash": RAW_A,
                "extractor_version": "ext-1",
                "level": "full",
                "text": "half written",
                "page_count": 1,
                "doc_content_hash": content_hash("half written"),
            }
        ),
        encoding="utf-8",
    )

    cache = ExtractionCache(root)
    assert cache.get(RAW_A, "ext-1", "full") is None  # .tmp не обслуживается

    cache.put(_doc("complete"), level="full", content_hash=RAW_A)
    assert cache.get(RAW_A, "ext-1", "full") == _doc("complete")
    assert leftover.exists()  # чужой temp не трогаем
    assert len(list(root.rglob("*.json"))) == 1


def test_put_leaves_no_tmp_behind(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    cache = ExtractionCache(root)
    cache.put(_doc("atomic"), level="full", content_hash=RAW_A)

    assert list(root.rglob("*.tmp")) == []
    assert len(list(root.rglob("*.json"))) == 1


def test_envelope_bytes_are_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    cache = ExtractionCache(root)
    doc = _doc("same")
    cache.put(doc, level="full", content_hash=RAW_A)
    first = _entry(root, RAW_A).read_bytes()
    cache.put(doc, level="full", content_hash=RAW_A)

    assert _entry(root, RAW_A).read_bytes() == first


# ---------------------------------------------------------------------------
# Бюджет и вытеснение
# ---------------------------------------------------------------------------


def test_no_eviction_without_budget(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    cache = ExtractionCache(root)
    for raw in (RAW_A, RAW_B, RAW_C):
        cache.put(_doc(raw), level="full", content_hash=raw)

    assert len(list(root.rglob("*.json"))) == 3
    assert cache.stats()["evicted"] == 0


def test_eviction_is_oldest_first_and_stays_under_budget(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    probe = ExtractionCache(root)
    probe.put(_doc("aaaa"), level="full", content_hash=RAW_A)
    size = _entry(root, RAW_A).stat().st_size

    cache = ExtractionCache(root, max_bytes=2 * size)
    cache.put(_doc("bbbb"), level="full", content_hash=RAW_B)
    # Явные mtime: A старее B, C (новый put) — самый свежий.
    os.utime(_entry(root, RAW_A), ns=(1_000_000_000, 1_000_000_000))
    os.utime(_entry(root, RAW_B), ns=(2_000_000_000, 2_000_000_000))

    cache.put(_doc("cccc"), level="full", content_hash=RAW_C)

    assert not _entry(root, RAW_A).exists()  # вытеснен самый старый
    assert _entry(root, RAW_B).exists()
    assert _entry(root, RAW_C).exists()
    assert _total_bytes(root) <= 2 * size
    assert cache.stats()["evicted"] == 1


def test_entry_larger_than_budget_is_evicted(tmp_path: Path) -> None:
    """Запись, которая одна больше бюджета, тоже вытесняется: кэш остаётся в бюджете."""
    root = tmp_path / "cache"
    cache = ExtractionCache(root, max_bytes=10)
    cache.put(_doc("too big for the budget"), level="full", content_hash=RAW_A)

    assert list(root.rglob("*.json")) == []
    assert _total_bytes(root) <= 10
    assert cache.stats()["evicted"] == 1


def test_negative_budget_rejected(tmp_path: Path) -> None:
    try:
        ExtractionCache(tmp_path / "cache", max_bytes=-1)
    except ValueError:
        pass
    else:  # pragma: no cover — защита от регрессии контракта
        raise AssertionError("ожидался ValueError для отрицательного max_bytes")


# ---------------------------------------------------------------------------
# Детерминизм и изоляция путей
# ---------------------------------------------------------------------------


def test_two_instances_over_same_root_see_same_entries(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    writer = ExtractionCache(root)
    reader = ExtractionCache(root)
    doc = _doc("shared")
    writer.put(doc, level="full", content_hash=RAW_A)

    assert reader.get(RAW_A, "ext-1", "full") == doc

    other = _doc("written by reader")
    reader.put(other, level="full", content_hash=RAW_B)
    assert writer.get(RAW_B, "ext-1", "full") == other


def test_path_traversal_keys_rejected(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    cache = ExtractionCache(root)
    doc = _doc("payload")

    for bad in ("../../escape", "ab/cd", "..", "", "zz", "ab\\cd"):
        assert cache.get(bad, "ext-1", "full") is None
        cache.put(doc, level="full", content_hash=bad)

    # Ничего не записано ни в корень, ни за его пределы.
    assert list(root.rglob("*")) == []
    assert not (tmp_path / "escape.json").exists()
    assert cache.stats()["invalid_key"] == 12  # 6 get + 6 put


def test_stats_snapshot_has_stable_keys(tmp_path: Path) -> None:
    stats = ExtractionCache(tmp_path / "cache").stats()
    assert set(stats) == {
        "hits",
        "misses",
        "writes",
        "corrupt",
        "evicted",
        "invalid_key",
    }
    assert all(isinstance(value, int) for value in stats.values())
