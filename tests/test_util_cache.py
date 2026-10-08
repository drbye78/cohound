"""Тесты ``core/util/cache.py`` (W7/T7.6, ADR 0016).

Покрывают round-trip байтов и JSON, промах, идемпотентность ``get_or_fetch``,
атомарность записи (без ``.tmp``), детерминизм и нормализацию ключей, а также
защиту от path traversal.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.util.cache import ContentAddressedCache, http_key, serp_key


# --- round-trip ---------------------------------------------------------------


def test_bytes_round_trip(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    data = b"\x00\x01binary payload\xff"
    path = cache.put_bytes("http", "abc123", data)
    assert path == tmp_path / "http" / "abc123.bin"
    assert path.read_bytes() == data
    assert cache.get_bytes("http", "abc123") == data


def test_json_round_trip(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    obj = {"query": "python", "results": [1, 2, 3], "nested": {"b": 2, "a": 1}}
    path = cache.put_json("serp", "deadbeef", obj)
    assert path == tmp_path / "serp" / "deadbeef.json"
    assert cache.get_json("serp", "deadbeef") == obj


def test_miss_returns_none(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    assert cache.get_bytes("http", "missing") is None
    assert cache.get_json("serp", "missing") is None


def test_dirs_created_on_demand(tmp_path: Path) -> None:
    root = tmp_path / "does" / "not" / "exist"
    cache = ContentAddressedCache(root)
    assert not root.exists()
    cache.put_bytes("http", "k", b"x")
    assert (root / "http" / "k.bin").is_file()


# --- get_or_fetch -------------------------------------------------------------


def test_get_or_fetch_fetches_once_then_serves_cache(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    calls = {"n": 0}

    def fetch() -> bytes:
        calls["n"] += 1
        return b"fetched-body"

    assert cache.get_or_fetch("http", "key1", fetch) == b"fetched-body"
    assert calls["n"] == 1
    assert cache.get_or_fetch("http", "key1", fetch) == b"fetched-body"
    assert calls["n"] == 1  # второй вызов — из кэша, fetch не дёргается
    assert (tmp_path / "http" / "key1.bin").read_bytes() == b"fetched-body"


def test_get_or_fetch_rejects_non_bytes(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    with pytest.raises(TypeError):
        cache.get_or_fetch("http", "k", lambda: "not-bytes")  # type: ignore[arg-type]


def test_put_bytes_rejects_non_bytes(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    with pytest.raises(TypeError):
        cache.put_bytes("http", "k", "str")  # type: ignore[arg-type]


# --- атомарность --------------------------------------------------------------


def _tmp_files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.name.endswith(".tmp")]


def test_no_tmp_left_after_success(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", "k", b"data")
    cache.put_json("serp", "k", {"a": 1})
    assert _tmp_files(tmp_path) == []


def test_no_tmp_left_and_target_untouched_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", "k", b"original")

    import core.util.cache as cache_mod

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(cache_mod.os, "replace", boom)
    with pytest.raises(OSError):
        cache.put_bytes("http", "k", b"new")
    assert _tmp_files(tmp_path) == []
    # Существующая запись не повреждена.
    assert cache.get_bytes("http", "k") == b"original"


def test_atomic_replace_is_used(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    import core.util.cache as cache_mod

    cache = ContentAddressedCache(tmp_path)
    seen: list[tuple[str, str]] = []
    real_replace = os.replace

    def spy(src: str, dst: str) -> None:
        seen.append((str(src), str(dst)))
        real_replace(src, dst)

    monkeypatch.setattr(cache_mod.os, "replace", spy)
    cache.put_bytes("http", "k", b"data")
    assert len(seen) == 1
    assert seen[0][0].endswith(".tmp")
    assert seen[0][1] == str(tmp_path / "http" / "k.bin")


# --- ключи --------------------------------------------------------------------


def test_http_key_deterministic(tmp_path: Path) -> None:
    url = "https://example.com/page?q=1"
    assert http_key(url) == http_key(url)
    assert http_key(url) == ContentAddressedCache.http_key(url)
    assert ContentAddressedCache().http_key(url) == http_key(url)
    assert http_key(f"  {url}  ") == http_key(url)  # strip не влияет
    assert len(http_key(url)) == 32


def test_http_key_distinguishes_urls() -> None:
    assert http_key("https://a.example/") != http_key("https://b.example/")
    # регистр пути значим (не приводим URL к lower)
    assert http_key("https://a.example/Path") != http_key("https://a.example/path")


def test_serp_key_normalization_and_determinism() -> None:
    assert serp_key("Python FastAPI") == serp_key("python   fastapi")
    assert serp_key("  Python\tFastAPI\n") == serp_key("python fastapi")
    assert len(serp_key("q")) == 32


def test_serp_key_provider_participates() -> None:
    assert serp_key("q") == serp_key("q", None)
    assert serp_key("q", None) == serp_key("q", "")
    assert serp_key("q", "Google") == serp_key("q", "google")
    assert serp_key("q", "google") != serp_key("q", "bing")


def test_serp_key_empty_query_rejected() -> None:
    with pytest.raises(ValueError):
        serp_key("   ")


def test_keys_are_distinct_between_http_and_serp() -> None:
    # одинаковый материал в разных пространствах ключей не коллизирует
    assert http_key("https://example.com/") != serp_key("https://example.com/")


# --- безопасность пути --------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    ["../evil", "../../etc/passwd", "a/b", "a\\b", "\x00evil", ".hidden", "..", ""],
)
def test_path_for_rejects_hostile_keys(tmp_path: Path, hostile: str) -> None:
    cache = ContentAddressedCache(tmp_path)
    with pytest.raises(ValueError):
        cache.path_for("http", hostile, ".bin")


def test_path_for_stays_within_root(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    resolved_root = tmp_path.resolve()
    target = cache.path_for("http", "abcdef0123456789", ".bin")
    assert target.is_relative_to(resolved_root)
    assert target == resolved_root / "http" / "abcdef0123456789.bin"


def test_path_for_rejects_unknown_kind_and_bad_suffix(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    with pytest.raises(ValueError):
        cache.path_for("evil", "k", ".bin")
    with pytest.raises(ValueError):
        cache.path_for("http", "k", "bin")
    with pytest.raises(ValueError):
        cache.path_for("http", "k", "/../x")


# --- конфигурация -------------------------------------------------------------


def test_default_root_is_cache() -> None:
    assert ContentAddressedCache().root == Path("cache")


def test_custom_subdirs(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path / "c", subdirs=("custom",))
    cache.put_bytes("custom", "k", b"x")
    assert (tmp_path / "c" / "custom" / "k.bin").read_bytes() == b"x"
    with pytest.raises(ValueError):
        cache.put_bytes("http", "k", b"x")


@pytest.mark.parametrize("bad", [(), ("a/b",), ("..",), ("",)])
def test_init_rejects_bad_subdirs(tmp_path: Path, bad: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        ContentAddressedCache(tmp_path, subdirs=bad)
