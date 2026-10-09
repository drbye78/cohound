"""Тесты ``OpenWebSearchAdapter`` (локальная утилита ``open-websearch``, subprocess).

Сеть и процесс не задействуются: запуск внедряется через шов ``runner`` (argv →
returncode/stdout/stderr), ``fetch`` — через ``http_get``. Покрываются: разбор JSON-
конверта ``data.results`` (``description``→snippet, rank — позиция), обработка
``status != "ok"``/``error``/ненулевого кода/невалидного JSON, argv one-shot команды
(``search … --limit … --engine … --json``), ``fetch`` с SSRF-валидацией и кэшем (R7),
``available`` без бинаря и без шва, структурное соответствие порту, I19-инвариант,
категорийная нейтральность (§1.3).

Фикстуры **категорийно-нейтральны**: example.com / «Example», без продуктов и вендоров.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

import core.adapters.search_openwebsearch as module
from core.adapters.search_openwebsearch import (
    DEFAULT_BINARY,
    DEFAULT_ENGINE,
    OpenWebSearchAdapter,
    SearchAdapterUnavailable,
    SearchError,
)
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key

URL_A = "https://example.com/a"
URL_B = "https://example.com/b"
URL_C = "https://example.com/c"

#: Заведомо отсутствующий бинарь: ``available`` без шва обязан быть ``False``.
MISSING_BINARY = "definitely-not-a-real-binary-xyz"

_RECORDS = [
    {"title": "Example A", "url": URL_A, "description": "first snippet",
     "source": "example.com", "engine": "duckduckgo"},
    {"title": "Example B", "url": URL_B, "description": "second snippet",
     "source": "example.com", "engine": "duckduckgo"},
    {"title": "Example C", "url": URL_C, "description": "third snippet",
     "source": "example.com", "engine": "duckduckgo"},
]


def _envelope(
    records: list[dict[str, Any]],
    *,
    status: str = "ok",
    error: Any = None,
    partial_failures: list[Any] | None = None,
) -> dict[str, Any]:
    """JSON-конверт утилиты (форма проверена на живом вызове)."""
    return {
        "status": status,
        "data": {
            "query": "neutral query",
            "engines": ["duckduckgo"],
            "totalResults": len(records),
            "results": records,
            "partialFailures": partial_failures or [],
        },
        "error": error,
        "hint": None,
    }


def _runner_returning(
    envelope: Any, *, rc: int = 0, stderr: str = ""
) -> Any:
    """Создаёт ``runner``-шов, отдающий заданный конверт и фиксирующий argv."""
    payload = envelope if isinstance(envelope, str) else json.dumps(envelope)

    def _run(argv: list[str]) -> tuple[int, str, str]:
        _run.calls.append(argv)  # type: ignore[attr-defined]
        return rc, payload, stderr

    _run.calls = []  # type: ignore[attr-defined]
    return _run


def _adapter(runner: Any, **kwargs: Any) -> OpenWebSearchAdapter:
    """Адаптер с внедрённым ``runner`` (реальный процесс не запускается)."""
    kwargs.setdefault("binary", MISSING_BINARY)
    return OpenWebSearchAdapter(runner=runner, **kwargs)


# ---------------------------------------------------------------------------
# Контракт порта / атрибуты
# ---------------------------------------------------------------------------


def test_class_attributes() -> None:
    adapter = _adapter(_runner_returning(_envelope([])))
    assert adapter.adapter_kind == "serp"
    assert adapter.provider == "open-websearch"


def test_implements_search_adapter_port() -> None:
    adapter = _adapter(_runner_returning(_envelope([])))
    assert isinstance(adapter, SearchAdapterPort)
    assert callable(adapter.search)
    assert callable(adapter.fetch)


def test_defaults() -> None:
    adapter = OpenWebSearchAdapter()
    assert adapter.binary == DEFAULT_BINARY
    assert adapter.engine == DEFAULT_ENGINE
    assert DEFAULT_ENGINE == "duckduckgo"  # google утилитой не поддерживается


def test_construct_without_dependencies_does_not_crash() -> None:
    adapter = OpenWebSearchAdapter()
    assert adapter.provider == "open-websearch"


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(module.ENV_BINARY, "/opt/ows")
    monkeypatch.setenv(module.ENV_ENGINE, "bing")
    adapter = OpenWebSearchAdapter()
    assert adapter.binary == "/opt/ows"
    assert adapter.engine == "bing"


def test_invalid_timeout_rejected() -> None:
    with pytest.raises(ValueError):
        OpenWebSearchAdapter(timeout=0)


# ---------------------------------------------------------------------------
# search: разбор и argv
# ---------------------------------------------------------------------------


def test_search_parses_candidates() -> None:
    adapter = _adapter(_runner_returning(_envelope(_RECORDS)))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert [r.title for r in results] == ["Example A", "Example B", "Example C"]
    assert [r.snippet for r in results] == ["first snippet", "second snippet", "third snippet"]
    assert [r.rank for r in results] == [1, 2, 3]
    assert all(isinstance(r, SearchResult) for r in results)


def test_search_returns_candidates_without_page_bytes() -> None:
    # I19: поиск отдаёт кандидатов; байты страницы берёт fetch, не search.
    adapter = _adapter(_runner_returning(_envelope(_RECORDS)))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert all(r.raw_bytes is None for r in results)


def test_search_argv_is_one_shot_json_command() -> None:
    runner = _runner_returning(_envelope(_RECORDS))
    _adapter(runner).search(SearchQuery(text="neutral query", limit=4))
    argv = runner.calls[0]
    assert argv[1] == "search"
    assert argv[2] == "neutral query"
    assert argv[argv.index("--limit") + 1] == "4"
    assert argv[argv.index("--engine") + 1] == "duckduckgo"
    assert argv[-1] == "--json"
    assert "serve" not in argv  # только one-shot, без демона


def test_search_engine_override() -> None:
    runner = _runner_returning(_envelope(_RECORDS))
    _adapter(runner, engine="bing").search(SearchQuery(text="neutral query"))
    argv = runner.calls[0]
    assert argv[argv.index("--engine") + 1] == "bing"


def test_search_respects_limit() -> None:
    adapter = _adapter(_runner_returning(_envelope(_RECORDS * 4)))
    assert len(adapter.search(SearchQuery(text="neutral query", limit=2))) == 2


def test_search_records_without_url_skipped() -> None:
    records = [{"title": "no url"}, _RECORDS[0]]
    adapter = _adapter(_runner_returning(_envelope(records)))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A]


def test_search_limit_zero_returns_empty_without_process() -> None:
    def _boom(argv: list[str]) -> tuple[int, str, str]:
        raise AssertionError("процесс не должен запускаться при limit<=0")

    adapter = _adapter(_boom)
    assert adapter.search(SearchQuery(text="neutral query", limit=0)) == []


def test_search_empty_query_rejected() -> None:
    adapter = _adapter(_runner_returning(_envelope([])))
    with pytest.raises(ValueError):
        adapter.search(SearchQuery(text="   "))


def test_partial_failures_are_not_an_error() -> None:
    envelope = _envelope(_RECORDS[:1], partial_failures=[{"engine": "bing", "error": "x"}])
    adapter = _adapter(_runner_returning(envelope))
    assert len(adapter.search(SearchQuery(text="neutral query"))) == 1


# ---------------------------------------------------------------------------
# Ошибки
# ---------------------------------------------------------------------------


def test_nonzero_returncode_raises_search_error() -> None:
    adapter = _adapter(_runner_returning(_envelope([]), rc=2, stderr="boom"))
    with pytest.raises(SearchError, match="кодом 2"):
        adapter.search(SearchQuery(text="neutral query"))


def test_error_status_raises_search_error() -> None:
    adapter = _adapter(_runner_returning(_envelope([], status="error", error="unsupported_engine")))
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_error_field_with_ok_status_raises_search_error() -> None:
    envelope = _envelope([], status="ok", error={"message": "bad"})
    adapter = _adapter(_runner_returning(envelope))
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_invalid_json_raises_search_error() -> None:
    adapter = _adapter(_runner_returning("not-json"))
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_non_object_json_raises_search_error() -> None:
    adapter = _adapter(_runner_returning("[1, 2, 3]"))
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_runner_failure_raises_search_error() -> None:
    def _boom(argv: list[str]) -> tuple[int, str, str]:
        raise RuntimeError("spawn failed")

    adapter = _adapter(_boom)
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_timeout_raises_search_error() -> None:
    def _timeout(argv: list[str]) -> tuple[int, str, str]:
        raise subprocess.TimeoutExpired(argv, 1)

    adapter = _adapter(_timeout)
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_search_error_is_runtime_error() -> None:
    assert issubclass(SearchError, RuntimeError)
    assert issubclass(SearchAdapterUnavailable, RuntimeError)


# ---------------------------------------------------------------------------
# fetch: SSRF, RawPage, кэш (R7)
# ---------------------------------------------------------------------------


def test_fetch_returns_rawpage_from_seam() -> None:
    adapter = OpenWebSearchAdapter(http_get=lambda url: b"<html>page</html>")
    page = adapter.fetch(URL_A)
    assert isinstance(page, RawPage)
    assert page.url == URL_A
    assert page.content == b"<html>page</html>"
    assert page.render_mode is RenderMode.HTTP
    assert page.fetched_at


def test_fetch_blocks_ssrf_loopback_without_http() -> None:
    calls: list[str] = []

    def _get(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        calls.append(url)
        return b"should not happen"

    adapter = OpenWebSearchAdapter(http_get=_get)
    with pytest.raises(ValueError):
        adapter.fetch("http://127.0.0.1/private")
    assert calls == []


def test_fetch_cache_hit_avoids_http_get(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    cache.put_bytes("http", http_key(URL_A), b"cached page")

    def _boom(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("http_get не должен вызываться при попадании в кэш")

    adapter = OpenWebSearchAdapter(http_get=_boom, cache=cache)
    assert adapter.fetch(URL_A).content == b"cached page"


# ---------------------------------------------------------------------------
# available / отсутствие готовности
# ---------------------------------------------------------------------------


def test_available_with_runner() -> None:
    adapter = OpenWebSearchAdapter(runner=_runner_returning(_envelope([])))
    assert adapter.available is True


def test_available_false_without_binary_or_runner() -> None:
    adapter = OpenWebSearchAdapter(binary=MISSING_BINARY)
    assert adapter._binary_path() is None
    assert adapter.available is False


def test_search_without_readiness_raises_unavailable() -> None:
    adapter = OpenWebSearchAdapter(binary=MISSING_BINARY)
    with pytest.raises(SearchAdapterUnavailable):
        adapter.search(SearchQuery(text="neutral query"))


def test_fetch_without_httpx_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    adapter = OpenWebSearchAdapter(runner=_runner_returning(_envelope([])))
    with pytest.raises(SearchAdapterUnavailable):
        adapter.fetch(URL_A)


# ---------------------------------------------------------------------------
# I19 структурно: модуль не ссылается на доказательства/ячейки/хранилище
# ---------------------------------------------------------------------------

_TYPE_REFERENCE_BANNED = ("CellEvidence", "Cell", "evidence", "core.domain", "core.store")


def test_i19_module_source_has_no_evidence_references() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    lowered = source.lower()
    for token in _TYPE_REFERENCE_BANNED:
        assert token.lower() not in lowered, f"I19: модуль ссылается на {token!r}"


def test_i19_module_imports_no_domain_or_store() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    assert all(not mod.startswith(("core.domain", "core.store")) for mod in imported)


# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------


def test_module_source_is_category_neutral() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    hits = [line for line in source.splitlines() if forbidden.search(line)]
    assert hits == []
