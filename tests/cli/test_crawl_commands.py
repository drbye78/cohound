"""Тесты CLI-поверхности ``hound crawl`` (L3b): run/status/list/resume/restart/cancel.

Команда — тонкая обёртка над durable-сеансом краула (Lane B/L1a): она не реализует
логику краула сама, а резолвит прогон/порты/фазы (как ``run``) и вызывает фазу корпуса Ш5
(:func:`core.pipeline.corpus.build_corpus`). Порты подменяются in-memory заглушками —
сеть не используется. Фикстуры категорийно-нейтральны (§1.3): ``p1/p2``, ``case_x``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.commands import COMMANDS
from core.cli.main import (
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    main,
)
from core.pipeline import Ports
from core.ports.types import FetchPlan, RawPage
from tests.fakes.ports import FakeChunker, FakeExtractor
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

TS = "2026-01-01T00:00:00+00:00"
URL_P1 = "https://docs.test/p1"
URL_P2 = "https://docs.test/p2"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


class _RecordingCrawler:
    """Краулер-заглушка, запоминающий URL'ы фетча (проверка idempotency/refetch)."""

    version = "recording-crawler-1"

    def __init__(
        self, pages: dict[str, RawPage] | None = None, *, raising: set[str] | None = None
    ) -> None:
        self.pages = dict(pages or {})
        self.raising = set(raising or ())
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        """Помечает URL и возвращает заданную страницу (или пустые байты)."""
        self.calls.append(url)
        if url in self.raising:
            raise RuntimeError(f"crawl failed: {url}")
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(
            url=url, content=b"", fetched_at=TS, fetch_mode=plan.fetch
        )


def _pages() -> dict[str, RawPage]:
    return {
        URL_P1: RawPage(url=URL_P1, content=b"one", fetched_at=TS),
        URL_P2: RawPage(url=URL_P2, content=b"two", fetched_at=TS),
    }


def _urls_file(tmp_path: Path) -> Path:
    path = tmp_path / "urls.json"
    path.write_text(
        f'{{"p1": ["{URL_P1}"], "p2": ["{URL_P2}"]}}', encoding="utf-8"
    )
    return path


def _ports(crawler: _RecordingCrawler) -> Ports:
    return Ports(crawler=crawler, extractor=FakeExtractor(), chunker=FakeChunker())


def _run_argv(
    package: Path,
    *,
    action: str,
    run_id: str,
    db: Path,
    urls: Path | None = None,
    extra: tuple[str, ...] = (),
) -> list[str]:
    """argv подкоманды ``crawl <action>`` для ``run``/``resume``/``restart``."""
    argv = [
        "crawl",
        action,
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--run-id",
        run_id,
        "--db",
        str(db),
        "--json",
    ]
    if urls is not None:
        argv += ["--urls", str(urls)]
    argv += list(extra)
    return argv


# ---------------------------------------------------------------------------
# Регистрация команды
# ---------------------------------------------------------------------------


def test_crawl_registered_in_commands() -> None:
    assert any(module.__name__ == "core.cli.commands.crawl" for module in COMMANDS)


def test_crawl_no_subcommand_is_usage_error() -> None:
    assert main(["crawl"]) == EXIT_USAGE


# ---------------------------------------------------------------------------
# status / list на пустой БД
# ---------------------------------------------------------------------------


def test_crawl_status_empty_session_returns_null(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет сеанса в существующей БД → ``EXIT_OK`` c ``status: null`` (валидный ответ)."""
    from core.store.sqlite_store import SqliteStore

    db = tmp_path / "empty.db"
    with SqliteStore(db):
        pass  # схема создана писателем; сеанса нет
    code = main(
        ["crawl", "status", "--run-id", "r1", "--db", str(db), "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["status"] is None
    assert payload["session_id"] == "crawl:r1"
    assert payload["counts"] == {}


def test_crawl_status_missing_db_is_not_found(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Отсутствующая БД — ``EXIT_NOT_FOUND``: читатель её не создаёт (§4.7 разбора CLI)."""
    db = tmp_path / "ghost" / "empty.db"

    code = main(["crawl", "status", "--run-id", "r1", "--db", str(db)])

    assert code == EXIT_NOT_FOUND
    assert not db.exists()
    assert "not_found" in capsys.readouterr().err


def test_crawl_list_empty_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Существующая БД без сеансов → пустой список (``EXIT_OK``)."""
    from core.store.sqlite_store import SqliteStore

    db = tmp_path / "empty.db"
    with SqliteStore(db):
        pass
    code = main(["crawl", "list", "--run-id", "r1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["count"] == 0
    assert payload["sessions"] == []


# ---------------------------------------------------------------------------
# run → сеанс с элементами; status → счётчики
# ---------------------------------------------------------------------------


def test_crawl_run_creates_session_with_items(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr1", db=tmp_path / "cr1.db",
            urls=_urls_file(tmp_path),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["session_id"] == "crawl:cr1"
    assert payload["crawl"]["fetched"] == 2
    assert sorted(crawler.calls) == [URL_P1, URL_P2]


def test_crawl_status_reports_counts_after_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(["crawl", "status", "--run-id", "cr1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "completed"
    assert payload["counts"]["fetched"] == 2
    assert payload["total"] == 2


def test_crawl_status_per_url_lists_items(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(
        ["crawl", "status", "--run-id", "cr1", "--db", str(db), "--per-url", "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    items = payload["items"]
    assert {row["product_id"] for row in items} == {"p1", "p2"}
    assert all(row["status"] == "fetched" for row in items)


# ---------------------------------------------------------------------------
# resume (идемпотентность) / restart (очистка)
# ---------------------------------------------------------------------------


def test_crawl_resume_is_idempotent_no_refetch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    argv = _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path))
    assert main(argv, ports=ports) == EXIT_OK
    assert sorted(crawler.calls) == [URL_P1, URL_P2]
    capsys.readouterr()

    crawler.calls.clear()
    resume_argv = _run_argv(
        package, action="resume", run_id="cr1", db=db, urls=_urls_file(tmp_path)
    )
    assert main(resume_argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["mode"] == "resume"
    assert payload["crawl"]["fetched"] == 2
    # Resume не пере-фетчит уже FETCHED-элементы (recovery + retained raw).
    assert crawler.calls == []


def test_crawl_restart_clears_and_refetches(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    crawler.calls.clear()
    restart_argv = _run_argv(
        package,
        action="restart",
        run_id="cr1",
        db=db,
        urls=_urls_file(tmp_path),
        extra=("--from-scratch",),
    )
    assert main(restart_argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["mode"] == "restart"
    assert payload["cleared"] is True
    # Очистка сеанса → повторный фетч; --from-scratch задокументирован как Phase 4.
    assert sorted(crawler.calls) == [URL_P1, URL_P2]
    assert any("from-scratch" in note for note in payload["notes"])


def test_crawl_run_reset_clears_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    crawler.calls.clear()
    reset_argv = _run_argv(
        package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path),
        extra=("--reset",),
    )
    assert main(reset_argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["cleared"] is True
    assert sorted(crawler.calls) == [URL_P1, URL_P2]


# ---------------------------------------------------------------------------
# cancel / list / --until
# ---------------------------------------------------------------------------


def test_crawl_cancel_sets_cancelled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(["crawl", "cancel", "--run-id", "cr1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "cancelled"


def test_crawl_cancel_missing_session_not_found(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "empty.db"
    code = main(["crawl", "cancel", "--run-id", "r1", "--db", str(db), "--json"])
    assert code == EXIT_NOT_FOUND
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["status"] is None


def test_crawl_list_reports_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    assert main(
        _run_argv(package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path)),
        ports=ports,
    ) == EXIT_OK
    capsys.readouterr()

    code = main(["crawl", "list", "--run-id", "cr1", "--db", str(db), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["count"] == 1
    assert payload["sessions"][0]["session_id"] == "crawl:cr1"


def test_crawl_run_until_before_corpus_skips(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--until`` раньше corpus → краул не запускается (статус ``skipped``, ``EXIT_OK``)."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr1.db"
    crawler = _RecordingCrawler(_pages())
    ports = _ports(crawler)
    argv = _run_argv(
        package, action="run", run_id="cr1", db=db, urls=_urls_file(tmp_path),
        extra=("--until", "g1"),
    )
    assert main(argv, ports=ports) == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "skipped"
    assert crawler.calls == []
    assert payload["crawl"] == {}


# ---------------------------------------------------------------------------
# Phase 4: фильтры / потолок попыток / follow / код возврата
# ---------------------------------------------------------------------------


def test_crawl_only_product_filters_url_set(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--only-product p1`` фетчит только URL продукта ``p1``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr-op", db=tmp_path / "cr-op.db",
            urls=_urls_file(tmp_path), extra=("--only-product", "p1"),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["session_status"] == "completed"
    assert crawler.calls == [URL_P1]


def test_crawl_only_failed_refetches_failed_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--only-failed`` ретраит только ``FAILED``-URL, не трогая ``FETCHED``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr-of.db"
    urls = _urls_file(tmp_path)
    failing = _RecordingCrawler(_pages(), raising={URL_P2})
    assert main(
        _run_argv(package, action="run", run_id="cr-of", db=db, urls=urls),
        ports=_ports(failing),
    ) == EXIT_ERROR  # p2 упал → сеанс PARTIAL
    capsys.readouterr()

    retry = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="resume", run_id="cr-of", db=db, urls=urls,
            extra=("--only-failed",),
        ),
        ports=_ports(retry),
    )
    assert code == EXIT_OK
    assert retry.calls == [URL_P2]  # только упавший URL
    payload = json_out(capsys)
    assert payload["session_status"] == "completed"


def test_crawl_only_failed_without_session_is_graceful(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--only-failed`` без сеанса не применяется (graceful) — обычный прогон + note."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr-nf", db=tmp_path / "cr-nf.db",
            urls=_urls_file(tmp_path), extra=("--only-failed",),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert sorted(crawler.calls) == [URL_P1, URL_P2]
    assert any("only-failed" in note for note in payload["notes"])


def test_crawl_max_attempts_flag_skips_exhausted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--max-attempts 1`` после сбоя: URL не пере-фетчится, терминальный ``failed``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cr-ma.db"
    urls = _urls_file(tmp_path)
    failing = _RecordingCrawler(_pages(), raising={URL_P2})
    first = main(
        _run_argv(package, action="run", run_id="cr-ma", db=db, urls=urls),
        ports=_ports(failing),
    )
    assert first == EXIT_ERROR
    capsys.readouterr()

    retry = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="resume", run_id="cr-ma", db=db, urls=urls,
            extra=("--max-attempts", "1"),
        ),
        ports=_ports(retry),
    )
    assert code == EXIT_ERROR
    assert retry.calls == []


def test_crawl_follow_emits_ndjson_progress(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--follow --json`` печатает NDJSON-снимки прогресса и финальный payload."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr-fo", db=tmp_path / "cr-fo.db",
            urls=_urls_file(tmp_path), extra=("--follow", "--interval", "0"),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out.strip().splitlines()
    records = [json.loads(line) for line in out]
    progress = [r for r in records if r.get("event") == "crawl_progress"]
    assert progress, "ожидались NDJSON-снимки crawl_progress"
    assert records[-1]["session_id"] == "crawl:cr-fo"


def test_crawl_follow_emits_product_boundaries_live(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--follow --json`` печатает живые границы продукта (``crawl_product``).

    Без них агрегат сеанса не отличает «продукт ещё в очереди» от «продукт потерян».
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr-fp", db=tmp_path / "cr-fp.db",
            urls=_urls_file(tmp_path), extra=("--follow", "--interval", "0"),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out.strip().splitlines()
    records = [json.loads(line) for line in out]
    product_events = [r for r in records if r.get("event") == "crawl_product"]
    assert [(r["phase"], r["product_id"]) for r in product_events] == [
        ("start", "p1"),
        ("end", "p1"),
        ("start", "p2"),
        ("end", "p2"),
    ]
    assert all(r["session_id"] == "crawl:cr-fp" for r in product_events)


def test_crawl_without_follow_emits_no_product_boundaries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``--follow`` stdout прежний: только финальный payload, без ``crawl_product``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr-nf2", db=tmp_path / "cr-nf2.db",
            urls=_urls_file(tmp_path),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "crawl_product" not in out
    assert "crawl_progress" not in out
    # stdout остаётся одной JSON-документом финального payload.
    assert json.loads(out.strip())["session_id"] == "crawl:cr-nf2"


def test_crawl_partial_session_returns_error_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Частичный краул (упавший URL) → ``EXIT_ERROR`` (не маскируется ``ok``)."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages(), raising={URL_P2})
    code = main(
        _run_argv(
            package, action="run", run_id="cr-part", db=tmp_path / "cr-part.db",
            urls=_urls_file(tmp_path),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["session_status"] == "partial"


def test_crawl_truncation_only_exits_ok_with_warning(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """N4: PARTIAL только из-за политического усечения → ``EXIT_OK`` + предупреждение,
    усечение видно в payload."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages())
    code = main(
        _run_argv(
            package, action="run", run_id="cr-tr", db=tmp_path / "cr-tr.db",
            urls=_urls_file(tmp_path), extra=("--max-documents", "1"),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["session_status"] == "partial"
    assert payload["crawl_truncation"]["truncated"] is True
    assert payload["crawl_truncation"]["truncated_reason"] == "max_documents"
    assert any("усечён политикой" in note for note in payload["notes"])
    # Причина в тексте — та, что записана: усечение бывает не только max_documents/max_depth
    # (F6/F8 добавили extract_backlog_full и disk_low_headroom), и список политик в сообщении
    # не отвечает на вопрос «какой лимит достигнут».
    assert any("max_documents" in note for note in payload["notes"])


def test_crawl_policy_truncation_with_failures_keeps_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """N4: реальный сбой фетча сохраняет ``EXIT_ERROR`` даже при наличии усечения."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _RecordingCrawler(_pages(), raising={URL_P2})
    code = main(
        _run_argv(
            package, action="run", run_id="cr-trf", db=tmp_path / "cr-trf.db",
            urls=_urls_file(tmp_path), extra=("--max-documents", "1"),
        ),
        ports=_ports(crawler),
    )
    assert code == EXIT_ERROR


# ---------------------------------------------------------------------------
# F1/Д-6: interrupted, свидетельство живости, видимость в list/status
# ---------------------------------------------------------------------------


def _db_with_session(
    tmp_path: Path,
    *,
    status: Any,
    owner: str | None = None,
    heartbeat_at: str | None = None,
    session_id: str = "crawl:r1",
) -> Path:
    """БД с единственным сеансом заданного статуса (без прогона: ``run_id=None``)."""
    from core.domain import CrawlSession
    from core.store.sqlite_store import SqliteStore

    db = tmp_path / "manual.db"
    with SqliteStore(db) as store:
        store.upsert_crawl_session(
            CrawlSession(
                session_id=session_id,
                run_id=None,
                profile="local",
                status=status,
                created_at=TS,
                owner=owner,
                heartbeat_at=heartbeat_at,
            )
        )
    return db


def test_crawl_cancel_running_session_is_interrupted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """F1: снятие ``running``-сеанса — ``interrupted`` (тело краула не финализировано)."""
    from core.domain import CrawlSessionStatus

    db = _db_with_session(
        tmp_path,
        status=CrawlSessionStatus.RUNNING,
        owner="other-host/4242/777",
        heartbeat_at="2026-01-01T00:00:00+00:00",
    )

    code = main(["crawl", "cancel", "--run-id", "r1", "--db", str(db), "--json"])

    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "interrupted"
    # Причина названа машинно-читаемо: «оператор снял» отличимо от «процесс убит».
    assert payload["error"].startswith("cancelled_by_operator")
    assert any("interrupted" in note for note in payload["notes"])


def test_crawl_cancel_not_running_session_stays_cancelled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Сеанс, который не исполняется, снимается как ``cancelled``: тела в полёте нет."""
    from core.domain import CrawlSessionStatus

    db = _db_with_session(tmp_path, status=CrawlSessionStatus.CREATED)

    assert main(["crawl", "cancel", "--run-id", "r1", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "cancelled"
    assert payload["error"].startswith("cancelled_by_operator")


def test_crawl_status_shows_liveness_of_interrupted_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``crawl status`` показывает ``interrupted``, причину и свидетельство живости."""
    from core.domain import CrawlSessionStatus

    db = _db_with_session(
        tmp_path,
        status=CrawlSessionStatus.INTERRUPTED,
        owner="other-host/4242/777",
        heartbeat_at="2026-01-01T00:00:00+00:00",
    )

    assert main(["crawl", "status", "--run-id", "r1", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] == "interrupted"
    assert payload["owner"] == "other-host/4242/777"
    assert payload["heartbeat_at"] == "2026-01-01T00:00:00+00:00"
    # Терминальный сеанс не «протухает»: recovery ему уже не нужен.
    assert payload["stale_reason"] is None


def test_crawl_list_names_stale_running_without_writing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``crawl list`` называет протухший ``running``, но **не** лечит его (read-only)."""
    from core.domain import CrawlSessionStatus
    from core.store.sqlite_store import SqliteStore

    db = _db_with_session(
        tmp_path,
        status=CrawlSessionStatus.RUNNING,
        owner="other-host/4242/777",
        heartbeat_at="2020-01-01T00:00:00+00:00",
    )

    assert main(["crawl", "list", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    row = payload["sessions"][0]
    assert row["status"] == "running"
    assert row["stale_reason"].startswith("stale_heartbeat")

    # Read-only: статус в БД не изменился (лечит open_session/run, а не list).
    with SqliteStore(db) as store:
        session = store.get_crawl_session("crawl:r1")
        assert session is not None
        assert session.status is CrawlSessionStatus.RUNNING


def test_crawl_list_filters_by_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``crawl list --run-id`` фильтрует, а не молча ничего не делает (§4.7 разбора CLI).

    Аргумент, который принят и проигнорирован, хуже отсутствующего: оператор уверен, что
    отфильтровал. Здесь два сеанса разных прогонов в одной БД — видно и фильтр, и его
    отсутствие.
    """
    from core.domain import CrawlSession, CrawlSessionStatus
    from core.domain.enums import RankingPolicy, RunMode
    from core.domain.models import Focus, RunConfig
    from core.store.sqlite_store import SqliteStore

    db = tmp_path / "sessions.db"
    cfg = RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="cohort_case_x",
        products=["p1"],
        package="case_x",
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )
    with SqliteStore(db) as store:
        for run_id in ("r1", "r2"):
            store.create_run(run_id, cfg)
            store.upsert_crawl_session(
                CrawlSession(
                    session_id=f"crawl:{run_id}",
                    run_id=run_id,
                    profile="local",
                    status=CrawlSessionStatus.CREATED,
                    created_at=TS,
                )
            )

    assert main(["crawl", "list", "--run-id", "r1", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["run_id"] == "r1"
    assert [row["session_id"] for row in payload["sessions"]] == ["crawl:r1"]

    # Без фильтра видны оба: фильтр действительно фильтрует, а не подменяет выборку.
    assert main(["crawl", "list", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["run_id"] is None
    assert [row["session_id"] for row in payload["sessions"]] == [
        "crawl:r1",
        "crawl:r2",
    ]


def test_interrupted_is_terminal_for_follow() -> None:
    """F1: ``interrupted`` обязан быть терминальным — иначе ``--follow`` ждал бы вечно."""
    from core.cli.context import _is_terminal_session_status

    assert _is_terminal_session_status("interrupted") is True


def test_crawl_run_touches_heartbeat_during_crawl(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1/Д-6: признак жизни пишется **по ходу** сеанса, а не только при открытии.

    Проверяется проводка: ``Heartbeat.touch`` вызывается из воронок фазы (запись исхода URL,
    границы шагов дренажа). Сам факт записи по интервалу — предмет
    ``tests/pipeline/test_liveness.py`` (троттлинг), здесь важна именно связанность: без неё
    heartbeat остался бы точкой старта, и медленный сеанс выглядел бы мёртвым.
    """
    from core.pipeline import liveness

    calls: list[bool] = []
    original = liveness.Heartbeat.touch

    def spy(self: Any, *, force: bool = False) -> bool:
        result = original(self, force=force)
        calls.append(result)
        return result

    monkeypatch.setattr(liveness.Heartbeat, "touch", spy)
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    code = main(
        _run_argv(
            package, action="run", run_id="cr-hb", db=tmp_path / "cr-hb.db",
            urls=_urls_file(tmp_path),
        ),
        ports=_ports(_RecordingCrawler(_pages())),
    )
    assert code == EXIT_OK
    capsys.readouterr()
    assert calls, "heartbeat не вызывался по ходу краула — сеанс выглядел бы мёртвым"
