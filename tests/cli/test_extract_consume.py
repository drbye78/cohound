"""CLI-поверхность F6: ``hound extract consume`` (17 §6).

Проверяется вход потребителя, а не второй потребитель:

1. **CLI = фаза.** ``crawl run --crawl-only`` + ``hound extract consume`` дают тот же
   корпус, что полный ``run``: те же ``(url, content_hash)`` документов и тот же
   ``partition_build_id`` (``pb``) партиции. Это и есть смысл входа — «та же функция
   ``consume``», а не параллельная реализация.
2. **Честный исход.** Нет сеанса или очередь пуста → ``EXIT_OK`` с текстом «конвертировать
   нечего»; частичный проход (строки остались незакрытыми) → ``EXIT_ERROR``.
3. **Сеть не трогается.** В режиме конверсии краулер не вызывается ни разу: сетевые
   шаги (дренаж корней, ревалидация, SERP) не исполняются.
4. **Печатаются числа прохода**, включая ``backlog_bytes`` — то, что оператор иначе
   смотрел бы в БД руками.

Порты подменяются in-memory заглушками; данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, main
from core.pipeline import Ports
from core.ports.types import FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from tests.fakes.ports import FakeChunker, FakeExtractor
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

TS = "2026-01-01T00:00:00+00:00"
URL_P1 = "https://docs.test/p1"
URL_P2 = "https://docs.test/p2"


class _RecordingCrawler:
    """Краулер-заглушка: отдаёт заданные байты и запоминает вызовы."""

    version = "recording-crawler-1"

    def __init__(self, pages: dict[str, RawPage] | None = None) -> None:
        self.pages = dict(pages or {})
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(
            url=url,
            content=f"<html>body of {url}</html>".encode(),
            fetched_at=TS,
            fetch_mode=plan.fetch,
            content_type="text/html",
        )


def _urls_file(tmp_path: Path) -> Path:
    path = tmp_path / "urls.json"
    path.write_text(f'{{"p1": ["{URL_P1}"], "p2": ["{URL_P2}"]}}', encoding="utf-8")
    return path


def _crawl_argv(
    package: Path,
    *,
    run_id: str,
    db: Path,
    urls: Path,
    extra: tuple[str, ...] = (),
) -> list[str]:
    return [
        "crawl",
        "run",
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--run-id",
        run_id,
        "--db",
        str(db),
        "--urls",
        str(urls),
        "--json",
        *extra,
    ]


def _consume_argv(
    package: Path,
    *,
    run_id: str,
    db: Path,
    extra: tuple[str, ...] = (),
    json_mode: bool = True,
) -> list[str]:
    argv = [
        "extract",
        "consume",
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--session",
        f"crawl:{run_id}",
        "--db",
        str(db),
    ]
    if json_mode:
        argv.append("--json")
    argv += list(extra)
    return argv


def _ports(crawler: _RecordingCrawler) -> Ports:
    return Ports(crawler=crawler, extractor=FakeExtractor(), chunker=FakeChunker())


def _docs(db: Path) -> list[tuple[str, str]]:
    with SqliteStore(db, read_only=True) as store:
        return sorted(
            (str(row["url"]), str(row["content_hash"]))
            for row in store.conn.execute("SELECT url, content_hash FROM corpus_docs")
        )


def _partitions(db: Path) -> list[tuple[str, str]]:
    with SqliteStore(db, read_only=True) as store:
        return sorted(
            (str(row["product_id"]), str(row["partition_build_id"]))
            for row in store.conn.execute(
                "SELECT product_id, partition_build_id FROM index_manifest"
            )
        )


# --------------------------------------------------------------------------- #
# (1) CLI = фаза: два шага дают тот же корпус, что один
# --------------------------------------------------------------------------- #


def test_cli_consume_matches_the_phase_corpus(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Краул без конверсии + ``extract consume`` = полный прогон по содержимому.

    Сравнивается **содержимое** (``corpus_docs`` и ``partition_build_id``), а не
    физическая раскладка партиции: порядок вставки — свойство расписания потребителя,
    а не измерения (T6 плана ``17``).
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    urls = _urls_file(tmp_path)

    # Путь A: краул без конверсии, затем отдельная команда конверсии.
    split_db = tmp_path / "split.db"
    assert (
        main(
            _crawl_argv(
                package,
                run_id="ex-split",
                db=split_db,
                urls=urls,
                extra=("--crawl-only",),
            ),
            ports=_ports(_RecordingCrawler()),
        )
        == EXIT_OK
    )
    capsys.readouterr()
    crawler = _RecordingCrawler()
    assert main(
        _consume_argv(package, run_id="ex-split", db=split_db), ports=_ports(crawler)
    ) == EXIT_OK
    payload = json_out(capsys)
    assert payload["totals"]["done"] == 2
    assert payload["totals"]["pending"] == 0
    assert payload["backlog_bytes"] == 0  # всё сконвертировано, ждать нечего
    # В режиме конверсии сеть не трогается вовсе.
    assert crawler.calls == []

    # Путь B: полный прогон той же фазы (он же конвертирует).
    full_db = tmp_path / "full.db"
    assert (
        main(
            _crawl_argv(package, run_id="ex-full", db=full_db, urls=urls),
            ports=_ports(_RecordingCrawler()),
        )
        == EXIT_OK
    )
    capsys.readouterr()

    assert _docs(split_db) == _docs(full_db)
    assert _docs(split_db)
    assert [pb for _pid, pb in _partitions(split_db)] == [
        pb for _pid, pb in _partitions(full_db)
    ]


# --------------------------------------------------------------------------- #
# (2) честные исходы: нет сеанса / пустая очередь / частичный проход
# --------------------------------------------------------------------------- #


def test_cli_consume_without_session_is_ok(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Нет сеанса → ``EXIT_OK`` и честный текст: конвертировать нечего."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)

    code = main(
        _consume_argv(package, run_id="ex-none", db=tmp_path / "none.db"),
        ports=_ports(_RecordingCrawler()),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["session_found"] is False
    assert payload["totals"] == {}
    assert payload["backlog_bytes"] == 0


def test_cli_consume_empty_queue_is_ok_and_honest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Сеанс есть, очередь пуста → ``EXIT_OK`` с нулями, а не выдуманный успех."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "empty.db"
    assert (
        main(
            _crawl_argv(
                package,
                run_id="ex-empty",
                db=db,
                urls=_urls_file(tmp_path),
                extra=("--crawl-only",),
            ),
            ports=_ports(_RecordingCrawler()),
        )
        == EXIT_OK
    )
    capsys.readouterr()
    # Ручная очистка очереди: потребителю нечего делать, но сеанс существует.
    with SqliteStore(db) as store, store.conn:
        store.conn.execute("DELETE FROM extract_queue")
    assert main(
        _consume_argv(package, run_id="ex-empty", db=db),
        ports=_ports(_RecordingCrawler()),
    ) == EXIT_OK
    payload = json_out(capsys)
    assert payload["totals"]["done"] == 0
    assert payload["totals"]["pending"] == 0
    assert payload["phase_status"] == "ok"


def test_cli_consume_partial_outcome_is_an_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Незакрытая очередь — не успех: код возврата называет это снаружи.

    Имитируется чужая **живая** аренда (строка ``claimed`` с текущим heartbeat): это
    ровно тот случай, когда потребитель не вправе ни закрыть строку, ни объявить корпус
    полным. ``EXIT_ERROR`` и ``pending > 0`` в выводе — правильный исход.
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "partial.db"
    assert (
        main(
            _crawl_argv(
                package,
                run_id="ex-partial",
                db=db,
                urls=_urls_file(tmp_path),
                extra=("--crawl-only",),
            ),
            ports=_ports(_RecordingCrawler()),
        )
        == EXIT_OK
    )
    capsys.readouterr()
    with SqliteStore(db) as store, store.conn:
        store.conn.execute(
            "UPDATE extract_queue SET state = 'claimed', claimed_by = 'someone-else',"
            " claimed_at = '2999-01-01T00:00:00+00:00',"
            " heartbeat_at = '2999-01-01T00:00:00+00:00'"
        )
    code = main(
        _consume_argv(package, run_id="ex-partial", db=db),
        ports=_ports(_RecordingCrawler()),
    )
    payload = json_out(capsys)
    assert payload["totals"]["pending"] == 2
    assert payload["totals"]["lost"] == 2
    assert code != EXIT_OK


# --------------------------------------------------------------------------- #
# (3) печать: человеку — числа, а не «готово»
# --------------------------------------------------------------------------- #


def test_cli_consume_prints_numbers_for_a_human(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Человекочитаемая форма печатает проход и backlog — то, что иначе искать в БД."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "human.db"
    assert (
        main(
            _crawl_argv(
                package,
                run_id="ex-human",
                db=db,
                urls=_urls_file(tmp_path),
                extra=("--crawl-only",),
            ),
            ports=_ports(_RecordingCrawler()),
        )
        == EXIT_OK
    )
    capsys.readouterr()
    assert main(
        _consume_argv(package, run_id="ex-human", db=db, json_mode=False),
        ports=_ports(_RecordingCrawler()),
    ) == EXIT_OK
    out = capsys.readouterr().out
    assert "converted: done=2" in out
    assert "pending: 0" in out
    assert "backlog: backlog_bytes=" in out
    assert "free_bytes=" in out