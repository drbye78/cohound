"""``hound artifacts``: чтение отказов краула и ручное скачивание (13-manual-artifacts).

Проверяется то, что видит оператор, и то, что остаётся на диске:

* регистрация группы и подкоманд (``--help`` — часть интерфейса);
* ``list`` читает **БД прогона** (файл ``artifacts_gap.yaml`` вторичен), фильтры
  ``--product``/``--root``/``--mask``/``--path`` и форма ``--json``; пустой список — ответ
  (``EXIT_OK``), а не ошибка;
* ``fetch --dry-run`` ничего не пишет и называет ``planned``;
* ``fetch`` с подменённым краулером кладёт байты в ``sources/manual/`` и пишет
  декларацию, которую читает :func:`core.pipeline.manual_artifacts.load_manual_artifacts`;
* ``--actor`` обязателен (код 2), отсутствующая БД — ``EXIT_NOT_FOUND`` (4).

Сеть не используется: краулер подменяется заглушкой. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, main
from core.domain import (
    CrawlItemStatus,
    CrawlSessionItem,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.crawl_session import default_session_id, open_session
from core.pipeline.manual_artifacts import load_manual_artifacts
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.1"
PACKAGE_ID = "case_x"
PRODUCT = "p1"
ROOT = "https://docs.test/tdb/"
PDF_URL = "https://docs.test/tdb/guide.pdf"
HTML_URL = "https://docs.test/tdb/page.html"
PDF_BODY = b"%PDF-1.4\n% test artifact\n"


def _cfg() -> RunConfig:
    """Минимальный конфиг прогона (категорийно нейтральный)."""
    return RunConfig(
        case_id=PACKAGE_ID,
        anchor=PRODUCT,
        cohort=f"cohort_{PACKAGE_ID}",
        products=[PRODUCT],
        package=PACKAGE_ID,
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 11, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _item(
    url: str,
    *,
    run_id: str = RUN_ID,
    product_id: str = PRODUCT,
    error_kind: str = "robots_disallow",
    http_status: int | None = 403,
    depth: int | None = 1,
) -> CrawlSessionItem:
    """Элемент сеанса: терминальный ``failed`` с классом отказа."""
    return CrawlSessionItem(
        session_id=default_session_id(run_id),
        product_id=product_id,
        url=url,
        status=CrawlItemStatus.FAILED,
        attempts=1,
        depth=depth,
        http_status=http_status,
        error_kind=error_kind,
        last_attempt="2026-11-01T00:00:00+00:00",
    )


def _seed_run(
    runs_dir: Path,
    run_id: str = RUN_ID,
    *,
    items: list[CrawlSessionItem] | None = None,
    created_at: str | None = None,
) -> Path:
    """Создаёт БД прогона с сеансом и элементами; возвращает путь к ``cohound.db``."""
    db = runs_dir / run_id / "cohound.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteStore(db)
    try:
        store.create_run(run_id, _cfg())
        if created_at is not None:
            store.conn.execute(
                "UPDATE runs SET created_at = ? WHERE run_id = ?", (created_at, run_id)
            )
            store.conn.commit()
        open_session(store, session_id=default_session_id(run_id), run_id=run_id)
        for item in items or []:
            store.upsert_crawl_session_item(item)
        store.conn.commit()
    finally:
        store.close()
    return db


def _package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Каталог пакета под ``tmp_path/packages`` + изолированные корни раскладки."""
    packages = tmp_path / "packages"
    monkeypatch.setenv("COHOUND_PACKAGES_DIR", str(packages))
    monkeypatch.setenv("COHOUND_RUNS_DIR", str(tmp_path / "runs"))
    package_dir = packages / PACKAGE_ID
    (package_dir / "sources").mkdir(parents=True)
    (package_dir / "sources" / "corpus_urls.yaml").write_text(
        f"{PRODUCT}:\n- {ROOT}\n", encoding="utf-8"
    )
    (package_dir / "cohort.yaml").write_text(
        f"case_id: {PACKAGE_ID}\n"
        f"frozen_at: '2026-11-01'\n"
        "entries:\n"
        f"- anchor_relation: anchor\n  product_id: {PRODUCT}\n"
        "  role_overlap: 1.0\n  selection_reason: anchor\n",
        encoding="utf-8",
    )
    return package_dir


class FakeCrawler:
    """Краулер-заглушка: отдаёт заранее объявленные байты, сети не касается."""

    version = "fake-1"
    engine = "http"
    supports_browser = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str, plan: Any = None) -> Any:
        from core.ports.types import RawPage

        self.calls.append(url)
        return RawPage(
            url=url,
            content=PDF_BODY,
            fetched_at="2026-11-14T00:00:00+00:00",
            status_code=200,
            content_type="application/pdf",
        )


# ---------------------------------------------------------------------------
# Регистрация
# ---------------------------------------------------------------------------


def test_group_and_subcommands_are_registered(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``artifacts --help`` и ``--help`` обеих подкоманд отвечают (часть интерфейса)."""
    assert main(["artifacts", "--help"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "list" in out and "fetch" in out

    assert main(["artifacts", "list", "--help"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "--mask" in out and "--root" in out
    assert "--url" not in out

    assert main(["artifacts", "fetch", "--help"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "--actor" in out and "--url" in out and "--dry-run" in out


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_filters_and_json_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Фильтры отбирают записи, ``--json`` несёт все поля + производные ``host``/``path``."""
    runs_dir = tmp_path / "runs"
    _seed_run(
        runs_dir,
        items=[
            _item(PDF_URL),
            _item(HTML_URL, error_kind="not_found", http_status=404),
        ],
    )
    db = runs_dir / RUN_ID / "cohound.db"

    assert main(["artifacts", "list", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["run_id"] == RUN_ID
    assert payload["count"] == 1, "неблокирующий not_found не входит в список"
    item = payload["items"][0]
    assert item["product_id"] == PRODUCT
    assert item["url"] == PDF_URL
    assert item["kind"] == "robots_disallow"
    assert item["host"] == "docs.test"
    assert item["path"] == "/tdb/guide.pdf"
    assert payload["summary"]["count"] == 1
    assert payload["summary"]["excluded_by_kind"] == {"not_found": 1}

    assert (
        main(["artifacts", "list", "--db", str(db), "--mask", "*.pdf", "--json"]) == EXIT_OK
    )
    assert json_out(capsys)["count"] == 1

    assert (
        main(["artifacts", "list", "--db", str(db), "--mask", "*.html", "--json"])
        == EXIT_OK
    )
    assert json_out(capsys)["count"] == 0

    assert (
        main(["artifacts", "list", "--db", str(db), "--path", "/tdb", "--json"]) == EXIT_OK
    )
    assert json_out(capsys)["count"] == 1

    # Граница сегмента: ``/tdb/guide`` — не префикс ``/tdb/guide.pdf`` (это разные
    # сегменты), поэтому фильтр по «почти пути» ничего не накрывает.
    assert (
        main(["artifacts", "list", "--db", str(db), "--path", "/tdb/guide", "--json"])
        == EXIT_OK
    )
    assert json_out(capsys)["count"] == 0

    assert (
        main(["artifacts", "list", "--db", str(db), "--root", ROOT, "--json"]) == EXIT_OK
    )
    assert json_out(capsys)["count"] == 1

    assert (
        main(["artifacts", "list", "--db", str(db), "--product", "other", "--json"])
        == EXIT_OK
    )
    assert json_out(capsys)["count"] == 0


def test_list_text_names_gap_file_and_fetch_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Текст называет число, исключённые классы, файл-копию и команду скачивания."""
    runs_dir = tmp_path / "runs"
    _seed_run(
        runs_dir,
        items=[
            _item(PDF_URL),
            _item(HTML_URL, error_kind="not_found", http_status=404),
        ],
    )
    db = runs_dir / RUN_ID / "cohound.db"

    assert main(["artifacts", "list", "--db", str(db)]) == EXIT_OK
    out = capsys.readouterr().out
    assert any(
        line.split()[:2] == [PRODUCT, "robots_disallow"] and PDF_URL in line
        for line in out.splitlines()
    ), out
    assert "всего: 1; по продуктам:" in out
    assert "не входят (не блокирующие): not_found=1" in out
    assert "artifacts_gap.yaml" in out
    assert f"hound artifacts fetch --run-id {RUN_ID}" in out


def test_list_empty_is_an_answer_not_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Пустой список — ``EXIT_OK``: «нечего скачивать» это ответ, а не отказ команды."""
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, items=[])
    db = runs_dir / RUN_ID / "cohound.db"

    assert main(["artifacts", "list", "--db", str(db), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["count"] == 0
    assert payload["items"] == []


def test_list_uses_last_run_of_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--package`` без ``--run-id`` берёт последний прогон пакета по ``created_at``."""
    runs_dir = tmp_path / "runs"
    _seed_run(
        runs_dir,
        "run.old",
        items=[_item(PDF_URL, run_id="run.old")],
        created_at="2026-11-01T00:00:00",
    )
    _seed_run(runs_dir, "run.new", items=[], created_at="2026-11-02T00:00:00")
    package_dir = _package(tmp_path, monkeypatch)

    assert main(["artifacts", "list", "--package", str(package_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["run_id"] == "run.new"
    assert payload["package"] == PACKAGE_ID


def test_list_package_without_runs_is_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет прогонов пакета — ошибка входа с числом просмотренных, а не выдуманный путь."""
    (tmp_path / "runs").mkdir()
    package_dir = _package(tmp_path, monkeypatch)

    code = main(["artifacts", "list", "--package", str(package_dir)])
    assert code == EXIT_USAGE
    assert "просмотрено прогонов: 0" in capsys.readouterr().err


def test_list_missing_run_db_is_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--run-id`` без БД — ``EXIT_NOT_FOUND``; файл не создаётся (читатель)."""
    runs_dir = tmp_path / "runs"
    monkeypatch.setenv("COHOUND_RUNS_DIR", str(runs_dir))

    code = main(["artifacts", "list", "--run-id", "ghost", "--json"])
    assert code == EXIT_NOT_FOUND
    assert not (runs_dir / "ghost" / "cohound.db").exists()
    assert json_out(capsys)["error"] == "not_found"


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


def test_fetch_dry_run_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--dry-run``: ``planned``, ни файла, ни декларации."""
    package_dir = _package(tmp_path, monkeypatch)
    monkeypatch.setattr("core.cli.commands.artifacts.HttpCrawler", FakeCrawler)

    code = main(
        [
            "artifacts",
            "fetch",
            "--package",
            str(package_dir),
            "--url",
            PDF_URL,
            "--product",
            PRODUCT,
            "--actor",
            "tester",
            "--dry-run",
        ]
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "planned" in out
    assert "dry-run: ничего не записано" in out
    assert not (package_dir / "sources" / "manual_artifacts.yaml").exists()
    assert not (package_dir / "sources" / "manual").exists()


def test_fetch_writes_bytes_and_declaration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Скачанные байты лежат в пакете, декларация читается загрузчиком и несёт sha256."""
    package_dir = _package(tmp_path, monkeypatch)
    monkeypatch.setattr("core.cli.commands.artifacts.HttpCrawler", FakeCrawler)

    code = main(
        [
            "artifacts",
            "fetch",
            "--package",
            str(package_dir),
            "--url",
            PDF_URL,
            "--product",
            PRODUCT,
            "--actor",
            "tester",
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["ok"] is True
    assert payload["items"][0]["status"] == "fetched"
    assert isinstance(payload["items"][0]["max_bytes"], int)
    assert payload["declaration"]["changed"] == 1

    manual_dir = package_dir / "sources" / "manual"
    written = [path for path in manual_dir.rglob("*") if path.is_file()]
    assert len(written) == 1
    assert written[0].read_bytes() == PDF_BODY

    declared = load_manual_artifacts(package_dir)
    assert declared.count == 1
    entry = declared.entries[0]
    assert entry.product_id == PRODUCT
    assert entry.url == PDF_URL
    assert entry.sha256 == hashlib.sha256(PDF_BODY).hexdigest()
    assert entry.size == len(PDF_BODY)
    assert entry.provided_by == "tester"
    assert entry.path == written[0].resolve()


def test_fetch_from_blocked_items_uses_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Без ``--url`` набор берётся из блокированных записей прогона."""
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, items=[_item(PDF_URL)])
    package_dir = _package(tmp_path, monkeypatch)
    monkeypatch.setattr("core.cli.commands.artifacts.HttpCrawler", FakeCrawler)

    code = main(
        [
            "artifacts",
            "fetch",
            "--run-id",
            RUN_ID,
            "--package",
            str(package_dir),
            "--actor",
            "tester",
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["run_id"] == RUN_ID
    assert payload["items"][0]["url"] == PDF_URL
    assert payload["items"][0]["status"] == "fetched"


def test_fetch_requires_actor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без ``--actor`` — ошибка входа (код 2): подача подписывается человеком."""
    package_dir = _package(tmp_path, monkeypatch)

    code = main(
        [
            "artifacts",
            "fetch",
            "--package",
            str(package_dir),
            "--url",
            PDF_URL,
            "--product",
            PRODUCT,
        ]
    )
    assert code == EXIT_USAGE
    assert "--actor" in capsys.readouterr().err
    assert not (package_dir / "sources" / "manual_artifacts.yaml").exists()


def test_fetch_rerun_is_idempotent_and_reports_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Повтор не переписывает декларацию: ``changed=0``, ``digest_changed=false``.

    Материал ``run_id`` — ``(product_id, url, sha256)``. Второй прогон команды не меняет
    ни его, ни провенанс записи: иначе «ничего не изменилось» выглядело бы правкой аудита,
    а оператор — предупреждением о новом ``run_id``, которого не будет.
    """
    package_dir = _package(tmp_path, monkeypatch)
    monkeypatch.setattr("core.cli.commands.artifacts.HttpCrawler", FakeCrawler)
    argv = [
        "artifacts",
        "fetch",
        "--package",
        str(package_dir),
        "--url",
        PDF_URL,
        "--product",
        PRODUCT,
        "--actor",
        "tester",
        "--json",
    ]

    assert main(argv) == EXIT_OK
    first = json_out(capsys)
    assert first["declaration"] == {
        "path": str(package_dir / "sources" / "manual_artifacts.yaml"),
        "changed": 1,
        "digest_changed": True,
    }

    assert main(argv) == EXIT_OK
    second = json_out(capsys)
    assert second["items"][0]["status"] == "unchanged"
    assert second["declaration"]["changed"] == 0
    assert second["declaration"]["digest_changed"] is False
