"""``hound gc`` (``core.pipeline.gc``): удаляется только то, на что не ссылается ``state.db``.

Что эти тесты держат закрытым:

* **живое множество из двух таблиц.** ``container_state.raw_path`` — не украшение:
  контейнер не документ, ``document_version`` у него нет по построению, и без второй
  таблицы байты архива удалялись бы как «ничьи»;
* **соседние продукты того же каталога не трогаются.** ``corpus_raw`` общий для всех
  продуктов, а ``state.db`` одного пакета знает не всех — чужой каталог считается и не
  удаляется;
* **кэш атрибутируется, а не угадывается.** Ключ без строки ``url_state`` остаётся:
  кэш глобальный и общий между пакетами, и «строки нет» значит «атрибуция неизвестна»;
* **отказы до удаления.** Пустая ``document_version`` при непустом ``corpus_raw`` — это
  «удалить весь корпус», а не «всё сироты»; ``--apply`` без ``--actor`` не пишет ничего;
* **dry-run ничего не удаляет**, а ``--apply`` оставляет след: строку на каждый файл.

Сеть не используется, реальные каталоги не трогаются: всё в ``tmp_path``.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from core.cli.commands.gc import main
from core.pipeline.gc import (
    AUDIT_NAME,
    CLASS_ORDER,
    GcRefused,
    scan_gc,
)
from core.util.cache import http_key

ROOT = "https://p1.test/docs/"
IN_SPACE = "https://p1.test/docs/guide.html"
OUT_SPACE = "https://retired.p1.test/docs/old.html"

# ------------------------------------------------------------------------ фикстуры


def _package(tmp_path: Path, *, products: tuple[str, ...] = ("p1",)) -> Path:
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text(
        "".join(f"{pid}:\n- {ROOT}\n" for pid in products), encoding="utf-8"
    )
    return package


def _state(
    tmp_path: Path,
    *,
    urls: list[tuple[str, str]] = (),
    raw_paths: list[str] = (),
    container_paths: list[str] = (),
    products: tuple[str, ...] = ("p1",),
) -> Path:
    """Минимальная подложка: только три таблицы, нужные решению об удалении."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE url_state (
            product_id TEXT, url TEXT, normalized_url TEXT, doc_version_id TEXT
        );
        CREATE TABLE document_version (
            doc_version_id TEXT, product_id TEXT, url TEXT, raw_path TEXT, status TEXT
        );
        CREATE TABLE container_state (
            product_id TEXT, url TEXT, normalized_url TEXT, raw_path TEXT, status TEXT
        );
        """
    )
    for product_id, url in urls:
        conn.execute(
            "INSERT INTO url_state VALUES (?, ?, ?, NULL)", (product_id, url, url)
        )
    for index, raw in enumerate(raw_paths):
        conn.execute(
            "INSERT INTO document_version VALUES (?, ?, ?, ?, 'active')",
            (f"dv_{index}", products[0], ROOT, raw),
        )
    for index, raw in enumerate(container_paths):
        conn.execute(
            "INSERT INTO container_state VALUES (?, ?, ?, ?, 'descended')",
            (products[0], f"https://p1.test/docs/bundle{index}.zip", ROOT, raw),
        )
    conn.commit()
    conn.close()
    return db


def _raw(root: Path, product: str, name: str, body: bytes = b"x") -> Path:
    directory = root / product
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(body)
    return path


def _cache_file(cache: Path, url: str, *, meta: bool = False) -> list[Path]:
    directory = cache / "http"
    directory.mkdir(parents=True, exist_ok=True)
    key = http_key(url)
    made = [directory / f"{key}.bin"]
    made[0].write_bytes(b"payload")
    if meta:
        made.append(directory / f"{key}.meta.json")
        made[1].write_text("{}", encoding="utf-8")
    return made


def _run(tmp_path: Path, package: Path, db: Path, *extra: str) -> int:
    return _run_dbs(tmp_path, package, [db], *extra)


def _run_dbs(
    tmp_path: Path,
    package: Path,
    dbs: list[Path],
    *extra: str,
    state_root: Path | None = None,
) -> int:
    argv = [
        "--package",
        str(package),
        "--corpus-raw",
        str(tmp_path / "corpus_raw"),
        "--cache",
        str(tmp_path / "cache"),
    ]
    for db in dbs:
        argv.extend(["--state-db", str(db)])
    if state_root is not None:
        argv.extend(["--state-root", str(state_root)])
    return main([*argv, *extra])


# ------------------------------------------------------------------------ corpus_raw


def test_unreferenced_raw_is_deleted_referenced_is_kept(tmp_path: Path) -> None:
    """Сирота удаляется, файл из ``document_version.raw_path`` — нет."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    referenced = _raw(root, "p1", "keep.bin")
    orphan = _raw(root, "p1", "drop.bin")
    db = _state(tmp_path, raw_paths=[str(referenced)])

    assert _run(tmp_path, package, db, "--apply", "--actor", "tester") == 0

    assert referenced.exists(), "файл из document_version удалять нельзя"
    assert not orphan.exists(), "файл без ссылки — сирота"


def test_container_only_reference_is_kept(tmp_path: Path) -> None:
    """Байты контейнера живы по ``container_state``: версии документа у них нет."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    archive = _raw(root, "p1", "bundle.bin")
    live = _raw(root, "p1", "live.bin")
    db = _state(tmp_path, raw_paths=[str(live)], container_paths=[str(archive)])

    assert _run(tmp_path, package, db, "--apply", "--actor", "tester") == 0

    assert archive.exists(), "container_state.raw_path — обязательная часть живого множества"


def test_foreign_product_dir_is_reported_not_deleted(tmp_path: Path) -> None:
    """Каталог не-продукта пакета считается и не трогается."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    foreign = _raw(root, "someone_else", "a.bin")
    db = _state(tmp_path)

    orphans = scan_gc(
        package=package,
        state_db=db,
        corpus_raw=root,
        cache=tmp_path / "cache",
    )

    assert [item.cls for item in orphans] == ["corpus_raw_foreign_product"]
    assert _run(tmp_path, package, db, "--apply", "--actor", "tester") == 0
    assert foreign.exists()


def test_empty_document_version_with_nonempty_corpus_refuses(tmp_path: Path) -> None:
    """Опорожнённая БД не читается как «удалить всё»: отказ до удаления."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    survivor = _raw(root, "p1", "a.bin")
    db = _state(tmp_path)  # document_version пуста

    assert _run(tmp_path, package, db, "--apply", "--actor", "tester") == 2
    assert survivor.exists()


def test_container_state_is_optional(tmp_path: Path) -> None:
    """БД без ``container_state`` принимается: её отсутствие — «контейнеров не было».

    Канонная БД пакета такой таблицы не имеет (замер 27.11.2026); отказ на ней
    заблокировал бы объединение живых множеств.
    """
    package = _package(tmp_path)
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE url_state (product_id TEXT, normalized_url TEXT);
        CREATE TABLE document_version (product_id TEXT, raw_path TEXT);
        """
    )
    conn.commit()
    conn.close()

    assert _run(tmp_path, package, db) == 0


def test_missing_url_state_refuses(tmp_path: Path) -> None:
    """Без ``url_state`` это не подложка пакета — отказ."""
    package = _package(tmp_path)
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE document_version (product_id TEXT, raw_path TEXT)")
    conn.commit()
    conn.close()

    assert _run(tmp_path, package, db) == 2


def test_empty_product_set_refuses(tmp_path: Path) -> None:
    """Пакет без продуктов и подложка без ``product_id`` — отказ."""
    package = tmp_path / "pkg"
    package.mkdir()
    db = _state(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM document_version")
    conn.commit()
    conn.close()

    assert _run(tmp_path, package, db) == 2


# ------------------------------------------------------------------------ http cache


def test_cache_key_outside_space_deleted_with_meta(tmp_path: Path) -> None:
    """Ключ вне объявленного пространства уходит вместе с сайдкаром, в пространстве — нет."""
    package = _package(tmp_path)
    outside = _cache_file(tmp_path / "cache", OUT_SPACE, meta=True)
    inside = _cache_file(tmp_path / "cache", IN_SPACE, meta=True)
    db = _state(tmp_path, urls=[("p1", OUT_SPACE), ("p1", IN_SPACE)])

    assert _run(tmp_path, package, db, "--apply", "--actor", "tester") == 0

    assert not any(path.exists() for path in outside), "вне пространства — сирота"
    assert all(path.exists() for path in inside), "в пространстве — живой материал"


def test_unattributed_cache_key_is_reported_and_kept(tmp_path: Path) -> None:
    """Ключ без строки ``url_state`` не удаляется: кэш общий между пакетами."""
    package = _package(tmp_path)
    unknown = _cache_file(tmp_path / "cache", "https://other.test/page.html")
    db = _state(tmp_path)

    orphans = scan_gc(
        package=package,
        state_db=db,
        corpus_raw=tmp_path / "corpus_raw",
        cache=tmp_path / "cache",
    )

    assert [item.cls for item in orphans] == ["http_cache_unattributed"]
    assert _run(tmp_path, package, db, "--apply", "--actor", "tester") == 0
    assert unknown[0].exists()


def test_unknown_only_class_refuses(tmp_path: Path) -> None:
    """Неизвестный класс в ``--only`` — отказ, а не молчаливый полный проход."""
    package = _package(tmp_path)
    db = _state(tmp_path)

    assert _run(tmp_path, package, db, "--only", "cache") == 2


# ------------------------------------------------------------------------ apply/аудит


def test_apply_requires_actor(tmp_path: Path) -> None:
    """``--apply`` без ``--actor`` — код 2 и ни одного удаления."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    orphan = _raw(root, "p1", "drop.bin")
    db = _state(tmp_path)

    assert _run(tmp_path, package, db, "--apply") == 2
    assert orphan.exists()


def test_dry_run_deletes_nothing_and_writes_no_audit(tmp_path: Path) -> None:
    """По умолчанию — только отчёт: ни файла, ни строки аудита."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    orphan = _raw(root, "p1", "drop.bin")
    db = _state(tmp_path, raw_paths=[str(live)])

    assert _run(tmp_path, package, db, "--json") == 0

    assert orphan.exists()
    assert not (db.parent / AUDIT_NAME).exists()


def test_audit_has_one_line_per_deleted_file(tmp_path: Path) -> None:
    """Аудит: сводка первой строкой, дальше по строке на каждый удалённый файл."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    _raw(root, "p1", "one.bin")
    _raw(root, "p1", "two.bin")
    _cache_file(tmp_path / "cache", OUT_SPACE, meta=True)
    db = _state(tmp_path, urls=[("p1", OUT_SPACE)], raw_paths=[str(live)])
    audit = tmp_path / "audit.jsonl"

    assert (
        _run(tmp_path, package, db, "--apply", "--actor", "tester", "--audit", str(audit))
        == 0
    )

    lines = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
    summary, *events = lines
    assert summary["actor"] == "tester"
    assert set(summary["classes"]) == set(CLASS_ORDER)
    assert summary["total"] == {"files": 4, "bytes": summary["total"]["bytes"]}
    assert len(events) == 4, "два .bin + байты и сайдкар кэша"
    assert {event["class"] for event in events} == {
        "corpus_raw_unreferenced",
        "http_cache_outside_space",
    }
    assert all(not Path(event["path"]).exists() for event in events)


def test_scan_does_not_touch_state_db(tmp_path: Path) -> None:
    """Замер ничего не пишет: файл БД остаётся байт-в-байт прежним."""
    package = _package(tmp_path)
    live = _raw(tmp_path / "corpus_raw", "p1", "live.bin")
    _raw(tmp_path / "corpus_raw", "p1", "drop.bin")
    db = _state(tmp_path, raw_paths=[str(live)])
    before = db.read_bytes()

    scan_gc(
        package=package,
        state_db=db,
        corpus_raw=tmp_path / "corpus_raw",
        cache=tmp_path / "cache",
    )

    assert db.read_bytes() == before


def test_scan_reports_refusal_type(tmp_path: Path) -> None:
    """Отказы поднимаются как :class:`GcRefused`, а не как пустой план."""
    package = _package(tmp_path)
    db = _state(tmp_path)
    with pytest.raises(GcRefused):
        scan_gc(
            package=package,
            state_db=db,
            corpus_raw=tmp_path / "corpus_raw",
            cache=tmp_path / "cache",
            only=frozenset({"nonexistent"}),
        )


# ------------------------------------------------------------------ несколько БД


def test_reference_in_second_db_keeps_file(tmp_path: Path) -> None:
    """Файл, на который ссылается только **вторая** БД, не удаляется.

    Это и есть измеренный дефект: один ``corpus_raw`` делят канонная БД и БД стенда, и
    прогон по одной из них удалял 5 455 файлов / 1.09 GB, нужных другой (27.11.2026).
    """
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    shared = _raw(root, "p1", "shared.bin")
    orphan = _raw(root, "p1", "drop.bin")
    first = _state(tmp_path / "a", raw_paths=[])
    second = _state(tmp_path / "b", raw_paths=[str(shared)])

    assert _run_dbs(tmp_path, package, [first, second], "--apply", "--actor", "t") == 0

    assert shared.exists(), "живое множество — объединение по всем БД"
    assert not orphan.exists()


def test_state_root_finds_nested_state_dbs(tmp_path: Path) -> None:
    """``--state-root`` находит все ``state.db`` под каталогом (вложенная раскладка)."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    shared = _raw(root, "p1", "shared.bin")
    state_root = tmp_path / "states"
    (state_root / "a").mkdir(parents=True)
    (state_root / "b").mkdir(parents=True)
    _state(state_root / "a")
    _state(state_root / "b", raw_paths=[str(shared)])

    assert (
        _run_dbs(
            tmp_path, package, [], "--apply", "--actor", "t", state_root=state_root
        )
        == 0
    )

    assert shared.exists()


def test_product_without_rows_in_any_db_refuses(tmp_path: Path) -> None:
    """Байты есть, строк нет ни в одной БД — «смотрели не туда», а не «ссылок нет»."""
    package = _package(tmp_path)
    survivor = _raw(tmp_path / "corpus_raw", "p1", "a.bin")
    db = _state(tmp_path / "a")  # ни одной строки продукта

    assert _run_dbs(tmp_path, package, [db], "--apply", "--actor", "t") == 2
    assert survivor.exists()


def test_no_state_db_and_no_root_refuses(tmp_path: Path) -> None:
    """Хотя бы одна БД обязательна: без подложки живое множество неизвестно."""
    package = _package(tmp_path)
    survivor = _raw(tmp_path / "corpus_raw", "p1", "a.bin")

    assert _run_dbs(tmp_path, package, []) == 2
    assert survivor.exists()


def test_uncovered_state_db_is_reported_but_not_blocking(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Соседняя БД вне набора — наблюдение в ``--json``, а не отказ."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    orphan = _raw(root, "p1", "drop.bin")
    states = tmp_path / "states"
    (states / "a").mkdir(parents=True)
    used = _state(
        states / "a",
        urls=[("p1", IN_SPACE)],
        raw_paths=[str(_raw(root, "p1", "live.bin"))],
    )
    # Третья БД рядом с использованной (в пределах 4 уровней вверх), но не в наборе.
    _state(states, raw_paths=[str(_raw(root, "p1", "elsewhere.bin"))])

    assert _run_dbs(tmp_path, package, [used], "--json") == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["uncovered_state_dbs"], "соседняя БД обязана быть названа"
    assert payload["uncovered_state_dbs"][0]["products"] == ["p1"]
    assert orphan.exists(), "наблюдение не блокирует и не удаляет"


def test_json_reports_every_state_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Оператор видит, что именно объединили: список БД и живых путей в каждой."""
    package = _package(tmp_path)
    shared = _raw(tmp_path / "corpus_raw", "p1", "shared.bin")
    first = _state(tmp_path / "a")
    second = _state(tmp_path / "b", raw_paths=[str(shared)])

    assert _run_dbs(tmp_path, package, [first, second], "--json") == 0
    payload = json.loads(capsys.readouterr().out)

    assert len(payload["state_dbs"]) == 2
    assert [info["live_paths"] for info in payload["state_db_info"]] == [0, 1]
    assert payload["live_paths"] == 1


# ------------------------------------------------------------------ цена удаления


def _run_db(
    directory: Path,
    *,
    run_id: str,
    refs: list[str] = (),
    g4: bool = False,
    cells: int = 0,
    session_status: str | None = None,
    session_refs: list[str] = (),
    with_raw_tables: bool = True,
) -> Path:
    """Минимальная прогонная БД: только таблицы, которые читает цензус.

    ``with_raw_tables=False`` — БД старой схемы: таблиц ссылок нет вовсе (читатель обязан
    пропустить их молча).
    """
    directory.mkdir(parents=True, exist_ok=True)
    db = directory / "cohound.db"
    conn = sqlite3.connect(db)
    statements = [
        "CREATE TABLE gate_decisions (run_id TEXT, gate TEXT, verdict TEXT)",
        "CREATE TABLE cells (run_id TEXT, product_id TEXT, param_id TEXT)",
        "CREATE TABLE crawl_sessions (session_id TEXT, status TEXT)",
    ]
    if with_raw_tables:
        statements += [
            "CREATE TABLE corpus_docs (doc_id TEXT, raw_path TEXT)",
            "CREATE TABLE extract_queue (session_id TEXT, raw_path TEXT)",
            "CREATE TABLE crawl_session_items (session_id TEXT, raw_path TEXT)",
        ]
    conn.executescript(";".join(statements))
    if g4:
        conn.execute("INSERT INTO gate_decisions VALUES (?, 'G4', 'approved')", (run_id,))
    for index in range(cells):
        conn.execute("INSERT INTO cells VALUES (?, 'p1', ?)", (run_id, f"param_{index}"))
    if session_status is not None:
        conn.execute("INSERT INTO crawl_sessions VALUES ('s1', ?)", (session_status,))
    if with_raw_tables:
        for raw in refs:
            conn.execute("INSERT INTO corpus_docs VALUES ('d', ?)", (raw,))
        for raw in session_refs:
            conn.execute("INSERT INTO crawl_session_items VALUES ('s1', ?)", (raw,))
    conn.commit()
    conn.close()
    return db


def _runs_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "runs"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def test_run_db_reference_becomes_run_history(tmp_path: Path) -> None:
    """Ссылка только из прогонной БД — класс ``corpus_raw_run_history``, и он удаляется."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    history = _raw(root, "p1", "history.bin")
    orphan = _raw(root, "p1", "drop.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_a", run_id="run_a", refs=[str(history)])

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--apply",
            "--actor",
            "t",
            "--runs-dir",
            str(runs),
        )
        == 0
    )

    assert not history.exists(), "цена названа — но удаление всё равно происходит"
    assert not orphan.exists()
    assert live.exists()


def test_price_split_live_vs_dead_runs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Живой прогон (G4 approved) и мёртвый попадают в разные строки цены."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    from_live = _raw(root, "p1", "from_live.bin", b"a" * 10)
    from_dead = _raw(root, "p1", "from_dead.bin", b"b" * 20)
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_live", run_id="run_live", refs=[str(from_live)], g4=True)
    _run_db(runs / "run_dead", run_id="run_dead", refs=[str(from_dead)])

    assert (
        _run_dbs(
            tmp_path, package, [db], "--json", "--runs-dir", str(runs)
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["price"]["by_live_runs"] == {"files": 1, "bytes": 10}
    assert payload["price"]["by_dead_runs"] == {"files": 1, "bytes": 20}
    assert payload["classes"]["corpus_raw_run_history"]["files"] == 2
    assert payload["classes"]["corpus_raw_unreferenced"]["files"] == 0


def test_price_resumable_session_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``by_resumable_session`` — флаг: ``running`` взводит, ``finished`` — нет."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    resumable = _raw(root, "p1", "resumable.bin")
    finished = _raw(root, "p1", "finished.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(
        runs / "run_running",
        run_id="run_running",
        refs=[str(resumable)],
        session_status="running",
        session_refs=[str(resumable)],
    )
    _run_db(
        runs / "run_finished",
        run_id="run_finished",
        refs=[str(finished)],
        session_status="finished",
        session_refs=[str(finished)],
    )

    assert _run_dbs(tmp_path, package, [db], "--json", "--runs-dir", str(runs)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["price"]["by_resumable_session"]["files"] == 1
    sessions = {
        run["run_id"]: run["resumable_sessions"]
        for run in payload["run_census"]["runs"]
    }
    assert sessions == {"run_running": 1, "run_finished": 0}


def test_without_runs_dbs_price_unknown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без прогонных БД класс истории пуст, кандидаты — в ``_unreferenced``, есть stderr."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    candidate = _raw(root, "p1", "candidate.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_a", run_id="run_a", refs=[str(candidate)])

    assert _run_dbs(tmp_path, package, [db], "--json") == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["classes"]["corpus_raw_run_history"]["files"] == 0
    assert payload["classes"]["corpus_raw_unreferenced"]["files"] == 1
    assert "цена удаления неизвестна" in captured.err


def test_audit_records_runs_per_file(tmp_path: Path) -> None:
    """Построчная запись несёт ``runs``; у истинной сироты список пуст."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    history = _raw(root, "p1", "history.bin")
    _raw(root, "p1", "drop.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_a", run_id="run_a", refs=[str(history)], g4=True)
    audit = tmp_path / "audit.jsonl"

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--apply",
            "--actor",
            "t",
            "--runs-dir",
            str(runs),
            "--audit",
            str(audit),
        )
        == 0
    )

    lines = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
    summary, *events = lines
    by_name = {Path(event["path"]).name: event for event in events}
    assert by_name["history.bin"]["runs"] == ["run_a"]
    assert by_name["drop.bin"]["runs"] == []
    assert summary["run_census"]["runs"][0]["run_id"] == "run_a"
    assert summary["run_census"]["price"]["by_live_runs"]["files"] == 1


def test_run_db_without_raw_tables_is_tolerated(tmp_path: Path) -> None:
    """БД без ``corpus_docs``/``extract_queue``/``crawl_session_items`` не ломает прогон."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    orphan = _raw(root, "p1", "drop.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_old", run_id="run_old", with_raw_tables=False)

    assert _run_dbs(tmp_path, package, [db], "--runs-dir", str(runs)) == 0
    assert orphan.exists(), "dry-run ничего не удаляет"


def test_runs_root_finds_nested_run_dbs(tmp_path: Path) -> None:
    """``--runs-root`` находит ``cohound.db`` рекурсивно и считает цену."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    history = _raw(root, "p1", "history.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs_root = tmp_path / "stands" / "runs"
    _run_db(runs_root / "deep" / "run_a", run_id="run_a", refs=[str(history)])

    assert (
        _run_dbs(tmp_path, package, [db], "--runs-root", str(runs_root)) == 0
    )
    assert history.exists()


# ------------------------------------------------------------------ соседние пакеты


def _package_with_root(tmp_path: Path, name: str, root: str) -> Path:
    """Пакет с одним продуктом ``p1`` и объявленным корнем ``root``."""
    package = tmp_path / name
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text(
        f"p1:\n- {root}\n", encoding="utf-8"
    )
    return package


def test_key_inside_neighbour_package_is_rescued(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Адрес вне первого пакета, но внутри второго — ключ не удаляется, а считается спасённым."""
    package = _package(tmp_path, products=("p1",))
    neighbour = _package_with_root(tmp_path, "neighbour", "https://other.test/docs/")
    other_url = "https://other.test/docs/page.html"
    kept = _cache_file(tmp_path / "cache", other_url, meta=True)
    db = _state(tmp_path / "state", urls=[("p1", other_url)])

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--json",
            "--also-package",
            str(neighbour),
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert all(path.exists() for path in kept), "ключ объявлен соседним пакетом"
    assert payload["classes"]["http_cache_outside_space"]["files"] == 0
    assert payload["rescued_by_other_packages"]["files"] == 2, "байты и сайдкар"
    assert payload["packages"] == [str(package), str(neighbour)]


def test_also_package_without_corpus_urls_refuses(tmp_path: Path) -> None:
    """Каталог без ``corpus_urls.yaml`` — отказ, а не тихое «пакета нет»."""
    package = _package(tmp_path)
    empty = tmp_path / "not_a_package"
    empty.mkdir()
    db = _state(tmp_path / "state")

    assert _run_dbs(tmp_path, package, [db], "--also-package", str(empty)) == 2


# ------------------------------------------------------------------ дренаж


def test_by_drain_counts_dead_run_with_running_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Мёртвый прогон с ``status='running'`` — это незавершённая докачка, а не архив."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    draining = _raw(root, "p1", "draining.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(
        runs / "run_drain",
        run_id="run_drain",
        refs=[str(draining)],
        session_status="running",
        session_refs=[str(draining)],
    )

    assert _run_dbs(tmp_path, package, [db], "--json", "--runs-dir", str(runs)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["price"]["by_drain"] == {"files": 1, "bytes": 1}
    assert payload["price"]["by_resumable_session"]["files"] == 1
    assert payload["price"]["by_dead_runs"]["files"] == 1


def test_by_drain_ignores_finished_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Завершённый сеанс дренажом не считается."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    archived = _raw(root, "p1", "archived.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(
        runs / "run_done",
        run_id="run_done",
        refs=[str(archived)],
        session_status="finished",
        session_refs=[str(archived)],
    )

    assert _run_dbs(tmp_path, package, [db], "--json", "--runs-dir", str(runs)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["price"]["by_drain"] == {"files": 0, "bytes": 0}
    assert payload["price"]["by_resumable_session"] == {"files": 0, "bytes": 0}
    assert payload["price"]["by_dead_runs"]["files"] == 1


def test_by_drain_excludes_live_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Живой прогон с возобновляемым сеансом — не дренаж (докачка не потеряна)."""
    package = _package(tmp_path)
    root = tmp_path / "corpus_raw"
    live = _raw(root, "p1", "live.bin")
    active = _raw(root, "p1", "active.bin")
    db = _state(tmp_path / "state", raw_paths=[str(live)])
    runs = _runs_dir(tmp_path)
    _run_db(
        runs / "run_live",
        run_id="run_live",
        refs=[str(active)],
        g4=True,
        session_status="running",
        session_refs=[str(active)],
    )

    assert _run_dbs(tmp_path, package, [db], "--json", "--runs-dir", str(runs)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["price"]["by_drain"] == {"files": 0, "bytes": 0}
    assert payload["price"]["by_resumable_session"]["files"] == 1


# ------------------------------------------------------------------ отчёт по прогонам


def test_runs_classes_split_signed_resumable_unsigned(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``runs_signed``/``runs_resumable``/``runs_unsigned`` — три разных факта, не один.

    Подпись G4 — опубликованное evidence; возобновляемый сеанс — переигровка без сети;
    всё остальное — не кандидаты на удаление (waiver легален, ячейки уже произведены).
    """
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_signed", run_id="run_signed", g4=True, cells=3)
    _run_db(
        runs / "run_resumable",
        run_id="run_resumable",
        session_status="running",
        session_refs=[],
    )
    _run_db(runs / "run_plain", run_id="run_plain", cells=7)

    assert _run_dbs(tmp_path, package, [db], "--json", "--runs-dir", str(runs)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["classes"]["runs_signed"]["runs"] == 1
    assert payload["classes"]["runs_resumable"]["runs"] == 1
    assert payload["classes"]["runs_unsigned"]["runs"] == 1
    for cls in ("runs_signed", "runs_resumable", "runs_unsigned"):
        assert payload["classes"][cls]["deletable"] is False, cls
    assert payload["classes"]["runs_unsigned"]["cells"] == 7
    assert [item["run_id"] for item in payload["runs"]["runs_unsigned"]] == ["run_plain"]
    assert payload["runs"]["runs_unsigned"][0]["cells"] == 7
    assert payload["runs"]["runs_unsigned"][0]["bytes"] > 0


def test_runs_report_prints_cells_and_size(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человекочитаемый отчёт печатает ячейки и размер, а не только ``run_id``.

    Иначе 1267-ячеечная карта неотличима от пустой пробы — и решение «удалить прогон»
    принималось бы вслепую.
    """
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_map", run_id="run_map", cells=1267)

    assert _run_dbs(tmp_path, package, [db], "--runs-dir", str(runs)) == 0
    out = capsys.readouterr().out

    assert "runs_unsigned" in out
    assert "1267" in out
    assert "НЕ кандидаты на удаление" in out


# ------------------------------------------------------------------ бюджеты кэшей


def test_cache_budget_reports_over_and_without(tmp_path: Path) -> None:
    """``cache_budget``: объявленный бюджет и его отсутствие — разные факты."""
    from core.pipeline.gc import build_gc_plan

    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    extract = tmp_path / "extract_cache"
    extract.mkdir()
    (extract / "a.json").write_bytes(b"x" * 20)
    embed = tmp_path / "embed_cache"
    embed.mkdir()
    (embed / "b.npy").write_bytes(b"y" * 5)

    plan = build_gc_plan(
        package=package,
        state_dbs=[db],
        corpus_raw=tmp_path / "corpus_raw",
        cache=tmp_path / "cache",
        extract_cache=extract,
        embed_cache=embed,
        eval_cache=None,
        extract_cache_budget=10,
        only=frozenset({"cache_budget"}),
    )
    rows = {row.name: row for row in plan.cache_budgets}

    assert rows["extract_cache"].bytes == 20
    assert rows["extract_cache"].budget == 10
    assert rows["extract_cache"].over_budget is True
    assert rows["extract_cache"].share == pytest.approx(2.0)
    assert rows["embed_cache"].budget is None
    assert rows["embed_cache"].over_budget is False
    assert rows["embed_cache"].share is None


# ------------------------------------------------------------------ --prune-run


def test_prune_run_refuses_signed(tmp_path: Path) -> None:
    """Подписанный G4 прогон не удаляется ни при каких флагах."""
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    target = _run_db(runs / "run_signed", run_id="run_signed", g4=True)

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--prune-run",
            "run_signed",
            "--apply",
            "--actor",
            "t",
        )
        == 2
    )
    assert target.exists(), "опубликованное evidence удалять нельзя"


def test_prune_run_refuses_resumable(tmp_path: Path) -> None:
    """Возобновляемый сеанс: удаление потеряло бы переигровку без сети."""
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    target = _run_db(
        runs / "run_resumable", run_id="run_resumable", session_status="running"
    )

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--prune-run",
            "run_resumable",
            "--apply",
            "--actor",
            "t",
        )
        == 2
    )
    assert target.exists()


def test_prune_run_refuses_unknown(tmp_path: Path) -> None:
    """Несуществующий ``run_id`` — отказ, а не молчаливое «удалять нечего»."""
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_a", run_id="run_a")

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--prune-run",
            "run_absent",
            "--apply",
            "--actor",
            "t",
        )
        == 2
    )


def test_prune_run_deletes_unsigned_and_warns_about_cells(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Незанятый прогон удаляется явным именем; ячейки — громкое предупреждение.

    Отказ на ``cells > 0`` сделал бы невозможным удаление законно отработавшего прогона,
    а молчание сделало бы удаление слепым: печатается число ячеек и ``outputs/``.
    """
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    target = _run_db(runs / "run_plain", run_id="run_plain", cells=42)
    (target.parent / "outputs").mkdir()
    (target.parent / "outputs" / "cells.csv").write_text("x", encoding="utf-8")
    audit = tmp_path / "audit.jsonl"

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--prune-run",
            "run_plain",
            "--apply",
            "--actor",
            "t",
            "--audit",
            str(audit),
        )
        == 0
    )

    captured = capsys.readouterr()
    assert not target.parent.exists(), "названный прогон удаляется целиком"
    assert "ВНИМАНИЕ: run_plain: 42 ячеек и 1 файлов outputs/" in captured.err
    lines = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
    summary, *events = lines
    assert summary["runs_pruned"][0]["run_id"] == "run_plain"
    assert summary["runs_pruned"][0]["cells"] == 42
    assert events[0]["class"] == "prune_run"
    assert events[0]["run_id"] == "run_plain"


# ------------------------------------------------------------------ паритет предиката


def test_space_predicate_parity_with_instrument(tmp_path: Path) -> None:
    """``core.pipeline.gc`` и ``scripts.measure_substrate_reuse`` решают пространство одинаково.

    Две копии предиката расходятся молча — в этом репозитории это уже случалось
    (``catalog_digest`` без ``semantics``). Гейт сверяет и построение корней, и решения
    на наборе адресов, где ответ известен: поддомен, ``www.``, граница сегмента пути,
    percent-encoded traversal.
    """
    from core.pipeline.crawl_frontier import CrawlLimits, Frontier
    from core.pipeline.gc import declared_scopes, in_space
    from scripts.measure_substrate_reuse import _in_space
    from scripts.measure_substrate_reuse import declared_scopes as instrument_scopes

    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text(
        "p1:\n- https://p1.test/docs/\n- https://www.p1.test/docs/\n", encoding="utf-8"
    )
    (package / "domain_profiles.yaml").write_text(
        "p1.test/docs:\n  max_depth: 3\nsub.p1.test:\n  max_depth: 1\n",
        encoding="utf-8",
    )

    core_roots, core_hosts = declared_scopes(package)
    inst_roots, inst_hosts = instrument_scopes(package)
    assert core_roots == inst_roots
    assert core_hosts == inst_hosts

    frontiers = [
        Frontier(root, CrawlLimits(), scope_hosts=core_hosts.get("p1", frozenset()))
        for root in core_roots["p1"]
    ]
    urls = [
        "https://p1.test/docs/guide.html",
        "https://p1.test/docs/",
        "https://p1.test/docs-old/page.html",
        "https://p1.test/docs/%2e%2e/private",
        "https://sub.p1.test/docs/page.html",
        "https://unlisted.p1.test/docs/page.html",
        "https://www.p1.test/docs/page.html",
        "https://other.test/docs/page.html",
        "https://p1.test/elsewhere",
        "not-a-url",
    ]
    for url in urls:
        assert in_space(url, frontiers, {}) == _in_space(url, frontiers, {}), url


# ------------------------------------------------------- бюджет http-кэша (mtime-LRU)


def _cache_pair(cache: Path, url: str, *, body: bytes, mtime: float) -> list[Path]:
    """Пара ``{key}.bin`` + ``{key}.meta.json`` с заданным ``mtime`` (порядок вытеснения)."""
    made = _cache_file(cache, url, meta=True)
    for path in made:
        os.utime(path, (mtime, mtime))
    for path in made:
        path.write_bytes(body)
    return made


def test_http_cache_over_budget_evicts_oldest(tmp_path: Path) -> None:
    """Бюджет меньше объёма: вытесняется самая старая пара, а не любая."""
    package = _package(tmp_path)
    cache = tmp_path / "cache"
    old = _cache_pair(cache, IN_SPACE, body=b"x" * 10, mtime=1_000.0)
    mid = _cache_pair(cache, "https://p1.test/docs/b.html", body=b"x" * 10, mtime=2_000.0)
    new = _cache_pair(cache, "https://p1.test/docs/c.html", body=b"x" * 10, mtime=3_000.0)
    db = _state(tmp_path / "state", urls=[("p1", IN_SPACE)])

    orphans = scan_gc(
        package=package,
        state_db=db,
        corpus_raw=tmp_path / "corpus_raw",
        cache=cache,
        http_cache_budget=40,
        only=frozenset({"http_cache_over_budget"}),
    )

    evicted = {item.path for item in orphans}
    assert evicted == set(old), "вытесняется самая старая ЗАПИСЬ (mtime), а не произвольная"
    assert all(path.exists() for path in mid + new), "свежие пары остаются"


def test_http_cache_without_budget_has_no_eviction(tmp_path: Path) -> None:
    """Бюджет не объявлен — класс пуст: «границы нет» не значит «всё в порядке»."""
    package = _package(tmp_path)
    cache = tmp_path / "cache"
    _cache_pair(cache, IN_SPACE, body=b"x" * 10, mtime=1_000.0)
    db = _state(tmp_path / "state", urls=[("p1", IN_SPACE)])

    orphans = scan_gc(
        package=package,
        state_db=db,
        corpus_raw=tmp_path / "corpus_raw",
        cache=cache,
        http_cache_budget=None,
        only=frozenset({"http_cache_over_budget"}),
    )

    assert orphans == []


# ------------------------------------------------------------ полная очистка кэша


def test_wipe_http_cache_dry_run_keeps_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без ``--apply`` очистка только считается: файлы на месте, цена названа."""
    package = _package(tmp_path)
    made = _cache_file(tmp_path / "cache", IN_SPACE, meta=True)
    db = _state(tmp_path / "state", urls=[("p1", IN_SPACE)])

    assert _run(tmp_path, package, db, "--wipe-http-cache") == 0

    assert all(path.exists() for path in made), "dry-run ничего не удаляет"
    out = capsys.readouterr().out
    assert "wipe_http_cache" in out
    assert "кэш R7 — не evidence" in out, "объём без цены читался бы как «просто место»"


def test_wipe_http_cache_apply_deletes_and_audits(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """С ``--apply`` каталог очищается целиком, и это видно в аудите."""
    package = _package(tmp_path)
    made = _cache_file(tmp_path / "cache", IN_SPACE, meta=True)
    db = _state(tmp_path / "state", urls=[("p1", IN_SPACE)])
    audit = tmp_path / "audit.jsonl"

    assert (
        _run(
            tmp_path,
            package,
            db,
            "--wipe-http-cache",
            "--apply",
            "--actor",
            "tester",
            "--audit",
            str(audit),
        )
        == 0
    )

    assert not any(path.exists() for path in made), "весь каталог http-кэша удалён"
    capsys.readouterr()
    lines = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
    assert any(line.get("class") == "wipe_http_cache" for line in lines)


def test_wipe_http_cache_missing_dir_is_not_a_refusal(tmp_path: Path) -> None:
    """Каталога нет — не отказ: очищать нечего, и это печатается, а не молчится."""
    package = _package(tmp_path)
    db = _state(tmp_path / "state")

    assert _run(tmp_path, package, db, "--wipe-http-cache") == 0


# --------------------------------------------------------- цензус цитирования прогонов


def _docs(tmp_path: Path, body: str) -> Path:
    root = tmp_path / "docs"
    (root / "reports").mkdir(parents=True)
    (root / "reports" / "note.md").write_text(body, encoding="utf-8")
    return root


def test_cited_run_is_marked_and_warns_before_prune(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Цитируемый прогон помечается, а ``--prune-run`` предупреждает, но не отказывает."""
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    target = _run_db(runs / "run_cited", run_id="run_cited")
    docs = _docs(tmp_path, "замер снят на прогоне `run_cited` (28.11.2026)\n")

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--docs-root",
            str(docs),
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "цитируется: 1" in out, "цитируемый прогон обязан быть отличим от пробы"

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--docs-root",
            str(docs),
            "--prune-run",
            "run_cited",
            "--apply",
            "--actor",
            "tester",
        )
        == 0
    )
    err = capsys.readouterr().err
    assert "цитируется в 1 документах" in err, "цена удаления называется ДО удаления"
    assert not target.parent.exists(), "предупреждение — не отказ: решение принял человек"


def test_uncited_run_has_empty_citations(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нецитируемый прогон не притворяется подложкой замера."""
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    _run_db(runs / "run_plain", run_id="run_plain")
    docs = _docs(tmp_path, "текст без упоминания прогонов\n")

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--docs-root",
            str(docs),
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "не цитируется" in out


def test_short_hash_form_counts_as_citation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Прогон, названный в замере коротким хешем, цитируется.

    Замер 28.11.2026: ``ru-pg-dbms-9c8dd0345f3f`` (из него измерен потолок согласия
    0.9381) встречается в документах только как ``9c8dd0345f3f`` — полного имени в них
    нет. Цензус по полному ``run_id`` объявлял его нецитируемым, то есть ровно тот прогон,
    ради которого проверка заведена, выглядел мусором.
    """
    package = _package(tmp_path)
    db = _state(tmp_path / "state")
    runs = _runs_dir(tmp_path)
    _run_db(runs / "ru-pg-dbms-0123456789ab", run_id="ru-pg-dbms-0123456789ab")
    docs = _docs(tmp_path, "потолок снят на прогоне `0123456789ab` (28.11.2026)\n")

    assert (
        _run_dbs(
            tmp_path,
            package,
            [db],
            "--runs-dir",
            str(runs),
            "--docs-root",
            str(docs),
        )
        == 0
    )

    out = capsys.readouterr().out
    assert "цитируется: 1" in out, "короткий хеш — тоже цитирование, а не совпадение"


def test_run_id_without_hash_is_not_matched_by_tail(tmp_path: Path) -> None:
    """``run_id`` без хеша сверяется только целиком: хвост слова не считается ссылкой."""
    from core.pipeline.gc import _citation_keys

    assert _citation_keys("gaussdb-probe") == ("gaussdb-probe",)
    assert _citation_keys("recon-pangolin_root") == ("recon-pangolin_root",)
    assert _citation_keys("pkg-0123456789ab") == ("pkg-0123456789ab", "0123456789ab")


def test_wipe_reports_size_after_deletion(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Объём очистки снимается ДО удаления: иначе отчёт печатал бы «0 B».

    Дефект был настоящим: ``WipePlan.bytes`` — свойство, суммирующее ``stat`` по файлам, а
    после ``--apply`` этих файлов уже нет. На реальном прогоне 28.11.2026 это дало строку
    «14 870 файлов 0 B (удаляется)» при освобождённых 1.5 GB.
    """
    package = _package(tmp_path)
    made = _cache_file(tmp_path / "cache", IN_SPACE, meta=True)
    db = _state(tmp_path / "state", urls=[("p1", IN_SPACE)])
    size = sum(path.stat().st_size for path in made)
    assert size > 0

    assert (
        _run(
            tmp_path,
            package,
            db,
            "--wipe-http-cache",
            "--apply",
            "--actor",
            "tester",
        )
        == 0
    )

    out = capsys.readouterr().out
    line = next(item for item in out.splitlines() if "wipe_http_cache" in item)
    assert f"{size} B" in line, f"объём снят до удаления, а не после: {line}"
    assert "освобождено" in out
