"""Прибор ``scripts/measure_substrate_reuse.py``: классы «версии нет» не смешиваются.

Дефект, который эти тесты держат закрытым: проход без сети печатал **одну** строку
«у URL нет активной версии документа; документ не вошёл в корпус» на четыре разных
факта — URL вне объявленного пространства (корень выведен из пакета), отказ сайта,
объявленную подачу и контейнер. Измерено 23.11.2026 на стенде: 1 585 таких строк, и ни
одна не была недостачей подложки.

Классы считаются по первичным таблицам (``url_state``/``document_version``/
``container_state``), а не по журналу прогона: журнал — не артефакт подложки.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.measure_substrate_reuse import Census, census, classify, declared_scopes

ROOT = "https://p1.test/docs/"
RETIRED = "https://retired.p1.test/docs/old.html"
KNOWN = "https://p1.test/docs/guide.html"


def _package(tmp_path: Path, *, roots: list[str], provided: list[str] = ()) -> Path:
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "sources" / "corpus_urls.yaml").write_text(
        "p1:\n" + "".join(f"- {url}\n" for url in roots), encoding="utf-8"
    )
    if provided:
        body = "provide:\n" + "".join(
            f"- path: sources/x.pdf\n  product_id: p1\n  url: {url}\n"
            f"  sha256: {'0' * 64}\n"
            for url in provided
        )
        (package / "sources" / "manual_artifacts.yaml").write_text(
            body, encoding="utf-8"
        )
    return package


def _state(tmp_path: Path, rows: list[tuple[str, str | None, str | None]]) -> Path:
    """Минимальная подложка: ``url_state`` + ``document_version`` + ``container_state``."""
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE url_state (
            product_id TEXT, url TEXT, normalized_url TEXT, doc_version_id TEXT,
            last_error_kind TEXT
        );
        CREATE TABLE document_version (
            doc_version_id TEXT, product_id TEXT, url TEXT, status TEXT
        );
        CREATE TABLE container_state (
            product_id TEXT, url TEXT, normalized_url TEXT, status TEXT
        );
        """
    )
    for url, pointer, kind in rows:
        conn.execute(
            "INSERT INTO url_state VALUES ('p1', ?, ?, ?, ?)", (url, url, pointer, kind)
        )
    conn.execute(
        "INSERT INTO document_version VALUES ('dv_ok', 'p1', ?, 'active')", (KNOWN,)
    )
    conn.execute(
        "INSERT INTO container_state VALUES ('p1', ?, ?, 'descended')",
        ("https://p1.test/docs/bundle.zip",) * 2,
    )
    conn.commit()
    conn.close()
    return db


def _one(package: Path, db: Path) -> Census:
    reports = census(package, db, [])
    assert len(reports) == 1
    return reports[0]


def test_declared_scopes_reads_roots_and_profile_hosts(tmp_path: Path) -> None:
    """Корни и допущенные поддомены — из пакета, тем же правилом, что у краула."""
    package = _package(tmp_path, roots=[ROOT])
    (package / "domain_profiles.yaml").write_text("cdn.p1.test: {}\n", encoding="utf-8")

    roots, scope_hosts = declared_scopes(package)

    assert roots == {"p1": [ROOT]}
    assert "cdn.p1.test" in scope_hosts["p1"]
    assert "p1.test" not in scope_hosts["p1"], "свой host корня — не «поддомен-допуск»"


def test_census_separates_outside_space_from_missing_version(tmp_path: Path) -> None:
    """URL выведенного корня — ``outside_space``, а не «переиспользовать нечего».

    Это и есть измеренный дефект: строка ``url_state`` живёт вечно, а корень можно
    вывести из пакета; под сетью такие URL отсекает ``Frontier``, под политикой без сети
    они возвращались в корпус (163 документа `edu.postgrespro.ru`, 23.11.2026).
    """
    package = _package(tmp_path, roots=[ROOT])
    db = _state(
        tmp_path,
        [
            (KNOWN, "dv_ok", None),
            (RETIRED, None, None),
            ("https://p1.test/docs/lost.html", None, None),
        ],
    )

    report = _one(package, db)

    assert report.counts["has_version"] == 1
    assert report.counts["outside_space"] == 1
    assert report.counts["no_version"] == 1, "только это — недостача подложки"
    assert report.examples["outside_space"] == [RETIRED]


def test_census_separates_refusal_from_missing_version(tmp_path: Path) -> None:
    """``robots_disallow``/``source_unavailable`` — ответ подложки, а не её пробел."""
    package = _package(tmp_path, roots=[ROOT])
    db = _state(
        tmp_path,
        [
            ("https://p1.test/docs/a.html", None, "robots_disallow"),
            ("https://p1.test/docs/b.html", None, "source_unavailable"),
        ],
    )

    report = _one(package, db)

    assert report.counts["refused"] == 2
    assert report.counts["no_version"] == 0
    assert report.refused_by_kind == {"robots_disallow": 1, "source_unavailable": 1}


def test_census_separates_provided_and_container(tmp_path: Path) -> None:
    """Объявленная подача и контейнер — ожидаемые отсутствия версии."""
    provided = "https://p1.test/docs/provided.pdf"
    package = _package(tmp_path, roots=[ROOT], provided=[provided])
    db = _state(
        tmp_path,
        [
            (provided, None, None),
            ("https://p1.test/docs/bundle.zip", None, None),
        ],
    )

    report = _one(package, db)

    assert report.counts["provided"] == 1
    assert report.counts["container"] == 1
    assert report.counts["no_version"] == 0


def test_census_names_unlinked_version_as_substrate_defect(tmp_path: Path) -> None:
    """Версия есть, а указатель пуст — дефект подложки, а не «версии нет»."""
    package = _package(tmp_path, roots=[ROOT])
    db = _state(tmp_path, [(KNOWN, None, None)])

    report = _one(package, db)

    assert report.counts["pointer_missing"] == 1
    assert report.counts["no_version"] == 0


def test_census_marks_superseded_version_separately(tmp_path: Path) -> None:
    """Вытесненная версия не переиспользуется, но и не «отсутствует»."""
    package = _package(tmp_path, roots=[ROOT])
    db = _state(tmp_path, [("https://p1.test/docs/old.html", "dv_old", None)])
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO document_version VALUES ('dv_old', 'p1', ?, 'superseded')",
        ("https://p1.test/docs/old.html",),
    )
    conn.commit()
    conn.close()

    report = _one(package, db)

    assert report.counts["superseded"] == 1
    assert report.counts["no_version"] == 0


def test_classify_does_not_require_network_validation(tmp_path: Path) -> None:
    """Пространство определяется host+prefix; SSRF — гейт фетча, не объявления.

    Проверка нужна потому, что от неё зависит цена прибора: ``validate_url`` на
    36 630 адресах занимал 10 минут (замер 23.11.2026), тогда как host+prefix — 10 с.
    Адрес, не прошедший SSRF, в ``url_state`` не попадал: запись делается после допуска.
    """
    package = _package(tmp_path, roots=[ROOT])
    _, scope_hosts = declared_scopes(package)
    from core.pipeline.crawl_frontier import CrawlLimits, Frontier

    frontier = Frontier(ROOT, CrawlLimits(), scope_hosts=scope_hosts["p1"])

    assert classify(
        url=KNOWN,
        pointer="dv_ok",
        kind=None,
        frontiers=[frontier],
        host_cache={},
        provided=frozenset(),
        containers=frozenset(),
        version_status={"dv_ok": "active"},
        active_urls=set(),
        product_id="p1",
    ) == ("has_version", "")


@pytest.mark.parametrize("url", ["https://p1.test/docs/guide.html"])
def test_census_is_read_only(tmp_path: Path, url: str) -> None:
    """Прибор ничего не пишет: подложка открывается ``mode=ro``."""
    package = _package(tmp_path, roots=[ROOT])
    db = _state(tmp_path, [(url, "dv_ok", None)])
    before = db.stat().st_mtime_ns

    _one(package, db)

    assert db.stat().st_mtime_ns == before
