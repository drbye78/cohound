"""Тесты CLI-команды ``hound package crawl-probe`` (без сети: порты инъектируются).

Покрыты: proposal-only по умолчанию (файл пакета не меняется), ``--write`` с сохранением
шапки байт-в-байт и неизмеренных полей, ``EXIT_USAGE`` без ``--actor``/на битый вход,
приоритет источников URL (``--urls`` > ``corpus_docs`` > synth), ``--only``, неизвестный
ключ, ``--no-browser``. Прогон — через :func:`core.cli.main.main` с инъекцией
:class:`~core.pipeline.crawl_probe_driver.ProbePorts` (тестовый шов).
"""

from __future__ import annotations

import json
from pathlib import Path

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.domain.enums import FetchMode
from core.pipeline.crawl_probe import RobotsGate
from core.pipeline.crawl_probe_driver import ProbePorts
from core.store.sqlite import connect
from tests.fakes.crawl_probe import ScriptedBrowserCrawler, ScriptedHttpCrawler, page
from tests.fakes.ports import FakeExtractor

MANIFEST = (
    "case_id: demo\n"
    "package_version: 1.0.0\n"
    "archetype: B\n"
    "modules:\n"
    "- module_id: core\n"
    "  weight: 100.0\n"
    "roles: []\n"
    "baselines: []\n"
    "baseline_participant: null\n"
)

HEADER = "# Обоснование (живые замеры) — шапка.\n# вторая строка шапки\n\n"

PROFILES = (
    HEADER
    + "alpha.test:\n"
    "  fetch: browser\n"
    "  extract: html\n"
    "  strategy: honest\n"
    "  wait_for: \"() => document.body && 1\"\n"
    "beta.test:\n"
    "  fetch: http\n"
)


def _package(tmp_path: Path, profiles: str = PROFILES) -> Path:
    """Пишет минимальный пакет с заданной ``domain_profiles.yaml``."""
    pkg = tmp_path / "demo"
    pkg.mkdir()
    (pkg / "package.yaml").write_text(MANIFEST, encoding="utf-8")
    (pkg / "domain_profiles.yaml").write_text(profiles, encoding="utf-8")
    return pkg


def _ports(http: ScriptedHttpCrawler, browser: ScriptedBrowserCrawler | None = None) -> ProbePorts:
    """Порты драйвера с фейковым экстрактором, робот-разрешением и без сна."""
    return ProbePorts(
        http=http,
        browser=browser,
        extractor=FakeExtractor(),
        robots=RobotsGate(lambda _host: ""),
        sleep=lambda _seconds: None,
    )


def _run(capsys, argv: list[str], ports: ProbePorts) -> tuple[int, dict, str]:
    """Запускает CLI, читает JSON-payload и stderr."""
    code = main(argv, crawl_probe_ports=ports)
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1]) if captured.out.strip() else {}
    return code, payload, captured.err


def _argv(pkg: Path, out: Path, *extra: str) -> list[str]:
    """Базовый argv ``crawl-probe`` (JSON, с ``--out``)."""
    return [
        "package",
        "crawl-probe",
        "--package",
        str(pkg),
        "--out",
        str(out),
        "--json",
        *extra,
    ]


# ---------------------------------------------------------------------------
# Proposal-only по умолчанию
# ---------------------------------------------------------------------------


def test_default_is_proposal_only_and_profiles_unchanged(
    capsys, tmp_path: Path
) -> None:
    """Без ``--write`` файл пакета не меняется, proposal пишется, unresolved — в payload."""
    pkg = _package(tmp_path)
    before = (pkg / "domain_profiles.yaml").read_bytes()
    out = tmp_path / "probe"
    http = ScriptedHttpCrawler(
        {
            "https://alpha.test/": page("https://alpha.test/", status_code=403),
            "https://beta.test/": page("https://beta.test/", status_code=200),
        }
    )
    browser = ScriptedBrowserCrawler(
        honest={
            "https://alpha.test/": page(
                "https://alpha.test/", fetch_mode=FetchMode.BROWSER, status_code=None
            )
        }
    )
    code, payload, _ = _run(
        capsys,
        _argv(pkg, out, "--only", "alpha.test", "--only", "beta.test", "--only", "ghost.test"),
        _ports(http, browser),
    )
    assert code == EXIT_OK
    assert payload["resolved"] == 2
    assert {item["key"] for item in payload["unresolved"]} == {"ghost.test"}
    assert (pkg / "domain_profiles.yaml").read_bytes() == before
    proposals = list(out.glob("*.proposal.yaml"))
    assert len(proposals) == 1
    assert str(proposals[0]) == payload["proposal_path"]


def test_write_requires_actor(capsys, tmp_path: Path) -> None:
    """``--write`` без ``--actor`` → ``EXIT_USAGE`` (ничего не пишется)."""
    pkg = _package(tmp_path)
    out = tmp_path / "probe"
    before = (pkg / "domain_profiles.yaml").read_bytes()
    code, _, err = _run(capsys, _argv(pkg, out, "--write"), _ports(ScriptedHttpCrawler()))
    assert code == EXIT_USAGE
    assert (pkg / "domain_profiles.yaml").read_bytes() == before
    assert not out.exists()


def test_write_preserves_header_and_untouched_fields(capsys, tmp_path: Path) -> None:
    """⚠ ``--write`` сохраняет шапку байт-в-байт и существующий ``wait_for``."""
    pkg = _package(tmp_path)
    out = tmp_path / "probe"
    http = ScriptedHttpCrawler(
        {
            "https://alpha.test/": page("https://alpha.test/", status_code=403),
            "https://beta.test/": page("https://beta.test/", status_code=200),
        }
    )
    browser = ScriptedBrowserCrawler(
        hybrid={
            "https://alpha.test/": page(
                "https://alpha.test/", fetch_mode=FetchMode.HTTP, status_code=200
            )
        }
    )
    code, _, _ = _run(
        capsys,
        _argv(pkg, out, "--write", "--actor", "tester", "--only", "alpha.test", "--only", "beta.test"),
        _ports(http, browser),
    )
    assert code == EXIT_OK
    updated = (pkg / "domain_profiles.yaml").read_text(encoding="utf-8")
    assert updated.startswith(HEADER)  # шапка байт-в-байт
    assert "Обоснование (живые замеры)" in updated
    assert "вторая строка шапки" in updated
    assert "wait_for" in updated  # неизмеренное поле сохранено
    assert "version" not in updated
    # Файл обязан остаться читаемым строгим загрузчиком.
    from core.config.domain_profiles import load_domain_profiles

    loaded = load_domain_profiles(pkg / "domain_profiles.yaml")
    assert loaded["beta.test"].fetch.value == "http"


# ---------------------------------------------------------------------------
# Приоритет источников URL и --only
# ---------------------------------------------------------------------------


def _three_key_package(tmp_path: Path) -> Path:
    return _package(
        tmp_path,
        HEADER + "a.test:\n  fetch: http\nb.test:\n  fetch: http\nc.test:\n  fetch: http\n",
    )


def _corpus_db(tmp_path: Path) -> Path:
    """Создаёт read-only БД прогона с одной строкой ``corpus_docs`` для ``b.test``."""
    db = tmp_path / "run.db"
    conn = connect(db)
    try:
        conn.execute(
            "INSERT INTO corpus_docs (doc_id, product_id, url) VALUES (?, ?, ?)",
            ("d1", "p1", "https://b.test/from-corpus"),
        )
        conn.commit()
    finally:
        conn.close()
    return db


def test_url_source_precedence_and_only(capsys, tmp_path: Path) -> None:
    """⚠ ``--urls`` > corpus_docs > synth; synth ⇒ source=synthesized + confidence=low; ``--only`` сужает."""
    pkg = _three_key_package(tmp_path)
    out = tmp_path / "probe"
    urls = tmp_path / "urls.yaml"
    urls.write_text("a.test:\n- https://a.test/custom/\n", encoding="utf-8")
    db = _corpus_db(tmp_path)
    http = ScriptedHttpCrawler(
        {
            "https://a.test/custom/": page("https://a.test/custom/"),
            "https://b.test/from-corpus": page("https://b.test/from-corpus"),
            "https://c.test/": page("https://c.test/"),
        }
    )
    code, payload, _ = _run(
        capsys,
        _argv(pkg, out, "--urls", str(urls), "--corpus-db", str(db)),
        _ports(http),
    )
    assert code == EXIT_OK
    by_key = {v["key"]: v for v in payload["verdicts"]}
    assert by_key["a.test"]["source"] == "urls"
    assert by_key["a.test"]["url"] == "https://a.test/custom/"
    assert by_key["b.test"]["source"] == "corpus"
    assert by_key["c.test"]["source"] == "synthesized"
    assert by_key["c.test"]["confidence"] == "low"
    assert by_key["c.test"]["observations"][0]["source"] == "synthesized"
    assert by_key["c.test"]["observations"][0]["confidence"] == "low"

    # --only сужает множество ключей.
    code2, payload2, _ = _run(capsys, _argv(pkg, out, "--only", "a.test"), _ports(http))
    assert code2 == EXIT_OK
    assert {v["key"] for v in payload2["verdicts"]} == {"a.test"}


def test_unknown_only_key_is_unresolved(capsys, tmp_path: Path) -> None:
    """Неизвестный ``--only`` ключ → ``unresolved`` ``unknown_key`` (не пропадает)."""
    pkg = _package(tmp_path)
    out = tmp_path / "probe"
    http = ScriptedHttpCrawler()
    code, payload, _ = _run(
        capsys, _argv(pkg, out, "--only", "ghost.test"), _ports(http)
    )
    assert code == EXIT_OK
    assert payload["unresolved"][0]["key"] == "ghost.test"
    assert payload["unresolved"][0]["reason"] == ["unknown_key"]


def test_no_browser_disables_escalation(capsys, tmp_path: Path) -> None:
    """``--no-browser``: HTTP не дал контента → ``unresolved`` ``browser_disabled``."""
    pkg = _package(tmp_path)
    out = tmp_path / "probe"
    http = ScriptedHttpCrawler()  # любой URL → CrawlError
    code, payload, _ = _run(
        capsys, _argv(pkg, out, "--no-browser", "--only", "beta.test"), _ports(http)
    )
    assert code == EXIT_OK
    assert payload["unresolved"][0]["reason"] == ["browser_disabled"]


# ---------------------------------------------------------------------------
# Ошибки входа
# ---------------------------------------------------------------------------


def test_missing_package_is_usage(capsys, tmp_path: Path) -> None:
    """Несуществующий пакет → ``EXIT_USAGE``."""
    code, payload, _ = _run(
        capsys, _argv(tmp_path / "nope", tmp_path / "probe"), _ports(ScriptedHttpCrawler())
    )
    assert code == EXIT_USAGE
    assert "package.yaml" in payload["message"]


def test_bad_urls_file_is_usage(capsys, tmp_path: Path) -> None:
    """``--urls`` с невалидным значением → ``EXIT_USAGE``."""
    pkg = _package(tmp_path)
    out = tmp_path / "probe"
    bad = tmp_path / "bad.yaml"
    bad.write_text("alpha.test:\n  nested: true\n", encoding="utf-8")
    code, _, _ = _run(
        capsys, _argv(pkg, out, "--urls", str(bad)), _ports(ScriptedHttpCrawler())
    )
    assert code == EXIT_USAGE


def test_missing_corpus_db_is_usage(capsys, tmp_path: Path) -> None:
    """Несуществующий ``--corpus-db`` → ``EXIT_USAGE`` (предпосылка названа явно)."""
    pkg = _package(tmp_path)
    out = tmp_path / "probe"
    code, _, _ = _run(
        capsys,
        _argv(pkg, out, "--corpus-db", str(tmp_path / "absent.db")),
        _ports(ScriptedHttpCrawler()),
    )
    assert code == EXIT_USAGE


def test_no_domain_profiles_is_usage(capsys, tmp_path: Path) -> None:
    """Пакет без ``domain_profiles.yaml`` → ``EXIT_USAGE``."""
    pkg = tmp_path / "empty"
    pkg.mkdir()
    (pkg / "package.yaml").write_text(MANIFEST, encoding="utf-8")
    code, _, _ = _run(
        capsys, _argv(pkg, tmp_path / "probe"), _ports(ScriptedHttpCrawler())
    )
    assert code == EXIT_USAGE


# ---------------------------------------------------------------------------
# --write: нулевой diff и дословность неизменённых профилей
# ---------------------------------------------------------------------------


def test_write_without_semantic_change_is_noop(capsys, tmp_path: Path) -> None:
    """⚠ ``--write`` без семантических изменений: файл байт-в-байт, ``changed: false``."""
    profiles = HEADER + "gamma.test:\n  fetch: http\n  extract: html\n"
    pkg = _package(tmp_path, profiles)
    before = (pkg / "domain_profiles.yaml").read_bytes()
    out = tmp_path / "probe"
    http = ScriptedHttpCrawler(
        {"https://gamma.test/": page("https://gamma.test/", status_code=200)}
    )
    code, payload, _ = _run(
        capsys,
        _argv(pkg, out, "--write", "--actor", "tester", "--only", "gamma.test"),
        _ports(http),
    )
    assert code == EXIT_OK
    assert payload["changed"] is False
    assert (pkg / "domain_profiles.yaml").read_bytes() == before


def test_write_changes_only_measured_key_and_keeps_others_verbatim(
    capsys, tmp_path: Path
) -> None:
    """⚠ Один ключ меняется; неизменённый профиль сохраняется дословно (кавычки целы)."""
    profiles = (
        HEADER
        + "alpha.test:\n  fetch: http\n  extract: html\n"
        + "beta.test:\n  fetch: browser\n  extract: html\n  strategy: honest\n"
        + '  wait_for: "() => document.body && 1"\n'
    )
    pkg = _package(tmp_path, profiles)
    out = tmp_path / "probe"
    http = ScriptedHttpCrawler(
        {"https://alpha.test/": page("https://alpha.test/", status_code=403)}
    )
    browser = ScriptedBrowserCrawler(
        honest={
            "https://alpha.test/": page(
                "https://alpha.test/", fetch_mode=FetchMode.BROWSER, status_code=None
            )
        }
    )
    code, payload, _ = _run(
        capsys,
        _argv(pkg, out, "--write", "--actor", "tester", "--only", "alpha.test"),
        _ports(http, browser),
    )
    assert code == EXIT_OK
    assert payload["changed"] is True
    updated = (pkg / "domain_profiles.yaml").read_text(encoding="utf-8")
    assert updated.startswith(HEADER)  # шапка байт-в-байт
    # Неизменённый beta — дословно, включая авторские кавычки wait_for.
    assert (
        "beta.test:\n"
        "  fetch: browser\n"
        "  extract: html\n"
        "  strategy: honest\n"
        '  wait_for: "() => document.body && 1"\n'
    ) in updated
    # Изменённый alpha — только измеренные оси, порядок модели fetch → extract → strategy.
    assert (
        "alpha.test:\n"
        "  fetch: browser\n"
        "  extract: html\n"
        "  strategy: honest\n"
    ) in updated
    from core.config.domain_profiles import load_domain_profiles

    loaded = load_domain_profiles(pkg / "domain_profiles.yaml")
    assert loaded["alpha.test"].fetch is FetchMode.BROWSER
    assert loaded["beta.test"].fetch is FetchMode.BROWSER


# ---------------------------------------------------------------------------
# Авторское ``wait_for``: сохраняется, не понижается, синтез условия запрещён
# ---------------------------------------------------------------------------


def test_write_preserves_authored_wait_for_and_never_synthesizes(
    capsys, tmp_path: Path
) -> None:
    """⚠ ``--write`` сохраняет авторское ``wait_for`` и не синтезирует условие для других.

    ``alpha.test`` несёт авторское условие и резолвится браузером **с** ним
    (``wait_for_confirmed``); ``beta.test`` без условия остаётся честной заглушкой
    (``wait_for_recommended``), но условие для него **не** выдумывается.
    """
    profiles = (
        HEADER
        + "alpha.test:\n  fetch: http\n  extract: html\n  strategy: honest\n"
        + '  wait_for: "() => document.body && 1"\n'
        + "beta.test:\n  fetch: http\n"
    )
    pkg = _package(tmp_path, profiles)
    out = tmp_path / "probe"
    alpha_url = "https://alpha.test/"
    beta_url = "https://beta.test/"
    http = ScriptedHttpCrawler(
        {
            alpha_url: page(alpha_url, status_code=403),
            beta_url: page(beta_url, status_code=403),
        }
    )
    stub = page(
        beta_url, fetch_mode=FetchMode.BROWSER, status_code=None, content=b"<html>stub</html>"
    )
    browser = ScriptedBrowserCrawler(
        hybrid={beta_url: stub},
        honest={
            alpha_url: page(alpha_url, fetch_mode=FetchMode.BROWSER, status_code=None),
            beta_url: stub,
        },
    )
    code, payload, _ = _run(
        capsys,
        _argv(
            pkg,
            out,
            "--write",
            "--actor",
            "tester",
            "--only",
            "alpha.test",
            "--only",
            "beta.test",
        ),
        _ports(http, browser),
    )
    assert code == EXIT_OK
    assert payload["changed"] is True
    by_key = {v["key"]: v for v in payload["verdicts"]}
    assert by_key["alpha.test"]["fetch"] == "browser"
    assert by_key["alpha.test"]["wait_for_confirmed"] is True
    assert by_key["beta.test"]["status"] == "unresolved"
    assert by_key["beta.test"]["wait_for_recommended"] is True
    # Артефакт показывает применение условия у наблюдения (browser+wait_for).
    proposal_text = Path(payload["proposal_path"]).read_text(encoding="utf-8")
    assert "wait_for_applied: true" in proposal_text

    from core.config.domain_profiles import load_domain_profiles

    loaded = load_domain_profiles(pkg / "domain_profiles.yaml")
    assert loaded["alpha.test"].fetch is FetchMode.BROWSER
    assert loaded["alpha.test"].wait_for == "() => document.body && 1"  # не снято/не понижено
    assert loaded["beta.test"].wait_for is None  # синтеза нет
    updated = (pkg / "domain_profiles.yaml").read_text(encoding="utf-8")
    assert updated.count("wait_for") == 1  # ровно одно — авторское
