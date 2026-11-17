"""План 27 §5: CLI ``hound baseline`` — read-only команды и запись через ``apply_log``.

Проверки держат контракт группы: ``--actor`` обязателен для записи, ``--dry-run`` ничего
не пишет, ``recon`` отказывает на пустом reference-корпусе вместо «фактов нет», а ни одна
подкоманда не трогает ``runs/``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.cli.errors import CliError
from core.cli.parser import build_parser
from core.pipeline.reference_products import load_package_facts

REFERENCE_YAML = """\
schema: reference_products/1
products:
  - baseline_id: upstream-pg
    product_id: postgresql
    canonical_name: PostgreSQL
    versions:
      - version: "18"
        docs_roots: ["https://www.postgresql.org/docs/18/"]
    current_version: "18"
"""

FACTS_CSV = (
    "baseline_id,param_id,version,status,url,quote,content_hash,observed_at,"
    "corpus_build_id,review,reviewed_by,note\n"
    "upstream-pg,p1,18,present,https://www.postgresql.org/docs/18/a.html,"
    '"the database supports jsonb storage with expression indexing",'
    f'{"a" * 64},2026-11-17T00:00:00+00:00,ref_postgresql_18_deadbeef,pending,,\n'
)


def _package(tmp_path: Path, *, reference: bool = True, facts: bool = True) -> Path:
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "baselines.csv").write_text(
        "baseline_id,param_id,min_version,inheritance_mode,note\n"
        "upstream-pg,p1,18,direct,\n",
        encoding="utf-8",
    )
    (package / "catalog.csv").write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        'p1,JSONB indexing,m,g,Кр,fact,"supports jsonb storage with expression indexing",'
        "explicit,,ru=jsonb|индекс,ru|en\n",
        encoding="utf-8",
    )
    if reference:
        (package / "sources" / "reference_products.yaml").write_text(
            REFERENCE_YAML, encoding="utf-8"
        )
    if facts:
        (package / "sources" / "baseline_facts.csv").write_text(FACTS_CSV, encoding="utf-8")
    return package


def _run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> dict:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = args.handler
    assert handler is not None
    code = handler(args)
    out = capsys.readouterr().out
    assert code == 0, out
    return json.loads(out)


def test_list_computes_role_without_cohort(tmp_path: Path, capsys) -> None:
    package = _package(tmp_path, facts=False)
    payload = _run(["baseline", "list", "--package", str(package), "--json"], capsys)
    assert payload["declared"] is True
    assert payload["products"]["upstream-pg"]["current_version"] == "18"
    # Паспортов не передали — роль не выдумывается: предков нет, значит parallel.
    assert payload["products"]["upstream-pg"]["role"] == "parallel"


def test_facts_filters_by_review(tmp_path: Path, capsys) -> None:
    package = _package(tmp_path)
    pending = _run(
        ["baseline", "facts", "--package", str(package), "--review", "pending", "--json"],
        capsys,
    )
    assert pending["count"] == 1
    accepted = _run(
        ["baseline", "facts", "--package", str(package), "--review", "accepted", "--json"],
        capsys,
    )
    assert accepted["count"] == 0


def test_review_requires_actor(tmp_path: Path) -> None:
    package = _package(tmp_path)
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["baseline", "review", "--package", str(package), "--baseline", "upstream-pg",
             "--accept"]
        )


def test_review_accept_then_report_counts_it(tmp_path: Path, capsys) -> None:
    package = _package(tmp_path)
    _run(
        ["baseline", "review", "--package", str(package), "--baseline", "upstream-pg",
         "--actor", "roger", "--accept", "--json"],
        capsys,
    )
    assert load_package_facts(package)[0].review.value == "accepted"
    report = _run(
        ["baseline", "report", "--package", str(package), "--json"], capsys
    )
    assert report["facts"]["accepted"] == 1


def test_report_does_not_touch_runs(tmp_path: Path, capsys, monkeypatch) -> None:
    package = _package(tmp_path)
    monkeypatch.chdir(tmp_path)
    _run(["baseline", "report", "--package", str(package), "--json"], capsys)
    assert not (tmp_path / "runs").exists()


def test_recon_refuses_empty_corpus(tmp_path: Path) -> None:
    """Пустой reference-корпус — отказ, а не «фактов нет» (§8.1)."""
    package = _package(tmp_path, facts=False)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--corpus-db", str(tmp_path / "missing.db")]
    )
    with pytest.raises(CliError, match="reference-корпус не найден"):
        args.handler(args)


def test_recon_writes_pending_facts_from_corpus_db(tmp_path: Path, capsys) -> None:

    from core.store.sqlite import connect

    package = _package(tmp_path, facts=False)
    db_path = tmp_path / "reference.db"
    conn = connect(db_path)
    conn.execute(
        "INSERT INTO corpus_docs (doc_id, product_id, url, content_hash, fetched_at, "
        "extractor_version) VALUES ('d1','postgresql','https://x','h','2026-11-17',"
        "'trafilatura-html-7')"
    )
    conn.execute(
        "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, url, chunk_no, text, "
        "content_hash, build_id) VALUES ('c1','d1','postgresql','https://x',0,"
        "'The database supports jsonb storage with expression indexing.','a',"
        "'ref_postgresql_18_deadbeef')"
    )
    conn.commit()
    conn.close()
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--corpus-db", str(db_path), "--json"],
        capsys,
    )
    assert payload["chunks"] == 1
    assert payload["present"] == 1
    stored = load_package_facts(package)
    assert stored[0].review.value == "pending"
    assert stored[0].quote and stored[0].quote in "The database supports jsonb storage with expression indexing."


def test_refresh_reports_diff_and_writes_nothing(tmp_path: Path, capsys) -> None:
    package = _package(tmp_path)
    before = (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8")
    payload = _run(
        ["baseline", "refresh", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    assert payload["dry_run"] is True
    assert (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8") == before
    assert "baselines.csv" in payload["note"]


def test_baseline_group_without_subcommand_is_usage_error() -> None:
    parser = build_parser()
    args = parser.parse_args(["baseline"])
    with pytest.raises(CliError, match="нужна подкоманда"):
        args.handler(args)


# ---------------------------------------------------------------------------
# map / report --with-map (этап 9 плана 27)
# ---------------------------------------------------------------------------


def _package_with_manifest(tmp_path: Path) -> Path:
    """Пакет с ``package.yaml``: карте нужны объявленные веса модулей."""
    package = _package(tmp_path)
    (package / "package.yaml").write_text(
        "case_id: pkg\n"
        'package_version: "1.0.0"\n'
        "archetype: A\n"
        "modules:\n"
        "  - module_id: m\n"
        "    weight: 100\n"
        "roles:\n"
        "  - role_id: r\n",
        encoding="utf-8",
    )
    return package


def _reference_db(tmp_path: Path) -> Path:
    """Reference-корпус на один документ: одна страница, один чанк."""
    from core.store.sqlite import connect

    db_path = tmp_path / "reference.db"
    conn = connect(db_path)
    conn.execute(
        "INSERT INTO corpus_docs (doc_id, product_id, url, content_hash, fetched_at, "
        "extractor_version) VALUES ('d1','postgresql','https://x','h','2026-11-17',"
        "'trafilatura-html-7')"
    )
    conn.execute(
        "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, url, chunk_no, text, "
        "content_hash, build_id) VALUES ('c1','d1','postgresql','https://x',0,"
        "'The database supports jsonb storage with expression indexing.','a',"
        "'ref_postgresql_18_deadbeef')"
    )
    conn.commit()
    conn.close()
    return db_path


class _MapEvaluator:
    """Фейковый оценщик карты: `✅` с цитатой, если промпт говорит про jsonb."""

    model_id = "fake@rev1"

    def complete(self, req):  # noqa: ANN001 — шов порта
        from core.domain.enums import Status
        from core.domain.models import LLMCellOutput, LLMEvidence
        from core.ports.llm import LLMResponse, TokenUsage

        if "jsonb" in req.prompt.lower():
            out = LLMCellOutput(
                status=Status.OK,
                justification="описано",
                evidence=[LLMEvidence(source_id="c1", quote="supports jsonb storage")],
            )
        else:
            out = LLMCellOutput(
                status=Status.UNKNOWN,
                justification="не установлено",
                denial_reason="нет дословного утверждения",
            )
        return LLMResponse(
            output=out, raw="{}", prompt_hash="ph", decoder="none", usage=TokenUsage()
        )


def test_map_writes_artifacts_outside_runs(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    db_path = _reference_db(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--corpus-db", str(db_path), "--json",
        ]
    )
    args.evaluator = _MapEvaluator()
    assert args.handler(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["counts"] == {"present": 1}
    assert payload["map_id"].startswith("map_")
    assert set(payload["paths"]) == {"cells", "manifest", "score"}
    for path in payload["paths"].values():
        assert Path(path).is_file()
        assert "/runs/" not in path
    assert not (tmp_path / "runs").exists()


def test_map_dry_run_writes_nothing(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    db_path = _reference_db(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--corpus-db", str(db_path), "--dry-run", "--json",
        ]
    )
    args.evaluator = _MapEvaluator()
    assert args.handler(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["paths"] == {}
    assert not (tmp_path / "reference").exists()


def test_report_with_map_publishes_agreement(tmp_path: Path, capsys, monkeypatch) -> None:
    """Согласие факт↔карта публикуется числами: κ, n и расхождения (§9.5)."""
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    db_path = _reference_db(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--corpus-db", str(db_path), "--json",
        ]
    )
    args.evaluator = _MapEvaluator()
    assert args.handler(args) == 0
    capsys.readouterr()

    report_args = parser.parse_args(
        [
            "baseline", "report", "--package", str(package), "--with-map", "--json",
        ]
    )
    assert report_args.handler(report_args) == 0
    payload = json.loads(capsys.readouterr().out)
    section = payload["map"]
    assert section["map_id"].startswith("map_")
    agreement = section["fact_map_agreement"]
    # Факт `present`, карта `✅` — пара входит в согласие и расхождением не является.
    assert agreement["n"] == 1
    assert agreement["disagreements"] == []
    # Одна пара одинаковых меток: маргиналы вырождены, κ не выдумывается (§9.5, A3).
    assert agreement["kappa"] is None
    assert agreement["undefined_reason"]


def test_report_without_map_says_so(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "report", "--package", str(package), "--with-map", "--json"]
    )
    assert args.handler(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["map"]["map_id"] is None
    assert "hound baseline map" in payload["map"]["note"]


def test_report_with_map_refuses_to_guess_between_maps(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Две карты и нет ``--map`` — отказ, а не «последняя по времени» (A3)."""
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    maps_dir = tmp_path / "reference" / "pkg" / "postgresql" / "18" / "maps"
    for name in ("map_a", "map_b"):
        (maps_dir / name).mkdir(parents=True)
        (maps_dir / name / "manifest.json").write_text(
            json.dumps(
                {
                    "schema": "reference-map/1",
                    "map_id": name,
                    "reference_build_id": "ref_x",
                    "product_id": "postgresql",
                    "version": "18",
                    "params": 1,
                    "counts": {},
                }
            ),
            encoding="utf-8",
        )
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "report", "--package", str(package), "--with-map", "--json"]
    )
    with pytest.raises(CliError, match="--map"):
        args.handler(args)
