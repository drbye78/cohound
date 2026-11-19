"""План 27 §5: CLI ``hound baseline`` — read-only команды и запись через ``apply_log``.

Проверки держат контракт группы: ``--actor`` обязателен для записи, ``--dry-run`` ничего
не пишет, ``recon`` отказывает на пустом reference-корпусе вместо «фактов нет», а ни одна
подкоманда не трогает ``runs/``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.cli.errors import EXIT_USAGE, CliError
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


def _corpus_db(tmp_path: Path, text: str) -> Path:
    """Reference-БД с одним чанком: минимальный носитель для разведки."""
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
        "content_hash, build_id) VALUES ('c1','d1','postgresql','https://x',0,?, 'a', "
        "'ref_postgresql_18_deadbeef')",
        (text,),
    )
    conn.commit()
    conn.close()
    return db_path


def _facts_with_accepted(package: Path, *, quote: str, version: str = "18") -> None:
    """Факт, подписанный человеком: проверяем, что пере-разведка его не съедает."""
    (package / "sources" / "baseline_facts.csv").write_text(
        "baseline_id,param_id,version,status,url,quote,content_hash,observed_at,"
        "corpus_build_id,review,reviewed_by,note\n"
        f"upstream-pg,p1,{version},present,https://www.postgresql.org/docs/18/a.html,"
        f'"{quote}",{"a" * 64},2026-11-17T00:00:00+00:00,'
        "ref_postgresql_18_deadbeef,accepted,roger,проверено вручную\n",
        encoding="utf-8",
    )


def test_recon_carries_signature_when_its_quote_is_still_in_corpus(
    tmp_path: Path, capsys
) -> None:
    """Подпись переживает пере-разведку, если её доказательство на месте (19.11.2026).

    Прибор выберет другую цитату — это не повод стирать решение человека: доказательство
    то же и оно проверяемо. Прежняя редакция отбрасывала все строки версии и молча
    обнуляла подписи.
    """
    package = _package(tmp_path)
    _facts_with_accepted(package, quote="The database supports")
    db_path = _corpus_db(
        tmp_path, "The database supports jsonb storage with expression indexing."
    )
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--corpus-db", str(db_path), "--json"],
        capsys,
    )
    assert payload["review_carried"] == [
        {"param_id": "p1", "version": "18", "review": "accepted", "reviewed_by": "roger"}
    ]
    assert payload["review_dropped"] == []
    stored = load_package_facts(package)[0]
    assert stored.review.value == "accepted"
    assert stored.reviewed_by == "roger"
    assert stored.quote == "The database supports"


def test_recon_drops_signature_whose_quote_is_gone(tmp_path: Path, capsys) -> None:
    """Цитаты в корпусе больше нет — подпись не переносится и потеря называется.

    Публиковать решение, чьё доказательство исчезло, нельзя; но и молчать об этом —
    дефект. Потеря попадает в ``review_dropped`` и печатается в текстовом выводе.
    """
    package = _package(tmp_path)
    _facts_with_accepted(package, quote="цитата, которой в корпусе нет")
    db_path = _corpus_db(
        tmp_path, "The database supports jsonb storage with expression indexing."
    )
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--corpus-db", str(db_path), "--json"],
        capsys,
    )
    assert payload["review_carried"] == []
    assert payload["review_dropped"] == [
        {
            "param_id": "p1",
            "version": "18",
            "review": "accepted",
            "reviewed_by": "roger",
            "reason": "quote_gone",
        }
    ]
    stored = load_package_facts(package)[0]
    assert stored.review.value == "pending"
    assert stored.reviewed_by is None


def test_recon_carries_signature_across_detailed_version(tmp_path: Path, capsys) -> None:
    """Факт записан подробной версией (``18.4.1``), сборка адресована мажором (``18``).

    Сравнение строками пропустило бы такую подпись, и она потерялась бы молча — ровно
    тот дефект, который закрывается. Сравнение идёт по мажору, как и в наследовании.
    """
    package = _package(tmp_path)
    _facts_with_accepted(package, quote="The database supports", version="18.4.1")
    db_path = _corpus_db(
        tmp_path, "The database supports jsonb storage with expression indexing."
    )
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--corpus-db", str(db_path), "--json"],
        capsys,
    )
    assert len(payload["review_carried"]) == 1
    assert payload["review_dropped"] == []
    # Строка одна, а не две: подробная версия факта и мажор сборки — один и тот же ключ.
    stored = load_package_facts(package)
    assert len(stored) == 1
    assert stored[0].review.value == "accepted"
    assert stored[0].version == "18.4.1"


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


BUILD_ID = "ref_postgresql_18_deadbeef"


def _build_root(tmp_path: Path) -> Path:
    """Корень версии reference-корпуса для пакета ``pkg`` (``pkg`` — имя каталога)."""
    return tmp_path / "reference" / "pkg" / "postgresql" / "18"


def _write_build(
    tmp_path: Path,
    *,
    build_id: str = BUILD_ID,
    root: Path | None = None,
    docs: int = 1,
    chunks: int = 1,
    complete: bool = True,
    current: bool = True,
) -> str:
    """Сборка reference-корпуса: каталог ``builds/{build_id}/`` с манифестом.

    Манифест — то, чем сборка **существует** (``list_builds`` видит каталог только с
    ``manifest.json``), поэтому фикстура пишет его всегда: без него «сборка есть» была бы
    утверждением о каталоге, а не о проходе.
    """
    base = root if root is not None else _build_root(tmp_path)
    build_dir = base / "builds" / build_id
    build_dir.mkdir(parents=True, exist_ok=True)
    (build_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema": "reference-corpus/2",
                "build_id": build_id,
                "package_id": "pkg",
                "product_id": "postgresql",
                "version": "18",
                "content_digest": build_id.rsplit("_", 1)[-1],
                "docs": docs,
                "chunks": chunks,
                "extractor_version": "trafilatura-html-8",
                "chunker_version": "md-heading-4",
                "observed_at": "2026-11-17T00:00:00+00:00",
                "roots": ["https://x/"],
                "complete": complete,
                "incomplete_reasons": [] if complete else ["truncated:max_documents"],
                "skipped": {} if complete else {"fetch_error": 2},
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    if current:
        (base / "current.json").write_text(
            json.dumps({"build_id": build_id}), encoding="utf-8"
        )
    return build_id


def _reference_db(tmp_path: Path, *, build_id: str = BUILD_ID, root: Path | None = None) -> str:
    """Reference-корпус на один документ: одна страница, один чанк (сборка целиком)."""
    from core.store.sqlite import connect

    base = root if root is not None else _build_root(tmp_path)
    build_dir = base / "builds" / build_id
    build_dir.mkdir(parents=True, exist_ok=True)
    db_path = build_dir / "reference.db"
    conn = connect(db_path)
    conn.execute(
        "INSERT INTO corpus_docs (doc_id, product_id, url, content_hash, fetched_at, "
        "extractor_version) VALUES ('d1','postgresql','https://x','h','2026-11-17',"
        "'trafilatura-html-8')"
    )
    conn.execute(
        "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, url, chunk_no, text, "
        "content_hash, build_id) VALUES ('c1','d1','postgresql','https://x',0,"
        "'The database supports jsonb storage with expression indexing.','a',?)",
        (build_id,),
    )
    conn.commit()
    conn.close()
    return _write_build(tmp_path, build_id=build_id, root=root)


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
    build_id = _reference_db(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--build", build_id, "--json",
        ]
    )
    args.evaluator = _MapEvaluator()
    assert args.handler(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["counts"] == {"present": 1}
    assert payload["map_id"].startswith("map_")
    assert payload["reference_build_id"] == build_id
    assert set(payload["paths"]) == {"cells", "manifest", "score"}
    for path in payload["paths"].values():
        assert Path(path).is_file()
        assert "/runs/" not in path
        # Карта живёт внутри сборки: смена модели меняет карту, но не корпус (§7.3).
        assert f"builds/{build_id}/maps/" in path
    assert not (tmp_path / "runs").exists()


def test_map_dry_run_writes_nothing(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    store = tmp_path / "refstore"
    build_id = _reference_db(tmp_path, root=store / "pkg" / "postgresql" / "18")
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--build", build_id, "--reference-dir", str(store), "--dry-run", "--json",
        ]
    )
    args.evaluator = _MapEvaluator()
    assert args.handler(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["paths"] == {}
    # Артефактов карты не появилось: ни cells.jsonl, ни score.json (§5).
    maps_dir = store / "pkg" / "postgresql" / "18" / "builds" / build_id / "maps"
    assert not maps_dir.exists() or not any(maps_dir.iterdir())
    assert not (tmp_path / "reference").exists()


def test_report_with_map_publishes_agreement(tmp_path: Path, capsys, monkeypatch) -> None:
    """Согласие факт↔карта публикуется числами: κ, n и расхождения (§9.5)."""
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    build_id = _reference_db(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--build", build_id, "--json",
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
    # Ошибка/состояние называет **следующую** команду цепочки: карта → сборка.
    assert "hound baseline map" in payload["map"]["note"]
    assert "hound baseline build" in payload["map"]["note"]


def test_report_with_map_refuses_to_guess_between_maps(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Две карты и нет ``--map`` — отказ, а не «последняя по времени» (A3)."""
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    build_id = _write_build(tmp_path)
    maps_dir = _build_root(tmp_path) / "builds" / build_id / "maps"
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


# ---------------------------------------------------------------------------
# build: сборка reference-корпуса (шаг 1 §8.1)
# ---------------------------------------------------------------------------

#: Корень документации из ``REFERENCE_YAML`` — сборка берёт корни **только** оттуда.
DOCS_ROOT = "https://www.postgresql.org/docs/18/"


class _FakePage:
    """Страница для фейкового краулера (минимум, который читает сборка)."""

    def __init__(self, url: str, content: bytes, content_type: str = "text/html") -> None:
        self.url = url
        self.content = content
        self.content_type = content_type
        self.status_code = 200
        self.fetched_at = "2026-11-17T00:00:00+00:00"
        self.fetch_mode = None
        self.lang = "en"
        self.from_cache = False


class _FakeCrawler:
    """Краулер-двойник: отдаёт заранее объявленные страницы и помнит запросы."""

    def __init__(self, pages: dict[str, _FakePage]) -> None:
        self.pages = pages
        self.fetched: list[str] = []

    def fetch(self, url: str, plan):  # noqa: ANN001 — шов порта
        self.fetched.append(url)
        if url not in self.pages:
            raise RuntimeError(f"нет страницы {url}")
        return self.pages[url]


class _FakeExtractor:
    """Экстрактор-двойник: текст = тело ответа, хеш — от текста (дедуп как у настоящего)."""

    version = "fake-extract-1"

    def extract(self, page, plan):  # noqa: ANN001 — шов порта
        from types import SimpleNamespace

        from core.util.hashing import content_hash

        text = page.content.decode("utf-8")
        return SimpleNamespace(
            text=text,
            content_hash=content_hash(text),
            error=None,
            extractor_version=self.version,
            partial=False,
            partial_reason=None,
            page_count=None,
            pages_converted=None,
            page_map=None,
            ocr_probe=None,
            pages_ocr=None,
            pages_failed=None,
            tables=None,
            blocks=None,
        )


class _FakeChunker:
    """Чанкер-двойник: один чанк на документ (нарезка здесь не предмет проверки)."""

    version = "fake-chunk-1"

    def chunk(self, extracted, doc_id: str):  # noqa: ANN001 — шов порта
        from types import SimpleNamespace

        return [
            SimpleNamespace(
                chunk_id=f"{doc_id}#0",
                chunk_no=0,
                text=extracted.text,
                content_hash=extracted.content_hash,
                heading_path=None,
                context_prefix=None,
                char_start=0,
                char_end=len(extracted.text),
                table_ref=None,
            )
        ]


class _FakeRobots:
    """Политика ``robots.txt``: ``allow=False`` запрещает всё (для проверки пропусков)."""

    def __init__(self, allow: bool = True) -> None:
        self.allow = allow

    def allowed(self, url: str) -> bool:  # noqa: ARG002 — шов порта
        return self.allow


def _run_with(argv: list[str], capsys: pytest.CaptureFixture[str], **ports) -> dict:
    """Прогон с инъекцией портов: composition root подменяется, путь команды — тот же."""
    parser = build_parser()
    args = parser.parse_args(argv)
    for name, value in ports.items():
        setattr(args, name, value)
    code = args.handler(args)
    out = capsys.readouterr().out
    assert code == 0, out
    return json.loads(out)


def _build_argv(package: Path, *extra: str) -> list[str]:
    return [
        "baseline", "build", "--package", str(package), "--baseline", "upstream-pg", *extra
    ]


def test_build_dry_run_prints_plan_and_touches_nothing(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """``--dry-run`` печатает план и не идёт в сеть: краулер-ловушка не вызывается."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    crawler = _FakeCrawler({})
    payload = _run_with(
        _build_argv(package, "--dry-run", "--json"), capsys, crawler=crawler
    )
    assert payload["dry_run"] is True
    # Корни — из декларации, а не из флагов/сети.
    assert payload["roots"] == [DOCS_ROOT]
    assert payload["limits"]["document_level"] == "full"
    assert payload["limits"]["pdf_pipeline"] == "structured"
    assert crawler.fetched == []
    assert not (tmp_path / "reference").exists()


def test_build_rate_limit_is_declared_in_canonical_form(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Темп объявляется **канонической строкой** — сравнение сборок не должно быть строковой лотереей."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    seconds = _run_with(
        _build_argv(package, "--rate-limit", "0.5", "--dry-run", "--json"), capsys
    )
    assert seconds["limits"]["rate_limit"] == "uniform(min=0.5,max=0.5)"
    declared = _run_with(
        _build_argv(package, "--rate-limit", "uniform(min=0.5,max=1.0)", "--dry-run", "--json"),
        capsys,
    )
    assert declared["limits"]["rate_limit"] == "uniform(min=0.5,max=1.0)"


def test_build_rejects_zero_documents_and_accepts_zero_depth(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """``max_documents=0`` — пустой корпус (ошибка конфигурации), ``max_depth=0`` — только корни.

    Дефолт глубины — **без ограничения** (граница пространства — объявленный корень):
    ``CrawlLimits``-овский ноль «только seeds» здесь читал бы одну страницу руководства
    и объявлял охват полным. Явный ``0`` остаётся законным объявлением «только корни».
    """
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(_build_argv(package, "--max-documents", "0"))
    default_payload = _run_with(_build_argv(package, "--dry-run", "--json"), capsys)
    assert default_payload["limits"]["max_depth"] is None
    payload = _run_with(_build_argv(package, "--max-depth", "0", "--dry-run", "--json"), capsys)
    assert payload["limits"]["max_depth"] == 0


def test_build_creates_build_and_names_next_command(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    pages = {
        DOCS_ROOT: _FakePage(
            DOCS_ROOT,
            b"<html><body><p>The database supports jsonb storage with expression "
            b"indexing.</p></body></html>",
        )
    }
    payload = _run_with(
        _build_argv(package, "--json"),
        capsys,
        crawler=_FakeCrawler(pages),
        extractor=_FakeExtractor(),
        chunker=_FakeChunker(),
        robots=_FakeRobots(),
    )
    assert payload["docs"] == 1
    assert payload["chunks"] == 1
    assert payload["complete"] is True
    assert payload["build_id"].startswith("ref_postgresql_18_")
    # Сборка стала текущей, и её манифест опубликован (§4: сборка адресуется содержимым).
    assert payload["current"] == payload["build_id"]
    assert Path(payload["manifest_path"]).is_file()
    assert "hound baseline recon" in payload["next_command"]
    assert payload["next_command"].endswith(payload["build_id"])


def test_build_prints_summary_and_next_step(tmp_path: Path, capsys, monkeypatch) -> None:
    """Человекочитаемая сводка: объёмы, охват и **следующая** команда."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    pages = {
        DOCS_ROOT: _FakePage(
            DOCS_ROOT, b"<html><body><p>The database supports jsonb storage.</p></body></html>"
        )
    }
    parser = build_parser()
    args = parser.parse_args(_build_argv(package))
    args.crawler = _FakeCrawler(pages)
    args.extractor = _FakeExtractor()
    args.chunker = _FakeChunker()
    args.robots = _FakeRobots()
    assert args.handler(args) == 0
    out = capsys.readouterr().out
    assert "документов 1, чанков 1" in out
    assert "охват полный" in out
    assert "дальше: hound baseline recon" in out


def test_build_reports_incomplete_coverage_with_reasons(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Запрет ``robots`` — неполный охват с причиной, а не молчаливый «полный»."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    payload = _run_with(
        _build_argv(package, "--json"),
        capsys,
        crawler=_FakeCrawler({DOCS_ROOT: _FakePage(DOCS_ROOT, b"<html><body>x</body></html>")}),
        extractor=_FakeExtractor(),
        chunker=_FakeChunker(),
        robots=_FakeRobots(allow=False),
    )
    assert payload["complete"] is False
    assert payload["skipped"]["robots"] == 1
    assert payload["docs"] == 0


def test_build_then_recon_reads_the_built_corpus(tmp_path: Path, capsys, monkeypatch) -> None:
    """Цепочка целиком: собранный корпус читается `recon` без указания сборки.

    Это пин контракта между двумя командами: `build` пишет `corpus_chunks` с `build_id` и
    `current.json`, `recon` находит сборку раскладкой и читает её — без ручной подстановки
    путей и без «соберите корпус руками».
    """
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    pages = {
        DOCS_ROOT: _FakePage(
            DOCS_ROOT,
            b"<html><body><p>The database supports jsonb storage with expression "
            b"indexing.</p></body></html>",
        )
    }
    built = _run_with(
        _build_argv(package, "--json"),
        capsys,
        crawler=_FakeCrawler(pages),
        extractor=_FakeExtractor(),
        chunker=_FakeChunker(),
        robots=_FakeRobots(),
    )
    recon = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg", "--json"],
        capsys,
    )
    assert recon["corpus_build_id"] == built["build_id"]
    assert recon["chunks"] == 1
    assert recon["present"] == 1
    stored = load_package_facts(package)
    assert stored[0].url == DOCS_ROOT


# ---------------------------------------------------------------------------
# recon: выбор сборки и лексический индекс (§8.1 шаги 5–6)
# ---------------------------------------------------------------------------


def _write_corpus(
    tmp_path: Path,
    build_id: str,
    chunks: list[tuple[str, str, str]],
    *,
    current: bool = False,
) -> str:
    """Сборка с заданными чанками: ``(doc_id, url, text)`` в порядке объявления."""
    from core.store.sqlite import connect

    root = _build_root(tmp_path)
    build_dir = root / "builds" / build_id
    build_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(build_dir / "reference.db")
    for doc_id, url, text in chunks:
        conn.execute(
            "INSERT INTO corpus_docs (doc_id, product_id, url, content_hash, fetched_at, "
            "extractor_version) VALUES (?, 'postgresql', ?, ?, '2026-11-17', "
            "'trafilatura-html-8')",
            (doc_id, url, f"hash-{doc_id}"),
        )
        conn.execute(
            "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, url, chunk_no, text, "
            "content_hash, build_id) VALUES (?, ?, 'postgresql', ?, 0, ?, ?, ?)",
            (f"{doc_id}#0", doc_id, url, text, f"chunk-{doc_id}", build_id),
        )
    conn.commit()
    conn.close()
    return _write_build(
        tmp_path,
        build_id=build_id,
        docs=len(chunks),
        chunks=len(chunks),
        current=current,
    )


def test_recon_uses_the_named_build(tmp_path: Path, capsys, monkeypatch) -> None:
    """``--build`` берёт именно указанную сборку, а не «единственную/текущую»."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    first = _write_corpus(
        tmp_path, "ref_a", [("d1", "https://x/a", "The database supports jsonb storage.")]
    )
    second = _write_corpus(
        tmp_path, "ref_b", [("d2", "https://x/b", "The database supports jsonb indexing.")]
    )
    payload = _run_with(
        [
            "baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
            "--build", second, "--json",
        ],
        capsys,
    )
    assert payload["corpus_build_id"] == second != first
    stored = load_package_facts(package)
    assert stored[0].url == "https://x/b"


def test_recon_refuses_two_corpus_addresses(tmp_path: Path, capsys, monkeypatch) -> None:
    """``--build`` и ``--corpus-db`` — разные адреса: молчаливое предпочтение одного запрещено."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    _write_corpus(tmp_path, "ref_a", [("d1", "https://x/a", "jsonb storage")], current=True)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
            "--build", "ref_a", "--corpus-db", str(tmp_path / "other.db"),
        ]
    )
    with pytest.raises(CliError, match="выберите один"):
        args.handler(args)


def test_recon_refuses_to_guess_between_builds(tmp_path: Path, capsys, monkeypatch) -> None:
    """Две сборки без ``current.json`` — отказ с перечнем, а не выбор по каталогу (A3)."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    _write_corpus(tmp_path, "ref_a", [("d1", "https://x/a", "jsonb storage")])
    _write_corpus(tmp_path, "ref_b", [("d2", "https://x/b", "jsonb storage")])
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg"]
    )
    with pytest.raises(CliError, match="--build"):
        args.handler(args)


def test_recon_finds_quote_by_index_not_by_corpus_order(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Регрессия замены линейного прохода: побеждает релевантный чанк, не первый.

    Первый по порядку корпуса чанк содержит только один термин правила (``indexing``) —
    линейный проход брал цитату из него. Лексический индекс ставит первым чанк, где
    терминов больше, и цитата берётся из него.
    """
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    build_id = _write_corpus(
        tmp_path,
        "ref_x",
        [
            ("a1", "https://x/glossary", "Glossary note about indexing only."),
            (
                "b1",
                "https://x/jsonb",
                "The database supports jsonb storage with expression indexing.",
            ),
        ],
        current=True,
    )
    payload = _run_with(
        [
            "baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
            "--json",
        ],
        capsys,
    )
    assert payload["present"] == 1
    # Движок индекса виден: «чем искали» — часть наблюдения.
    assert payload["engine"] == "bm25-py-2"
    assert payload["top_k"] >= 1
    stored = load_package_facts(package)
    assert stored[0].url == "https://x/jsonb"
    assert "jsonb storage" in stored[0].quote
    assert payload["corpus_build_id"] == build_id


# ---------------------------------------------------------------------------
# recon: видимость делящихся цитат (§8.1 шаг 7a: зачёт по фразе)
# ---------------------------------------------------------------------------


def _package_with_shared_term(tmp_path: Path) -> Path:
    """Пакет из двух параметров с **одним** авторским термином (``jsonb``)."""
    package = _package(tmp_path, facts=False)
    (package / "catalog.csv").write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        'p1,JSONB indexing,m,g,Кр,fact,"supports jsonb storage",explicit,,ru=jsonb,ru|en\n'
        'p2,JSONB operators,m,g,Кр,fact,"supports jsonb operators",explicit,,ru=jsonb,ru|en\n',
        encoding="utf-8",
    )
    return package


def test_recon_reports_shared_quotes_and_unique_ratio(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Одна цитата на два параметра видна числом, а не наследуется молча.

    Замер 18.11.2026: зачёт по слову давал 45 ``present`` при 30 уникальных цитатах, и
    одна цитата обслуживала 8 параметров. Доля уникальных делает этот класс дефекта
    наблюдаемым.
    """
    monkeypatch.chdir(tmp_path)
    package = _package_with_shared_term(tmp_path)
    _write_corpus(
        tmp_path,
        "ref_x",
        [("d1", "https://x/jsonb", "The database supports jsonb storage.")],
        current=True,
    )
    payload = _run(
        [
            "baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
            "--json",
        ],
        capsys,
    )
    assert payload["present"] == 2
    assert payload["quotes"] == 2
    assert payload["unique_quotes"] == 1
    assert payload["unique_quote_ratio"] == 0.5
    assert len(payload["shared_quotes"]) == 1
    shared = payload["shared_quotes"][0]
    assert shared["params"] == ["p1", "p2"]
    assert shared["quote"].startswith("The database supports jsonb storage")


def test_recon_unique_ratio_is_none_without_present(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Ноль ``present`` — ``None``, а не ``0.0``: «нечего делить» ≠ «всё делится»."""
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    _write_corpus(
        tmp_path,
        "ref_x",
        [("d1", "https://x/other", "Unrelated page about backups.")],
        current=True,
    )
    payload = _run(
        [
            "baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
            "--json",
        ],
        capsys,
    )
    assert payload["present"] == 0
    assert payload["quotes"] == 0
    assert payload["unique_quotes"] == 0
    assert payload["shared_quotes"] == []
    assert payload["unique_quote_ratio"] is None


def test_recon_prints_quote_sharing_line(tmp_path: Path, capsys, monkeypatch) -> None:
    """Человекочитаемый вывод называет долю уникальных и общие цитаты."""
    monkeypatch.chdir(tmp_path)
    package = _package_with_shared_term(tmp_path)
    _write_corpus(
        tmp_path,
        "ref_x",
        [("d1", "https://x/jsonb", "The database supports jsonb storage.")],
        current=True,
    )
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg"]
    )
    assert args.handler(args) == 0
    out = capsys.readouterr().out
    assert "цитат: 2, уникальных 1 (доля 0.50)" in out
    assert "общих цитат: 1" in out
    assert "p1, p2: The database supports jsonb storage." in out


# ---------------------------------------------------------------------------
# list: дашборд состояния корпуса
# ---------------------------------------------------------------------------


def test_list_reports_corpus_state_and_next_command(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    _write_build(tmp_path, docs=812, chunks=41230, complete=False)
    parser = build_parser()
    args = parser.parse_args(["baseline", "list", "--package", str(package)])
    assert args.handler(args) == 0
    out = capsys.readouterr().out
    assert "сборок 1" in out
    assert "812 док / 41230 чанков" in out
    assert "охват неполный" in out
    assert "дальше (upstream-pg): hound baseline recon" in out


def test_list_json_carries_corpus_state(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    build_id = _write_build(tmp_path, docs=812, chunks=41230, complete=False)
    payload = _run(["baseline", "list", "--package", str(package), "--json"], capsys)
    # Существующие поля не переименованы.
    assert payload["declared"] is True
    assert payload["products"]["upstream-pg"]["role"] == "parallel"
    state = payload["corpus"]["upstream-pg"]
    assert state["builds"] == 1
    assert state["current"] == build_id
    assert (state["docs"], state["chunks"]) == (812, 41230)
    assert state["complete"] is False
    assert state["incomplete_reasons"] == ["truncated:max_documents"]
    assert payload["next"]["step"] == "recon"
    assert "hound baseline recon" in payload["next_commands"]["upstream-pg"]["command"]


def test_list_next_step_is_review_when_facts_are_pending(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path)
    _write_build(tmp_path)
    payload = _run(["baseline", "list", "--package", str(package), "--json"], capsys)
    assert payload["next"]["step"] == "review"
    assert "hound baseline review" in payload["next"]["command"]


def test_list_next_step_is_propose_when_facts_are_signed_but_policy_is_empty(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    # Разрыв, который закрывает ``propose``: факты собраны и подписаны, а строк политики
    # нет. Без этого шага цепочка после ``review`` уходила прямо в карту, и наблюдение
    # оставалось наблюдением — ровно то состояние, в котором жил ``ru-pg-dbms``.
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path)
    _write_build(tmp_path)
    (package / "baselines.csv").write_text(
        "baseline_id,param_id,min_version,inheritance_mode,note\n", encoding="utf-8"
    )
    (package / "sources" / "baseline_facts.csv").write_text(
        FACTS_CSV.replace("pending,,\n", "accepted,roger,\n"), encoding="utf-8"
    )
    payload = _run(["baseline", "list", "--package", str(package), "--json"], capsys)
    assert payload["next"]["step"] == "propose"
    assert "hound baseline propose" in payload["next"]["command"]


def test_list_keeps_policy_step_out_of_the_chain_when_rows_exist(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    # Строка политики уже есть — предлагать нечего: шаг ``propose`` не появляется, даже
    # когда подписанных фактов больше, чем строк.
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path)
    _write_build(tmp_path)
    (package / "sources" / "baseline_facts.csv").write_text(
        FACTS_CSV.replace("pending,,\n", "accepted,roger,\n"), encoding="utf-8"
    )
    payload = _run(["baseline", "list", "--package", str(package), "--json"], capsys)
    assert payload["next"]["step"] != "propose"


# ---------------------------------------------------------------------------
# Ошибки называют следующую команду (§5: без шага ошибка бесполезна)
# ---------------------------------------------------------------------------


def test_recon_without_corpus_names_build_command(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg"]
    )
    with pytest.raises(CliError, match="hound baseline build"):
        args.handler(args)


def test_map_without_corpus_names_build_command(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "map", "--package", str(package), "--baseline", "upstream-pg"]
    )
    args.evaluator = _MapEvaluator()
    with pytest.raises(CliError, match="hound baseline build"):
        args.handler(args)


def test_group_help_names_the_chain() -> None:
    """Порядок цепочки виден из отказа группы, а не только из документации."""
    parser = build_parser()
    args = parser.parse_args(["baseline"])
    with pytest.raises(CliError, match="build → recon → review → propose → report"):
        args.handler(args)


def test_map_llm_concurrency_flag_exists_and_keeps_map_id(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """`--llm-concurrency` ускоряет исполнение, но не меняет `map_id`.

    Ось числа вызовов в полёте — условие исполнения (как `--llm-concurrency` прогона, его
    нет в `EXECUTION_CONTRACT_FIELDS`): если бы она входила в `map_id`, две карты одного
    каталога на одной сборке стали бы несравнимы из-за планировщика.
    """
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    build_id = _reference_db(tmp_path)
    parser = build_parser()
    ids = []
    for concurrency in ("1", "4"):
        args = parser.parse_args(
            [
                "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
                "--build", build_id, "--json", "--llm-concurrency", concurrency,
                "--dry-run",
            ]
        )
        args.evaluator = _MapEvaluator()
        assert args.handler(args) == 0
        payload = json.loads(capsys.readouterr().out)
        ids.append(payload["map_id"])
    assert ids[0] == ids[1], "число вызовов в полёте не является условием измерения"


def test_map_progress_goes_to_stderr_not_stdout(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Прогресс не ломает `--json`: длинный прогон обязан быть видимым, а stdout — чистым."""
    monkeypatch.chdir(tmp_path)
    package = _package_with_manifest(tmp_path)
    build_id = _reference_db(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        [
            "baseline", "map", "--package", str(package), "--baseline", "upstream-pg",
            "--build", build_id, "--json", "--dry-run", "--llm-concurrency", "2",
        ]
    )
    args.evaluator = _MapEvaluator()
    assert args.handler(args) == 0
    captured = capsys.readouterr()
    json.loads(captured.out)  # stdout — только результат
    assert "[map 1/" in captured.err


# ---------------------------------------------------------------------------
# propose: наблюдения → строки политики (шаг между review и report)
# ---------------------------------------------------------------------------

MULTI_VERSION_YAML = """\
schema: reference_products/1
products:
  - baseline_id: upstream-pg
    product_id: postgresql
    canonical_name: PostgreSQL
    versions:
      - version: "18"
        docs_roots: ["https://www.postgresql.org/docs/18/"]
      - version: "17"
        docs_roots: ["https://www.postgresql.org/docs/17/"]
      - version: "11"
        docs_roots: ["https://www.postgresql.org/docs/11/"]
    current_version: "18"
"""

ACCEPTED_FACTS_CSV = (
    "baseline_id,param_id,version,status,url,quote,content_hash,observed_at,"
    "corpus_build_id,review,reviewed_by,note\n"
    "upstream-pg,p1,18,present,https://www.postgresql.org/docs/18/a.html,"
    '"the database supports jsonb storage with expression indexing",'
    f'{"a" * 64},2026-11-17T00:00:00+00:00,ref_postgresql_18_deadbeef,accepted,roger,\n'
)


def _package_accepted(tmp_path: Path) -> Path:
    """Пакет без строк политики и с **подписанным** фактом: предложению есть что сказать.

    ``package.yaml`` здесь обязателен: ``--apply`` пишет через ``package_admin.add_baseline``,
    а тот отказывает каталогу без манифеста («это не пакет категории»).
    """
    package = _package_with_manifest(tmp_path)
    (package / "baselines.csv").write_text(
        "baseline_id,param_id,min_version,inheritance_mode,note\n", encoding="utf-8"
    )
    (package / "sources" / "baseline_facts.csv").write_text(
        ACCEPTED_FACTS_CSV, encoding="utf-8"
    )
    return package


def test_propose_writes_candidate_file_without_touching_baselines(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_accepted(tmp_path)
    baselines_before = (package / "baselines.csv").read_text(encoding="utf-8")
    payload = _run(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    assert payload["proposed"] == ["p1"]
    assert payload["counts"]["direct"] == 1
    assert payload["written"] is True
    assert Path(payload["path"]).is_file()
    assert "--apply" in payload["next_command"]
    # Строки политики пишет только `--apply`: предложение — не решение.
    assert (package / "baselines.csv").read_text(encoding="utf-8") == baselines_before


def test_propose_dry_run_writes_no_file(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_accepted(tmp_path)
    payload = _run(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--dry-run", "--json"],
        capsys,
    )
    assert payload["written"] is False
    assert not Path(payload["path"]).exists()


def test_propose_prints_summary_and_next_command(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package_accepted(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg"]
    )
    assert args.handler(args) == 0
    out = capsys.readouterr().out
    assert "предлагается 1 (direct 1, conditional 0)" in out
    assert "дальше: hound baseline propose" in out


def test_propose_apply_refuses_unfilled_decisions(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Разметку не выдумываем: пустая ``decision`` — отказ с перечнем и путём файла."""
    monkeypatch.chdir(tmp_path)
    package = _package_accepted(tmp_path)
    _run(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--apply", "--actor", "roger"]
    )
    with pytest.raises(CliError) as exc:
        args.handler(args)
    assert exc.value.exit_code == EXIT_USAGE
    assert "p1" in str(exc.value)
    assert "baselines.upstream-pg.candidate.csv" in str(exc.value)


def test_propose_apply_writes_accepted_rows_and_reports_execution_change(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    from core.pipeline.reference_propose import (
        candidates_path,
        load_candidates,
        write_candidates,
    )

    monkeypatch.chdir(tmp_path)
    package = _package_accepted(tmp_path)
    _run(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    rows = load_candidates(candidates_path(package, "upstream-pg"))
    write_candidates(
        package, "upstream-pg",
        [{**row, "decision": "accept"} if row["param_id"] == "p1" else row
         for row in rows],
    )
    payload = _run(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--apply", "--actor", "roger", "--json"],
        capsys,
    )
    assert [item["param_id"] for item in payload["apply"]["added"]] == ["p1"]
    assert payload["execution_change"] is True
    assert "run_id" in payload["note"]
    stored = (package / "baselines.csv").read_text(encoding="utf-8").splitlines()
    assert stored[1:] == ["upstream-pg,p1,18,direct,"]


def test_propose_apply_requires_actor(tmp_path: Path) -> None:
    package = _package_accepted(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "propose", "--package", str(package), "--baseline", "upstream-pg",
         "--apply"]
    )
    with pytest.raises(CliError, match="--actor"):
        args.handler(args)


# ---------------------------------------------------------------------------
# recon --all-versions и охват сборки (I2/I2c)
# ---------------------------------------------------------------------------


def test_recon_all_versions_reports_missing_build_and_writes_once(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """Версия без сборки — видимая строка, а не тихий пропуск; запись одна на все версии."""
    from core.domain.enums import FactStatus

    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    (package / "sources" / "reference_products.yaml").write_text(
        MULTI_VERSION_YAML, encoding="utf-8"
    )
    _reference_db(tmp_path, build_id="ref_v18")
    _reference_db(
        tmp_path,
        build_id="ref_v17",
        root=tmp_path / "reference" / "pkg" / "postgresql" / "17",
    )
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--all-versions", "--json"],
        capsys,
    )
    assert [item["version"] for item in payload["versions"]] == ["18", "17"]
    assert [item["version"] for item in payload["skipped_versions"]] == ["11"]
    assert "hound baseline build" in payload["skipped_versions"][0]["reason"]
    stored = load_package_facts(package)
    assert {(fact.version, fact.status) for fact in stored} == {
        ("18", FactStatus.PRESENT),
        ("17", FactStatus.PRESENT),
    }


def test_recon_all_versions_prints_skipped_line(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    (package / "sources" / "reference_products.yaml").write_text(
        MULTI_VERSION_YAML, encoding="utf-8"
    )
    _reference_db(tmp_path, build_id="ref_v18")
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--all-versions"]
    )
    assert args.handler(args) == 0
    out = capsys.readouterr().out
    assert "@18: сборка ref_v18" in out
    assert "@17: сборки нет, пропущено" in out
    assert "@11: сборки нет, пропущено" in out
    assert "пропущено 2" in out


def test_recon_refuses_version_with_all_versions(tmp_path: Path) -> None:
    package = _package(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--all-versions", "--version", "18"]
    )
    with pytest.raises(CliError, match="взаимоисключающие"):
        args.handler(args)


def test_recon_publishes_absent_only_when_coverage_is_complete(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    """``complete=true`` + «не нашли» ⇒ ``absent``; ``complete=false`` ⇒ ``unclear``."""
    from core.domain.enums import FactStatus

    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    build_id = _write_corpus(
        tmp_path,
        "ref_x",
        [("d1", "https://x/other", "Unrelated page about backups.")],
        current=True,
    )
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    assert payload["absent"] == 1
    assert load_package_facts(package)[0].status is FactStatus.ABSENT

    _write_build(tmp_path, build_id=build_id, complete=False, current=True)
    payload = _run(
        ["baseline", "recon", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    assert payload["unclear"] == 1
    assert payload["absent"] == 0
    assert load_package_facts(package)[0].status is FactStatus.UNCLEAR


# ---------------------------------------------------------------------------
# refresh --apply
# ---------------------------------------------------------------------------

TWO_VERSION_FACTS = (
    "baseline_id,param_id,version,status,url,quote,content_hash,observed_at,"
    "corpus_build_id,review,reviewed_by,note\n"
    "upstream-pg,p1,17,present,https://www.postgresql.org/docs/17/a.html,"
    f'"old text about jsonb storage",{"a" * 64},2026-11-17T00:00:00+00:00,'
    "ref_postgresql_17_deadbeef,pending,,\n"
    "upstream-pg,p1,18,present,https://www.postgresql.org/docs/18/a.html,"
    f'"new text about jsonb storage",{"b" * 64},2026-11-18T00:00:00+00:00,'
    "ref_postgresql_18_deadbeef,pending,,\n"
)


def test_refresh_apply_writes_facts_and_never_touches_baselines(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    (package / "sources" / "baseline_facts.csv").write_text(
        TWO_VERSION_FACTS, encoding="utf-8"
    )
    baselines_before = (package / "baselines.csv").read_text(encoding="utf-8")
    payload = _run(
        ["baseline", "refresh", "--package", str(package), "--baseline", "upstream-pg",
         "--apply", "--actor", "roger", "--json"],
        capsys,
    )
    assert [item["param_id"] for item in payload["applied"]] == ["p1"]
    assert payload["dry_run"] is False
    assert (package / "baselines.csv").read_text(encoding="utf-8") == baselines_before
    log = (package / ".authoring" / "apply_log.jsonl").read_text(encoding="utf-8")
    assert "baseline-refresh" in log


def test_refresh_without_apply_still_writes_nothing(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    package = _package(tmp_path, facts=False)
    (package / "sources" / "baseline_facts.csv").write_text(
        TWO_VERSION_FACTS, encoding="utf-8"
    )
    before = (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8")
    payload = _run(
        ["baseline", "refresh", "--package", str(package), "--baseline", "upstream-pg",
         "--json"],
        capsys,
    )
    assert payload["dry_run"] is True
    assert payload["applied"] == []
    assert (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8") == before
    assert not (package / ".authoring" / "apply_log.jsonl").exists()


def test_refresh_apply_requires_actor(tmp_path: Path) -> None:
    package = _package(tmp_path)
    parser = build_parser()
    args = parser.parse_args(
        ["baseline", "refresh", "--package", str(package), "--baseline", "upstream-pg",
         "--apply"]
    )
    with pytest.raises(CliError, match="--actor"):
        args.handler(args)


# ---------------------------------------------------------------------------
# --passports-dir по умолчанию для всей группы (тихий ложный ноль роли)
# ---------------------------------------------------------------------------

#: Минимальный argv каждой подкоманды: проверяется только наличие ``passports_dir``.
_BASELINE_ARGV: dict[str, list[str]] = {
    "build": ["--baseline", "upstream-pg", "--dry-run"],
    "list": [],
    "facts": [],
    "report": [],
    "review": ["--baseline", "upstream-pg", "--actor", "roger", "--accept"],
    "refresh": ["--baseline", "upstream-pg"],
    "recon": ["--baseline", "upstream-pg"],
    "propose": ["--baseline", "upstream-pg"],
    "map": ["--baseline", "upstream-pg"],
}


def test_baseline_is_in_the_passports_dir_defaults() -> None:
    """Без дефолта роль предка читалась бы по пустому набору паспортов (ложный «parallel»)."""
    from core.cli.main import _PASSPORTS_DIR_COMMANDS

    assert "baseline" in _PASSPORTS_DIR_COMMANDS


def test_every_baseline_subcommand_accepts_passports_dir(tmp_path: Path) -> None:
    """``main`` подставляет дефолт через ``getattr`` — у каждой подкоманды поле обязано быть."""
    package = _package(tmp_path)
    parser = build_parser()
    for name, extra in _BASELINE_ARGV.items():
        args = parser.parse_args(
            ["baseline", name, "--package", str(package), *extra]
        )
        assert getattr(args, "passports_dir", "missing") is None, name
