"""Пины команды ``hound search`` — разведка по корпусу прогона (без сети и без прогона).

Команда — композиция, а не новая подсистема, поэтому проверяются её **решения**, а не
выдача целиком: что она читает (только корпус и строку прогона), чего она не делает
молча (не понижает профиль, не подменяет модель вектора, не отвечает без LLM), и как
она выбирает прогон по умолчанию.

Корпус и плечи — фейки: настоящий ``CorpusReader`` пинит свой набор тестов
(``tests/store/test_corpus_reader.py``), а здесь важно, что команда передаёт в него
и что печатает.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.adapters.lexical_bm25 import LexicalHit
from core.cli.commands import search
from core.cli.errors import CliError
from core.cli.parser import build_parser
from core.cli.wiring import RetrievalPorts
from core.config.deploy import DeployConfig
from core.ports.vector_index import RetrievedChunk
from core.store.corpus_reader import PartitionRef, RunCorpusRef
from core.util.schema_gate import SchemaVersionMismatch

CATALOG = """param_id,title,module,group,importance,param_type,rule,absence_rule,role_binding,concept_terms,search_langs
p_one,Подключаемый механизм хранения,core,g,Баз,fact,правило,distribution_absence,core_engine,"{""ru"": [""сменный механизм""], ""en"": [""pluggable storage""]}",ru|en
"""

#: Декларация reference-линии: одна линия, одна версия — минимум, который нужен команде.
DECLARATION = """schema: reference_products/1
products:
  - baseline_id: upstream-pg
    product_id: postgresql
    canonical_name: PostgreSQL
    versions:
      - version: "18"
        docs_roots: ["https://example.org/docs/18/"]
    current_version: "18"
"""


def _chunk(chunk_id: str, score: float, *, product: str = "p1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        product_id=product,
        text=f"текст {chunk_id}",
        content_hash="h" * 16,
        heading_path="раздел",
        build_id="pb_1",
        score=score,
    )


class FakeLexical:
    """Лексическое плечо: возвращает заранее заданную выдачу по номеру запроса."""

    build_id = "cb_test"

    def __init__(self, by_query: dict[str, list[RetrievedChunk]]) -> None:
        self.by_query = by_query
        self.seen: list[str] = []

    def search(
        self, query: str, product_ids: list[str], top_k: int, lang: str | None = None
    ) -> list[RetrievedChunk]:
        self.seen.append(query)
        return self.by_query.get(query, [])[:top_k]


@dataclass
class FakeCorpus:
    ref: RunCorpusRef
    parts: list[PartitionRef]
    meta: dict[str, dict[str, str]] = field(default_factory=dict)
    closed: bool = False
    expected_schema_version: str = "9.9.9"
    mismatch: bool = False

    def run_ref(self) -> RunCorpusRef:
        return self.ref

    def partitions(self, build_id: str) -> list[PartitionRef]:
        return self.parts

    def iter_chunks(self, build_id: str, *, product_id: str | None = None) -> Any:
        return iter(())

    def get_chunk(self, chunk_id: str) -> Any:
        return None

    def get_chunks(self, chunk_ids: Any) -> dict[str, Any]:
        return {}

    def chunk_meta(self, chunk_ids: Any) -> dict[str, dict[str, str]]:
        return {chunk_id: self.meta.get(chunk_id, {}) for chunk_id in chunk_ids}

    def close(self) -> None:
        self.closed = True


def _ref(**overrides: Any) -> RunCorpusRef:
    values: dict[str, Any] = {
        "run_id": "run-1",
        "package_id": "pkg",
        "index_build_id": "cb_test",
        "retrieval_profile": "R2",
        "retrieval_cutoff_k": 10,
        "embed_model_id": "BAAI/bge-m3",
        "schema_version": "9.9.9",
    }
    values.update(overrides)
    return RunCorpusRef(**values)


def _ports(lexical: FakeLexical | None) -> RetrievalPorts:
    return RetrievalPorts(lexical_index=lexical, vector_index=None, embedder=None)


def _install(
    monkeypatch: pytest.MonkeyPatch,
    corpus: FakeCorpus,
    ports: RetrievalPorts,
) -> None:
    monkeypatch.setattr(search, "CorpusReader", lambda *a, **k: corpus)
    monkeypatch.setattr(search, "build_retrieval_ports", lambda **kwargs: ports)
    monkeypatch.setattr(search, "load_deploy_config", lambda path=None: DeployConfig())


def _args(*argv: str) -> Any:
    return build_parser().parse_args(["search", *argv])


def _package(tmp_path: Path) -> Path:
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "catalog.csv").write_text(CATALOG, encoding="utf-8")
    return package


def test_free_text_search_returns_material_and_closes_the_reader(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    corpus = FakeCorpus(
        ref=_ref(),
        parts=[PartitionRef("p1", "pb_1", "BAAI/bge-m3", 1024)],
        meta={"c1": {"url": "https://example.org/a", "lang": "ru"}},
    )
    lexical = FakeLexical({"запрос": [_chunk("c1", 3.0), _chunk("c2", 1.0)]})
    _install(monkeypatch, corpus, _ports(lexical))

    assert search.run(_args("запрос", "--db", "/tmp/x.db")) == 0

    printed = capsys.readouterr().out
    assert "РАЗВЕДКА" in printed
    assert "https://example.org/a" in printed
    assert "c2" in printed
    # Читатель закрыт: команда не держит файл БД после себя.
    assert corpus.closed is True


def test_param_mode_searches_the_terms_the_evaluator_would_use(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """``--param`` берёт запросы из ``render_queries``, а не из заголовка."""
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    lexical = FakeLexical(
        {
            "Подключаемый механизм хранения": [_chunk("c1", 1.0)],
            "сменный механизм": [_chunk("c2", 5.0)],
            "pluggable storage": [_chunk("c2", 2.0)],
        }
    )
    _install(monkeypatch, corpus, _ports(lexical))
    args = _args(
        "--param", "p_one", "--package", str(_package(tmp_path)), "--db", "/tmp/x.db",
        "--json",
    )
    args.json = False  # печать человека: проверяем, что запросы названы

    assert search.run(args) == 0
    printed = capsys.readouterr().out
    assert "сменный механизм" in printed
    assert "pluggable storage" in printed
    assert "absence_rule=distribution_absence" in printed


def test_param_hits_are_merged_by_max_across_queries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    lexical = FakeLexical(
        {
            "Подключаемый механизм хранения": [_chunk("c1", 1.0)],
            "сменный механизм": [_chunk("c1", 4.0)],
            "pluggable storage": [_chunk("c2", 2.0)],
        }
    )
    _install(monkeypatch, corpus, _ports(lexical))
    payload = search._collect(
        _args("--param", "p_one", "--package", str(_package(tmp_path)), "--db", "/tmp/x.db")
    )
    assert [hit["chunk_id"] for hit in payload["hits"]] == ["c1", "c2"]
    assert payload["hits"][0]["score"] == 4.0
    assert payload["hits"][0]["queries"] == [0, 1]
    assert payload["reconnaissance"] is True


def test_foreign_schema_is_refused_unless_explicitly_allowed(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """Гейт версии остаётся: без флага — отказ, с флагом — чтение корпуса."""

    def _raise(*a: Any, **k: Any) -> Any:
        raise SchemaVersionMismatch("файл объявляет 3.20.0, код ждёт 3.26.0")

    monkeypatch.setattr(search, "CorpusReader", _raise)
    with pytest.raises(CliError) as excinfo:
        search._collect(_args("запрос", "--db", "/tmp/x.db"))
    assert "--allow-legacy-schema" in str(excinfo.value)
    assert excinfo.value.exit_code == 2

    corpus = FakeCorpus(ref=_ref(schema_version="3.20.0"), parts=[PartitionRef("p1", "pb_1", None, None)])
    corpus.mismatch = True
    _install(monkeypatch, corpus, _ports(FakeLexical({"запрос": []})))
    payload = search._collect(
        _args("запрос", "--db", "/tmp/x.db", "--allow-legacy-schema")
    )
    assert payload["schema"]["mismatch"] is True
    assert payload["hits"] == []


def test_unknown_product_names_the_ones_that_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    _install(monkeypatch, corpus, _ports(FakeLexical({})))
    with pytest.raises(CliError) as excinfo:
        search._collect(_args("запрос", "--db", "/tmp/x.db", "--product", "нет_такого"))
    assert "нет_такого" in str(excinfo.value)
    assert "p1" in str(excinfo.value)


def test_run_without_index_build_refuses_instead_of_searching_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = FakeCorpus(ref=_ref(index_build_id=None), parts=[])
    _install(monkeypatch, corpus, _ports(None))
    with pytest.raises(CliError) as excinfo:
        search._collect(_args("запрос", "--db", "/tmp/x.db"))
    assert "индексная сборка" in str(excinfo.value)


def test_query_and_param_together_are_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    _install(monkeypatch, corpus, _ports(FakeLexical({})))
    with pytest.raises(CliError) as excinfo:
        search._collect(
            _args(
                "запрос", "--param", "p_one", "--package", str(_package(tmp_path)),
                "--db", "/tmp/x.db",
            )
        )
    assert "либо" in str(excinfo.value)


def test_dense_profile_without_a_declared_model_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Модель не объявлена ⇒ отказ, а не тихий переход на лексику (O6)."""
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    _install(
        monkeypatch,
        corpus,
        RetrievalPorts(lexical_index=FakeLexical({}), vector_index=object(), embedder=None),
    )
    with pytest.raises(CliError) as excinfo:
        search._collect(
            _args("запрос", "--db", "/tmp/x.db", "--retrieval-profile", "R1")
        )
    assert "модель весов не объявлена" in str(excinfo.value)


def test_answer_without_llm_configuration_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    _install(monkeypatch, corpus, _ports(FakeLexical({"запрос": [_chunk("c1", 1.0)]})))

    class _NoLlm:
        available = False
        model_id = ""
        base_url = ""

    monkeypatch.setattr(search, "build_discovery_llm", lambda **kwargs: _NoLlm())
    with pytest.raises(CliError) as excinfo:
        search._collect(_args("запрос", "--db", "/tmp/x.db", "--answer"))
    assert "не сконфигурирована" in str(excinfo.value)


def _make_run(db_path: Path, run_id: str, created_at: str) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY, created_at TEXT)")
        conn.execute("INSERT INTO runs VALUES (?, ?)", (run_id, created_at))
        conn.commit()
    finally:
        conn.close()


def test_latest_run_is_chosen_by_created_at_not_by_mtime(tmp_path: Path) -> None:
    runs = tmp_path / "runs"
    _make_run(runs / "run-old" / "cohound.db", "run-old", "2026-11-01T00:00:00+00:00")
    _make_run(runs / "run-new" / "cohound.db", "run-new", "2026-11-18T00:00:00+00:00")
    # Битый файл не участвует в выборе и не роняет команду.
    (runs / "run-broken").mkdir()
    (runs / "run-broken" / "cohound.db").write_bytes(b"not a database")
    assert search._latest_run_id(runs) == "run-new"


def test_latest_run_without_any_run_is_not_found(tmp_path: Path) -> None:
    with pytest.raises(CliError) as excinfo:
        search._latest_run_id(tmp_path / "empty")
    assert excinfo.value.exit_code == 4


# ---------------------------------------------------------------------------
# Сторона reference-корпуса (план 27): ``--scope reference|both``
# ---------------------------------------------------------------------------


def _write_reference_db(path: Path, chunks: list[dict[str, Any]]) -> None:
    """Минимальная БД чанков: ровно те колонки, которые читает ``iter_reference_chunks``.

    Настоящая схема пинится в ``tests/store``; здесь нужен только носитель выдачи, и
    ``chunk_no`` есть потому, что проход упорядочен ``doc_id, chunk_no, chunk_id``.
    """
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE corpus_chunks (chunk_id TEXT, doc_id TEXT, product_id TEXT, "
            "url TEXT, heading_path TEXT, lang TEXT, chunk_no INTEGER, text TEXT, "
            "context_prefix TEXT, content_hash TEXT, build_id TEXT)"
        )
        for number, chunk in enumerate(chunks):
            conn.execute(
                "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    chunk["chunk_id"],
                    chunk["doc_id"],
                    "postgresql",
                    chunk.get("url"),
                    chunk.get("heading_path"),
                    "en",
                    number,
                    chunk["text"],
                    "",
                    chunk.get("content_hash", "h" * 64),
                    chunk.get("build_id", "ref_a"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _reference_tree(
    tmp_path: Path,
    *,
    version: str = "18",
    builds: tuple[str, ...] = ("ref_a",),
    current: str | None = "ref_a",
    complete: bool = True,
    chunks: list[dict[str, Any]] | None = None,
    package: str = "pkg",
) -> Path:
    """Раскладка ``reference-corpus/2``: ``builds/{build_id}/{reference.db,manifest.json}``."""
    root = tmp_path / "reference"
    version_dir = root / package / "postgresql" / version
    for build in builds:
        build_dir = version_dir / "builds" / build
        build_dir.mkdir(parents=True)
        _write_reference_db(
            build_dir / "reference.db",
            chunks
            or [
                {
                    "chunk_id": "r1",
                    "doc_id": "d1",
                    "url": "https://docs.example/r1",
                    "heading_path": "Раздел",
                    "text": "pluggable toast storage",
                }
            ],
        )
        (build_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "schema": "reference-corpus/2",
                    "build_id": build,
                    "package_id": package,
                    "product_id": "postgresql",
                    "version": version,
                    "content_digest": "d",
                    "docs": 1,
                    "chunks": 1,
                    "complete": complete,
                }
            ),
            encoding="utf-8",
        )
    if current is not None:
        (version_dir / "current.json").write_text(
            json.dumps({"build_id": current}), encoding="utf-8"
        )
    return root


def _reference_package(tmp_path: Path, *, declaration: str | None = DECLARATION) -> Path:
    """Пакет с каталогом и (опционально) декларацией reference-линий."""
    package = _package(tmp_path)
    (package / "sources").mkdir()
    if declaration is not None:
        (package / "sources" / "reference_products.yaml").write_text(
            declaration, encoding="utf-8"
        )
    return package


def _paths(tmp_path: Path) -> Any:
    """Раскладка хоста: ``reference`` — обычное поле ``HARNESS_FIELDS``."""
    return SimpleNamespace(
        packages=tmp_path / "packages",
        passports=tmp_path / "passports",
        runs=tmp_path / "runs",
        index_store=tmp_path / "index_store",
        reference=tmp_path / "reference",
    )


class FakeBm25:
    """Reference-движок с управляемым счётом: нужен, чтобы проверить **порядок блоков**."""

    engine_version = "fake-bm25"

    def __init__(self) -> None:
        self.docs: list[Any] = []

    def index(self, docs: Any) -> None:
        self.docs = list(docs)

    def search(
        self, query: str, product_ids: list[str] | None = None, top_k: int = 8,
        lang: str | None = None,
    ) -> list[LexicalHit]:
        return [LexicalHit("r1", "postgresql", 99.0)][:top_k]


def test_reference_scope_reads_reference_db_without_opening_the_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``--scope reference`` не открывает run-DB: сторона предка живёт вне ``runs/``."""
    package = _reference_package(tmp_path)
    _reference_tree(tmp_path)
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))

    def _no_reader(*a: Any, **k: Any) -> Any:
        raise AssertionError("--scope reference не открывает run-DB")

    monkeypatch.setattr(search, "CorpusReader", _no_reader)

    payload = search._collect(
        _args("pluggable toast", "--scope", "reference", "--package", str(package))
    )
    assert payload["run_id"] is None
    assert payload["reference"]["build_id"] == "ref_a"
    assert payload["reference"]["product_id"] == "postgresql"
    assert payload["reference"]["version"] == "18"
    assert [hit["side"] for hit in payload["hits"]] == ["reference"]
    assert payload["hits"][0]["chunk_id"] == "r1"
    assert payload["hits"][0]["heading_path"] == "Раздел"
    assert payload["hits"][0]["url"] == "https://docs.example/r1"


def test_reference_scope_refuses_a_dense_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Векторов у reference-корпуса нет ⇒ отказ, а не тихое понижение до R0."""
    package = _reference_package(tmp_path)
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))
    with pytest.raises(CliError) as excinfo:
        search._collect(
            _args(
                "q", "--scope", "reference", "--package", str(package),
                "--retrieval-profile", "R2",
            )
        )
    assert excinfo.value.exit_code == 2
    assert "векторов" in str(excinfo.value)


def test_reference_param_uses_parameter_terms_not_render_queries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Постановка задачи у предка — термины recon, а не запросы Ш6."""
    package = _reference_package(tmp_path)
    _reference_tree(tmp_path)
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))
    seen: list[str] = []

    def _terms(row: Any) -> list[str]:
        seen.append(row.param_id)
        return ["pluggable", "toast"]

    def _no_render(row: Any) -> list[str]:
        raise AssertionError("reference-сторона не использует render_queries")

    monkeypatch.setattr(search, "parameter_terms", _terms)
    monkeypatch.setattr(search, "render_queries", _no_render)

    payload = search._collect(
        _args("--param", "p_one", "--scope", "reference", "--package", str(package))
    )
    assert seen == ["p_one"]
    assert payload["reference_queries"] == ["pluggable toast"]
    assert payload["builders"] == {"cohort": None, "reference": "parameter_terms"}


def test_both_scope_prints_two_blocks_without_resorting_between_sides(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """Счета двух движков несравнимы: reference с 99.0 не поднимается над когортой."""
    package = _reference_package(tmp_path)
    _reference_tree(tmp_path)
    corpus = FakeCorpus(ref=_ref(), parts=[PartitionRef("p1", "pb_1", None, None)])
    _install(
        monkeypatch, corpus, _ports(FakeLexical({"pluggable toast": [_chunk("c1", 0.5)]}))
    )
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))
    monkeypatch.setattr(search, "Bm25Index", FakeBm25)

    payload = search._collect(
        _args(
            "pluggable toast", "--scope", "both", "--package", str(package),
            "--db", "/tmp/x.db",
        )
    )
    assert [hit["side"] for hit in payload["hits"]] == ["cohort", "reference"]
    assert [hit["score"] for hit in payload["hits"]] == [0.5, 99.0]
    assert payload["reference"]["profile"] == "R0"

    search._print_text(payload)
    printed = capsys.readouterr().out
    assert printed.index("КОРПУС КОГОРТЫ") < printed.index("REFERENCE-КОРПУС")
    # Каждый блок нумеруется со своего начала: это два списка, а не один рейтинг.
    assert printed.count(" 1. [") == 2


def test_missing_reference_declaration_names_the_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    package = _reference_package(tmp_path, declaration=None)
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))
    with pytest.raises(CliError) as excinfo:
        search._collect(_args("q", "--scope", "reference", "--package", str(package)))
    message = str(excinfo.value)
    assert "reference_products.yaml" in message
    assert "hound baseline list" in message
    assert excinfo.value.exit_code == 2


def test_several_reference_builds_without_current_refuse_and_list_them(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Выбор по mtime запрещён: без указателя и с двумя сборками — отказ с перечнем."""
    package = _reference_package(tmp_path)
    _reference_tree(tmp_path, builds=("ref_a", "ref_b"), current=None)
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))
    with pytest.raises(CliError) as excinfo:
        search._collect(_args("q", "--scope", "reference", "--package", str(package)))
    message = str(excinfo.value)
    assert "ref_a" in message and "ref_b" in message
    assert "--reference-build" in message
    assert excinfo.value.exit_code == 2


def test_incomplete_reference_build_is_visible_in_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``complete=false`` — предупреждение в выдаче, а не молчание; код возврата тот же."""
    package = _reference_package(tmp_path)
    _reference_tree(tmp_path, complete=False)
    monkeypatch.setattr(search, "_resolve_paths", lambda args: _paths(tmp_path))
    payload = search._collect(
        _args("pluggable toast", "--scope", "reference", "--package", str(package))
    )
    assert payload["reference"]["complete"] is False
    assert "неполная" in payload["reference"]["notice"]
    assert "проходу" in payload["reference"]["notice"]
