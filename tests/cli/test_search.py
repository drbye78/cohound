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

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

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
