"""Тесты прибора кросс-языкового сопоставления (`scripts/measure_recon_matching.py`).

Прибор отвечает числом на вопрос «какой слой теряет `present` — запрос, отбор или правило
зачёта», и ломается он тихо: при любом из дефектов ниже числа остаются правдоподобными, а
вывод меняется на противоположный. Поэтому каждый проверяется отдельно, **без сети и без
LLM** — индекс строится на игрушечном корпусе, судья не вызывается.

* **правило `any` не равно `phrase`** — иначе прибор «доказывал» бы, что чинить нечего:
  замер 18.11.2026 даёт 165 против 37 на одном корпусе.
* **`all` требует все слова одного термина**, а не все слова всех терминов: иначе
  многотерминный параметр не засчитывается никогда.
* **цитата всегда подстрока чанка** (I22): правило решает, какое предложение засчитать, но
  не переписывает его.
* **границы предложения** считаются по строке и по точке с пробелом — иначе соседняя
  строка таблицы «подтверждает» параметр, а это ровно тот класс дефекта, который мерится.
* **делимость цитат видна числом**: одна цитата на два параметра даёт `shared` и долю < 1,
  ноль `present` — `None`, а не `0.0` («нечего делить» ≠ «всё делится»).
* **запрос и зачёт — разные построители**: `measure` обязан спросить индекс по `terms`, а
  засчитать по `phrases`; подмена одного другим меняет вывод о слое.
* **судья требует объявленную модель**: `--judge-sample` без `--llm-model` — отказ
  argparse, а не тихий выбор модели из окружения (I21).
* **интервал Уилсона** на малых `n` шире нормального приближения и не выходит за [0, 1].
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

from core.domain.enums import AbsenceRuleType, Importance, ParamType
from core.domain.models import CatalogRow

_ROOT = Path(__file__).resolve().parents[2]


def _load_module() -> Any:
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_recon_matching.py"
    spec = importlib.util.spec_from_file_location("measure_recon_matching", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_recon_matching"] = module
    spec.loader.exec_module(module)
    return module


MEASURE = _load_module()


def _param(param_id: str = "p1", **overrides: Any) -> CatalogRow:
    values: dict[str, Any] = {
        "param_id": param_id,
        "title": "JSONB indexing",
        "module": "m",
        "group": "g",
        "importance": Importance.CRITICAL,
        "param_type": ParamType.FACT,
        "rule": "supports jsonb storage",
        "absence_rule": AbsenceRuleType.EXPLICIT,
        "concept_terms": {"en": ["jsonb storage"], "ru": ["хранение jsonb"]},
    }
    values.update(overrides)
    return CatalogRow(**values)


def _chunk(chunk_id: str, text: str, doc_id: str = "d1") -> Any:
    return MEASURE.ChunkText(
        chunk_id=chunk_id, doc_id=doc_id, url="https://x/", text=text, content_hash="h" * 64
    )


# ---------------------------------------------------------------------------
# Правила зачёта
# ---------------------------------------------------------------------------


def test_any_rule_accepts_a_single_word_and_phrase_rule_does_not() -> None:
    """Главный дефект прибора: «слово» и «фраза» — разные числа на одном тексте."""
    text = "The backup was incremental and the storage is fine."
    assert MEASURE.quote_for_rule(text, ["incremental backup"], "any") is not None
    assert MEASURE.quote_for_rule(text, ["incremental backup"], "phrase") is None
    assert MEASURE.quote_for_rule(text, ["incremental backup"], "all") is not None


def test_all_rule_requires_every_word_of_one_term() -> None:
    """`all` — все слова **одного** термина, а не по слову из разных."""
    text = "The point of recovery is to restore data."
    # «point» и «recovery» есть, «time» нет — фразы нет, и all её не изобретает.
    assert MEASURE.quote_for_rule(text, ["point in time recovery"], "all") is None
    assert MEASURE.quote_for_rule(text, ["point of recovery"], "all") is not None
    # А по любому слову тот же текст засчитывается — это и есть различие слоёв.
    assert MEASURE.quote_for_rule(text, ["point in time recovery"], "any") is not None


def test_quote_is_a_substring_of_the_chunk() -> None:
    """I22: прибор не переписывает цитату, а выбирает предложение."""
    text = "Intro.\nThe database supports jsonb storage with indexing.\nTail."
    quote = MEASURE.quote_for_rule(text, ["jsonb storage"], "phrase")
    assert quote is not None and quote in text
    assert quote.startswith("The database supports")


def test_sentence_boundaries_do_not_leak_across_lines() -> None:
    """Строка таблицы рядом не должна «подтверждать» параметр соседней строкой."""
    text = "| X040 |  | Basic table mapping |  |\nThe jsonb type stores documents."
    quote = MEASURE.quote_for_rule(text, ["jsonb type"], "phrase")
    assert quote == "The jsonb type stores documents."


def test_unknown_rule_is_rejected() -> None:
    with pytest.raises(ValueError):
        MEASURE.quote_for_rule("text", ["term"], "sometimes")


# ---------------------------------------------------------------------------
# Делимость цитат
# ---------------------------------------------------------------------------


def test_summary_counts_shared_quotes_and_ratio() -> None:
    """Одна цитата на два параметра — видна и делает долю меньше единицы."""
    quote = "The database supports jsonb storage."
    stats = MEASURE.summarise({"p1": ("c1", quote), "p2": ("c1", quote), "p3": None})
    assert stats["found"] == 2
    assert stats["unique_quotes"] == 1
    assert stats["unique_ratio"] == 0.5
    assert stats["shared"] == [{"quote": quote, "params": ["p1", "p2"]}]


def test_summary_ratio_is_none_without_found() -> None:
    """Ноль найденного — `None`: делить нечего, и это не то же, что «всё делится»."""
    stats = MEASURE.summarise({"p1": None, "p2": None})
    assert stats["found"] == 0
    assert stats["unique_ratio"] is None
    assert stats["shared"] == []


def test_summary_orders_shared_by_param_count() -> None:
    """Порядок не зависит от порядка обхода: сначала цитата с большим числом параметров."""
    stats = MEASURE.summarise(
        {
            "p1": ("c1", "second"),
            "p2": ("c1", "second"),
            "p3": ("c2", "first"),
            "p4": ("c2", "first"),
            "p5": ("c2", "first"),
        }
    )
    assert [item["quote"] for item in stats["shared"]] == ["first", "second"]


# ---------------------------------------------------------------------------
# Запрос и зачёт — разные слои
# ---------------------------------------------------------------------------


def test_measure_queries_by_terms_and_accepts_by_phrases() -> None:
    """Подмена слоя меняет вывод: запрос — recall, зачёт — точность."""
    chunks = [_chunk("c1", "The backup was incremental and small.")]
    index = MEASURE.build_index(chunks)
    params = [_param("p1", title="Incremental backup", concept_terms={"en": ["incremental backup"]})]

    strict = MEASURE.measure(
        params=params,
        chunks=chunks,
        index=index,
        terms=MEASURE.parameter_terms,
        phrases=MEASURE.parameter_phrases,
        rule="phrase",
        top_k=5,
    )
    loose = MEASURE.measure(
        params=params,
        chunks=chunks,
        index=index,
        terms=MEASURE.parameter_terms,
        phrases=MEASURE.parameter_terms,
        rule="any",
        top_k=5,
    )
    assert strict["p1"] is None, "фраза в тексте не встречается — зачёта быть не должно"
    assert loose["p1"] is not None, "по слову тот же текст засчитывается — это и есть дефект"


def test_measure_returns_none_when_query_is_empty() -> None:
    """Параметр без терминов не ищется вовсе — и это `None`, а не выдуманная цитата."""
    chunks = [_chunk("c1", "Anything.")]
    index = MEASURE.build_index(chunks)
    param = _param("p1", title="QL", concept_terms={})
    results = MEASURE.measure(
        params=[param],
        chunks=chunks,
        index=index,
        terms=MEASURE.parameter_terms,
        phrases=MEASURE.parameter_phrases,
        rule="phrase",
        top_k=5,
    )
    assert results["p1"] is None


# ---------------------------------------------------------------------------
# Интервал и CLI
# ---------------------------------------------------------------------------


def test_wilson_interval_widens_on_small_samples() -> None:
    """На n=25 интервал шире нормального приближения и остаётся внутри [0, 1]."""
    low, high = MEASURE.wilson(2, 25)
    assert 0.0 <= low < 2 / 25 < high <= 1.0
    assert high - low > 0.15
    assert MEASURE.wilson(0, 0) == (0.0, 1.0)


def test_judge_sample_requires_declared_model(capsys: pytest.CaptureFixture[str]) -> None:
    """Модель судьи — объявленный вход: без `--llm-model` прибор отказывает, а не угадывает."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE.main(["--db", "x.db", "--catalog", "catalog.csv", "--judge-sample", "5"])
    assert excinfo.value.code == 2
    assert "--llm-model" in capsys.readouterr().err


def test_unknown_variant_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        MEASURE.main(["--db", "x.db", "--catalog", "catalog.csv", "--variants", "dense"])
    assert excinfo.value.code == 2
    assert "неизвестный вариант" in capsys.readouterr().err


def test_empty_corpus_is_named_not_silently_zero(tmp_path: Path) -> None:
    """Пустой корпус — отказ с адресом, а не отчёт из нулей."""
    db = tmp_path / "reference.db"
    import sqlite3

    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE corpus_chunks (chunk_id TEXT, doc_id TEXT, url TEXT, heading_path TEXT,"
        " text TEXT, content_hash TEXT, chunk_no INTEGER)"
    )
    conn.commit()
    conn.close()
    catalog = tmp_path / "catalog.csv"
    catalog.write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        'p1,JSONB indexing,m,g,Кр,fact,"supports jsonb",explicit,,{"en":["jsonb"]},ru|en\n',
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as excinfo:
        MEASURE.main(["--db", str(db), "--catalog", str(catalog)])
    assert "корпус пуст" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Dense-плечо
# ---------------------------------------------------------------------------


class _FakeEmbedder:
    """Детерминированный «эмбеддер»: вектор — по ключевым словам текста.

    Настоящая модель в тесте не нужна и вредна: проверяется не качество векторов, а то, что
    отбор идёт по косинусу и что плечи сравниваются на одном запросе.
    """

    model_id = "fake@0"

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        out: list[list[float]] = []
        for text in texts:
            lowered = text.lower()
            out.append(
                [
                    1.0 if "jsonb" in lowered else 0.0,
                    1.0 if "backup" in lowered else 0.0,
                    0.1,
                ]
            )
        return out


def test_dense_candidates_rank_by_cosine_and_respect_top_k() -> None:
    """Отбор — по косинусу: ближайший чанк первым, `top_k` режет хвост."""
    chunks = [
        _chunk("c1", "backup and restore notes"),
        _chunk("c2", "jsonb storage indexing"),
        _chunk("c3", "unrelated text about nothing"),
    ]
    param = _param("p1", concept_terms={"en": ["jsonb storage"]})
    candidates = MEASURE.dense_candidates(
        params=[param],
        chunks=chunks,
        embedder=_FakeEmbedder(),
        terms=MEASURE.parameter_terms,
        top_k=2,
    )
    picked = candidates(param)
    assert len(picked) == 2
    assert picked[0] == "c2", "чанк с термином обязан быть первым"


def test_dense_candidates_without_terms_returns_nothing() -> None:
    """Параметр без терминов не ищется — и dense не выдумывает ему кандидатов."""
    chunks = [_chunk("c1", "jsonb")]
    param = _param("p1", title="", concept_terms={})
    candidates = MEASURE.dense_candidates(
        params=[param],
        chunks=chunks,
        embedder=_FakeEmbedder(),
        terms=MEASURE.parameter_terms,
        top_k=5,
    )
    assert candidates(param) == []


def test_dense_arm_refuses_when_model_is_not_declared(monkeypatch: pytest.MonkeyPatch) -> None:
    """Модель — объявленный вход: без `embed.model_id` плечо не собирается, а не «берём что есть»."""
    import core.cli.wiring as wiring
    import core.config.deploy as deploy_module

    monkeypatch.setattr(deploy_module, "load_deploy_config", lambda _path: type("D", (), {"embed": object()})())
    monkeypatch.setattr(wiring, "resolve_embed_spec", lambda _cfg, _run: None)
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._measure_dense_arm(
            params=[],
            chunks=[],
            index=MEASURE.Bm25Index(),
            top_k=5,
            deploy_path=Path("config/deploy.yaml"),
            device="",
            precision="",
        )
    assert "не объявлена модель эмбеддера" in str(excinfo.value)


def test_read_chunks_slice_returns_first_rows_in_corpus_order(tmp_path: Path) -> None:
    """`--max-chunks` — срез корпуса, а не «N произвольных строк» (и он обязан исполняться).

    До 18.11.2026 срез не работал вовсе: `LIMIT` ставился перед `ORDER BY`, SQLite отвечал
    `near "ORDER": syntax error`, и быстрая проверка прибора была невозможна.
    """
    import sqlite3

    db = tmp_path / "reference.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE corpus_chunks (chunk_id TEXT, doc_id TEXT, url TEXT, heading_path TEXT,"
        " text TEXT, content_hash TEXT, chunk_no INTEGER)"
    )
    conn.executemany(
        "INSERT INTO corpus_chunks VALUES (?,?,?,?,?,?,?)",
        [
            ("c2", "d1", None, None, "second", "h", 2),
            ("c1", "d1", None, None, "first", "h", 1),
            ("c3", "d2", None, None, "third", "h", 1),
        ],
    )
    conn.commit()
    conn.close()

    sliced = MEASURE.read_chunks(db, max_chunks=2)
    assert [chunk.chunk_id for chunk in sliced] == ["c1", "c2"], "порядок — документа, не вставки"
    assert len(MEASURE.read_chunks(db)) == 3
