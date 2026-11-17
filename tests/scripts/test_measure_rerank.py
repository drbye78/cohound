"""Тесты прибора ``scripts/measure_rerank.py`` (O23 плана 26).

Прибор отвечает на вопрос «меняет ли реранк адрес эталонного документа», и цена ошибки
здесь — вердикт об оси ``rerank_spec``. Поэтому проверяются свойства, поломка которых даёт
**правдоподобное, но неверное** число:

* реранкер, который **переставляет** кандидатов, обязан поднять ``hit@K`` — иначе прибор
  мерит не реранк, а что-то ещё (например, порядок плеча);
* реранкер, который порядок не меняет, обязан дать ``Δ = 0`` и ``McNemar p = 1.0`` —
  иначе «эффект» появляется из статистики, а не из данных;
* реранкер, вернувший меньше баллов, чем документов, — отказ: неполный список молча
  переставил бы кандидатов;
* базовый top-K считается из **той же** выдачи, что видел реранкер (``top_n``), а метрика —
  по срезу ``cutoff_k``: оба числа публикуются, подмена одного другим меняет предмет.

Синтетическая run-DB строится в ``tmp_path`` (колонки — только те, что читает прибор);
сеть, GPU и веса не нужны: реранкер — фейк с журналом вызовов.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_rerank as measure  # noqa: E402

RUN_ID = "case-run-0001"
PRODUCT = "p"


class _Reranker:
    """Фейк порта: балл задаёт тест, вызовы журналируются."""

    def __init__(self, fn: Any, *, model_id: str = "org/model@sha1") -> None:
        self._fn = fn
        self.model_id = model_id
        self.device = "cpu"
        self.calls: list[tuple[str, list[str]]] = []

    def rerank(self, query: str, documents: list[str]) -> list[float]:
        self.calls.append((query, list(documents)))
        return list(self._fn(query, list(documents)))


def _param(param_id: str, title: str) -> dict[str, Any]:
    """Строка каталога в форме ``plan_inputs.json`` (минимум для ``CatalogRow``)."""
    return {
        "param_id": param_id,
        "title": title,
        "module": "Модуль",
        "group": "Группа",
        "importance": "Кр",
        "param_type": "fact",
        "rule": "Правило ячейки",
        "concept_terms": {},
        "search_langs": [],
    }


def _make_run(
    tmp_path: Path,
    *,
    docs: list[tuple[str, str]],
    params: list[dict[str, Any]],
    expected: list[str],
    golden_run_id: str = RUN_ID,
) -> tuple[Path, Path]:
    """Синтетический прогон: run-DB, снимок каталога и документный эталон.

    ``docs`` — ``[(doc_id, text)]``, по одному куску на документ: прибору важно
    ранжирование, а не нарезка.
    """
    db_path = tmp_path / "cohound.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE runs (
            run_id TEXT,
            package_id TEXT,
            index_build_id TEXT,
            retrieval_cutoff_K INTEGER
        );
        CREATE TABLE corpus_chunks (
            chunk_id TEXT,
            doc_id TEXT,
            product_id TEXT,
            chunk_no INTEGER,
            text TEXT,
            heading_path TEXT,
            context_prefix TEXT,
            content_hash TEXT,
            char_start INTEGER,
            char_end INTEGER
        );
        """
    )
    conn.execute("INSERT INTO runs VALUES (?, ?, ?, ?)", (RUN_ID, "case", "cb_test", 10))
    for doc_id, text in docs:
        conn.execute(
            "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"{doc_id}:0", doc_id, doc_id.split(":", 1)[0], 0, text, "", "", f"h:{doc_id}", 0, len(text)),
        )
    conn.commit()
    conn.close()

    (tmp_path / "plan_inputs.json").write_text(
        json.dumps({"params": params}, ensure_ascii=False), encoding="utf-8"
    )
    golden_path = tmp_path / "golden.json"
    golden_path.write_text(
        json.dumps(
            {
                "format": "cohound-retrieval-golden-docs/1",
                "case_id": "case",
                "corpus_run_id": golden_run_id,
                "entries": [
                    {
                        "cell": f"{PRODUCT}/{params[0]['param_id']}",
                        "product_id": PRODUCT,
                        "param_id": params[0]["param_id"],
                        "source_url": "https://example.test/doc",
                        "expected_doc_ids": expected,
                        "match": "exact",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return db_path, golden_path


#: Плечо BM25 ставит ``distractor`` первым: у него больше вхождений терминов запроса.
_DOCS = [
    ("p:distractor", "backup configuration " * 5),
    ("p:good", "backup configuration"),
]
_TITLE = "backup configuration"


def _reordering(query: str, documents: list[str]) -> list[float]:
    """Нужный документ — первым: короткий текст (``p:good``) получает высокий балл."""
    return [1.0 if len(text) < 40 else 0.0 for text in documents]


def _identity(query: str, documents: list[str]) -> list[float]:
    """Порядок не меняется: балл строго убывает по входу."""
    return [1.0 - index * 0.01 for index in range(len(documents))]


# ---------------------------------------------------------------------------
# Главное: реранк меняет адрес — и это видно числом
# ---------------------------------------------------------------------------


def test_rerank_that_reorders_raises_hit_at_k(tmp_path: Path) -> None:
    db, golden = _make_run(
        tmp_path, docs=_DOCS, params=[_param("param_a", _TITLE)], expected=["p:good"]
    )
    reranker = _Reranker(_reordering)

    payload = measure.run(
        db, golden, reranker=reranker, top_n=2, cutoff_k=1, bootstrap=200
    )

    assert payload["metrics"]["baseline"]["hit_at_k"] == 0.0
    assert payload["metrics"]["reranked"]["hit_at_k"] == 1.0
    assert payload["paired"]["better"] == 1
    assert payload["paired"]["worse"] == 0
    # Оба числа — условие реранка и срез эталона — публикуются раздельно.
    assert payload["top_n"] == 2
    assert payload["cutoff_k"] == 1
    # Реранкер видел всех кандидатов top-N, а не только срез K.
    assert reranker.calls == [(_TITLE, ["backup configuration " * 5, "backup configuration"])]
    assert payload["cost"]["pairs"] == 2


def test_rerank_that_keeps_order_gives_zero_delta(tmp_path: Path) -> None:
    """Порядок не изменился — эффекта нет: Δ = 0 и McNemar p = 1.0, а не «на грани»."""
    db, golden = _make_run(
        tmp_path, docs=_DOCS, params=[_param("param_a", _TITLE)], expected=["p:good"]
    )

    payload = measure.run(
        db, golden, reranker=_Reranker(_identity), top_n=2, cutoff_k=1, bootstrap=200
    )

    assert payload["paired"]["n_pairs"] == 1
    assert payload["paired"]["delta"] == 0.0
    assert payload["paired"]["mcnemar_p"] == 1.0
    assert payload["metrics"]["baseline"]["hit_at_k"] == payload["metrics"]["reranked"]["hit_at_k"]


def test_reranker_with_short_score_list_is_refused(tmp_path: Path) -> None:
    db, golden = _make_run(
        tmp_path, docs=_DOCS, params=[_param("param_a", _TITLE)], expected=["p:good"]
    )
    with pytest.raises(ValueError, match="баллов"):
        measure.run(
            db,
            golden,
            reranker=_Reranker(lambda q, docs: [1.0]),
            top_n=2,
            cutoff_k=1,
        )


def test_top_n_below_cutoff_k_is_refused(tmp_path: Path) -> None:
    db, golden = _make_run(
        tmp_path, docs=_DOCS, params=[_param("param_a", _TITLE)], expected=["p:good"]
    )
    with pytest.raises(ValueError, match="top_n"):
        measure.run(db, golden, reranker=_Reranker(_identity), top_n=1, cutoff_k=5)


def test_golden_from_another_run_is_refused(tmp_path: Path) -> None:
    """``doc_id`` разных корпусов несравнимы: чужой ``corpus_run_id`` — отказ, не число."""
    db, golden = _make_run(
        tmp_path,
        docs=_DOCS,
        params=[_param("param_a", _TITLE)],
        expected=["p:good"],
        golden_run_id="other-run",
    )
    with pytest.raises(ValueError, match="несравнимы"):
        measure.run(db, golden, reranker=_Reranker(_identity), top_n=2, cutoff_k=1)


# ---------------------------------------------------------------------------
# Чистые части: порядок, CSV, разбор модели
# ---------------------------------------------------------------------------


class _Chunk:
    def __init__(self, chunk_id: str) -> None:
        self.chunk_id = chunk_id


def test_order_by_score_is_descending_with_chunk_id_tie_break() -> None:
    chunks = [_Chunk("c"), _Chunk("a"), _Chunk("b")]
    assert measure._order_by_score(chunks, [0.5, 0.5, 1.0]) == ["b", "a", "c"]


def test_cells_csv_leaves_delta_blank_when_a_rank_is_missing(tmp_path: Path) -> None:
    """``None`` в CSV — пустая ячейка: ноль читался бы как «было первым местом»."""
    out = tmp_path / "cells.csv"
    measure._write_cells_csv(
        out,
        [
            {
                "cell": "p/a",
                "product_id": "p",
                "param_id": "a",
                "candidates": 2,
                "queries": 1,
                "rank_baseline": None,
                "rank_reranked": 1,
                "unresolved": False,
            },
            {
                "cell": "p/b",
                "product_id": "p",
                "param_id": "b",
                "candidates": 2,
                "queries": 1,
                "rank_baseline": 3,
                "rank_reranked": 1,
                "unresolved": False,
            },
        ],
    )
    with out.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["delta"] == ""
    assert rows[0]["rank_baseline"] == ""
    assert rows[1]["delta"] == "2"


def test_split_model_requires_explicit_pin() -> None:
    assert measure._split_model("org/model@sha") == ("org/model", "sha")
    assert measure._split_model("org/model") == ("org/model", None)


def test_main_refuses_unpinned_model(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "cohound.db"
    db.write_bytes(b"")
    golden = tmp_path / "golden.json"
    golden.write_text("{}", encoding="utf-8")
    code = measure.main(
        ["--db", str(db), "--golden", str(golden), "--model", "org/model"]
    )
    assert code == 1
    assert "ревизии" in capsys.readouterr().err


def test_main_returns_one_for_missing_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = measure.main(
        [
            "--db",
            str(tmp_path / "нет.db"),
            "--golden",
            str(tmp_path / "нет.json"),
            "--model",
            "org/model@sha",
        ]
    )
    assert code == 1
    assert "БД не найдена" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Объявленный режим скоринга: --scoring доезжает до отчёта
# ---------------------------------------------------------------------------


class _Adapter:
    """Фейк ``LocalReranker``: запоминает объявленные условия, считает по ``_identity``.

    Подменяет адаптер целиком, потому что проверяется **провод**: какие условия прибор
    объявил адаптеру и что он записал в отчёт. Сам режим скоринга проверен в
    ``tests/adapters/test_rerank_local.py``.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.model_id = f"{kwargs['model_id']}@{kwargs['revision']}"
        self.device = str(kwargs.get("device", "cpu"))
        self.scoring = str(kwargs.get("scoring", "cross-encoder"))
        self.batch_size = int(
            kwargs.get("batch_size") or (4 if self.scoring == "decoder-yes-no" else 16)
        )
        self.decision_tokens = tuple(kwargs.get("decision_tokens") or ("yes", "no"))
        self.decision_token_ids = (7, 8) if self.scoring == "decoder-yes-no" else None

    def rerank(self, query: str, documents: list[str]) -> list[float]:
        return _identity(query, documents)


_PROMPT = "Q:{query}\nD:{document}\nA:"


def _run_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *extra: str,
) -> tuple[int, Path]:
    """Прогон ``main`` с подменённым адаптером; возвращает код и путь отчёта."""
    db, golden = _make_run(
        tmp_path, docs=_DOCS, params=[_param("param_a", _TITLE)], expected=["p:good"]
    )
    monkeypatch.setattr(measure, "LocalReranker", _Adapter)
    out = tmp_path / "report.json"
    code = measure.main(
        [
            "--db",
            str(db),
            "--golden",
            str(golden),
            "--model",
            "org/model@sha",
            "--device",
            "cpu",
            "--top-n",
            "2",
            "--cutoff-k",
            "1",
            "--bootstrap",
            "50",
            "--out-json",
            str(out),
            *extra,
        ]
    )
    return code, out


def test_scoring_reaches_the_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Режим, промпт, его ``sha256``, пара токенов и батч — в отчёте, а не только в флагах."""
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text(_PROMPT + "\n", encoding="utf-8")

    code, out = _run_main(
        tmp_path,
        monkeypatch,
        "--scoring",
        "decoder-yes-no",
        "--prompt-file",
        str(prompt_file),
        "--decision-tokens",
        "1,0",
    )

    assert code == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["scoring"] == "decoder-yes-no"
    # Шаблон записан дословно (снят только завершающий перевод строки файла).
    assert payload["prompt_template"] == _PROMPT
    assert payload["prompt_sha256"] == hashlib.sha256(_PROMPT.encode("utf-8")).hexdigest()
    assert payload["prompt_source"] == str(prompt_file)
    assert payload["decision_tokens"] == ["1", "0"]
    assert payload["decision_token_ids"] == [7, 8]
    # Батч — фактический: --batch-size не задан, значит дефолт режима (4 у декодера).
    assert payload["batch_size"] == 4


def test_cross_encoder_report_has_no_prompt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """У кросс-энкодера промпта нет: поля пусты, а батч — прежние 16."""
    code, out = _run_main(tmp_path, monkeypatch)

    assert code == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["scoring"] == "cross-encoder"
    assert payload["prompt_template"] is None
    assert payload["prompt_sha256"] is None
    assert payload["decision_token_ids"] is None
    assert payload["batch_size"] == 16


def test_decoder_without_prompt_file_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _ = _run_main(tmp_path, monkeypatch, "--scoring", "decoder-yes-no")
    assert code == 1
    assert "--prompt-file" in capsys.readouterr().err


def test_prompt_file_with_cross_encoder_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Объявленное и неисполняемое условие — отказ, а не тихое игнорирование."""
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text(_PROMPT, encoding="utf-8")
    code, _ = _run_main(tmp_path, monkeypatch, "--prompt-file", str(prompt_file))
    assert code == 1
    assert "промпта нет" in capsys.readouterr().err


def test_unknown_scoring_is_refused(tmp_path: Path) -> None:
    """Третьего режима нет: ``--scoring auto`` — отказ CLI, а не догадка."""
    with pytest.raises(SystemExit) as excinfo:
        measure.main(
            [
                "--db",
                str(tmp_path / "cohound.db"),
                "--golden",
                str(tmp_path / "golden.json"),
                "--model",
                "org/model@sha",
                "--scoring",
                "auto",
            ]
        )
    assert excinfo.value.code == 2


def test_decision_tokens_must_be_a_pair() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="decision-tokens"):
        measure._decision_tokens("yes")


# ---------------------------------------------------------------------------
# Прогресс по ячейке: результат переживает падение процесса
# ---------------------------------------------------------------------------


def test_run_reports_each_cell_to_the_callback(tmp_path: Path) -> None:
    """Каждая ячейка уходит в колбэк сразу: файл прогресса — не отчёт «в конце»."""
    db, golden = _make_run(
        tmp_path, docs=_DOCS, params=[_param("param_a", _TITLE)], expected=["p:good"]
    )
    seen: list[dict[str, Any]] = []

    payload = measure.run(
        db,
        golden,
        reranker=_Reranker(_reordering),
        top_n=2,
        cutoff_k=1,
        bootstrap=50,
        on_cell=seen.append,
    )

    assert len(seen) == len(payload["per_cell"]) == 1
    cell = seen[0]
    assert cell["cell"] == payload["per_cell"][0]["cell"]
    # В строке достаточно, чтобы пересчитать ячейку: ожидание и обе выдачи.
    assert cell["expected_doc_ids"] == ["p:good"]
    assert cell["retrieved_baseline"] == ["p:distractor"]
    assert cell["retrieved_reranked"] == ["p:good"]
    assert cell["rank_baseline"] is None
    assert cell["rank_reranked"] == 1


def test_progress_file_is_written_next_to_the_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``main`` ведёт ``*.progress.jsonl`` рядом с отчётом: заголовок + ячейка."""
    code, out = _run_main(tmp_path, monkeypatch)
    progress = out.with_name(out.stem + ".progress.jsonl")

    assert code == 0
    assert progress.is_file()
    rows = [
        json.loads(line)
        for line in progress.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows[0]["kind"] == "header"
    assert rows[0]["model"] == "org/model@sha"
    assert rows[0]["scoring"] == "cross-encoder"
    assert rows[0]["prompt_sha256"] is None
    assert [row["cell"] for row in rows[1:]] == [
        cell["cell"] for cell in json.loads(out.read_text(encoding="utf-8"))["per_cell"]
    ]


def test_progress_writer_flushes_each_line(tmp_path: Path) -> None:
    """Строка видна в файле до закрытия — иначе смысл файла пропадает."""
    path = tmp_path / "p.jsonl"
    writer = measure._ProgressWriter(path, header={"kind": "header"})
    writer({"cell": "p/a"})
    text = path.read_text(encoding="utf-8")
    assert len(text.splitlines()) == 2
    writer.close()

