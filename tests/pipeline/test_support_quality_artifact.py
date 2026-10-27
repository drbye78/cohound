"""Тесты артефакта качества поддержки (A3: у замера есть производитель).

Оба замера считаются в :mod:`core.pipeline.evaluate`; здесь проверяется только то, что
отчёт их **публикует** — иначе они остаются числами, которые никто не видит (тот же
дефект, что пустой `relevant_evidence_coverage`: поле есть, производителя нет).
"""

from __future__ import annotations

import yaml

from core.domain import Cell, CellState, LLMEvidence, SourceKind, Status
from core.domain.models import CatalogRow
from core.pipeline.support_quality import (
    support_quality,
    support_quality_line,
    write_support_quality_artifact,
)

#: Визитка продукта — ровно тот класс цитаты, который замер обязан не засчитать.
HEADER = "Product 1.0 (2026-01-01, Upstream 18)"


def _param(param_id: str, title: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="m",
        group="g",
        importance="Кр",
        param_type="fact",
        rule="r",
        absence_rule="explicit",
    )


def _cell(param_id: str, *, product: str = "p1", status: Status = Status.OK) -> Cell:
    # I1: evaluated ✅/⚠️ требует evidence ≥ 1 — цитата-визитка предмет замера не меняет,
    # но ячейка без доказательства вообще не собирается (и это правильно).
    return Cell(
        run_id="run-1",
        product_id=product,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=status,
        source_kind=SourceKind.INHERITANCE,
        evidence=[LLMEvidence(source_id="chunk-1", quote=HEADER)],
    )


class _FakeStore:
    """Носитель только одного метода — читателя цитат (больше замерам не нужно)."""

    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.rows = rows

    def iter_cell_quotes(self, run_id: str) -> list[dict[str, str]]:
        return list(self.rows)


def test_measurements_get_producer_and_stay_observations(tmp_path) -> None:
    """Шапка как единственная поддержка видна и в файле, и строкой; вердикт не меняется."""
    params = {"p_jit": _param("p_jit", "JIT-компиляция целых операторов")}
    cells = [_cell("p_jit")]
    store = _FakeStore(
        [
            {
                "product_id": "p1",
                "param_id": "p_jit",
                "citation_quote": HEADER,
                "evidence_type": "derived",
                "source_kind": "inheritance",
            }
        ]
    )

    payload = support_quality(cells=cells, catalog=params, store=store, run_id="run-1")

    assert payload["overlap"]["cells_considered"] == 1
    assert payload["overlap"]["cells_flagged"] == 1
    assert payload["overlap"]["by_status"] == {"✅": 1}
    assert payload["concentration"]["products"]["p1"]["distinct_quotes"] == 1
    assert payload["concentration"]["products"]["p1"]["max_share"] == 1.0

    path = write_support_quality_artifact(
        payload, tmp_path / "outputs" / "support_quality.yaml", run_id="run-1"
    )
    assert path is not None
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert document["artifact"] == "support_quality"
    assert document["run_id"] == "run-1"
    assert document["overlap"]["cells_flagged"] == 1
    # Наблюдение, а не решение: в файле названо, что вердикты не затронуты.
    assert "не гейты" in document["note"]

    line = support_quality_line(payload)
    assert line is not None
    assert "не о предмете" in line
    # Одна ячейка — не концентрация: «19 ячеек на одной цитате» и «одна ячейка на одной
    # цитате» — разные факты, и второй шумом в строке оператора был бы ложью масштаба.
    assert "концентрация" not in line


def test_line_names_concentration_when_it_is_real() -> None:
    """Строка оператора называет концентрацию, только когда она есть (≥2 ячеек)."""
    payload = {
        "overlap": {"cells_considered": 3, "cells_flagged": 0, "by_status": {}},
        "concentration": {
            "products": {
                "p1": {
                    "max_share": 0.6667,
                    "top_quote_count": 2,
                    "distinct_quotes": 2,
                }
            }
        },
    }

    line = support_quality_line(payload)

    assert line is not None
    assert "концентрация поддержки: p1 — 2 ячеек на одной цитате" in line
    assert "0.67" in line

def test_concentration_distinguishes_independent_quotes(tmp_path) -> None:
    """`distinct_quotes` отличает «N независимых цитат» от «одна визитка на N ячеек»."""
    params = {
        "p_a": _param("p_a", "Параллельный ANALYZE"),
        "p_b": _param("p_b", "Параллельный EXPLAIN ANALYZE"),
        "p_c": _param("p_c", "Квоты ресурсов на уровне запросов"),
    }
    cells = [_cell("p_a"), _cell("p_b"), _cell("p_c"), _cell("p_d", status=Status.OK)]
    params["p_d"] = _param("p_d", "Ролевой доступ к функциям")
    store = _FakeStore(
        [
            {
                "product_id": "p1",
                "param_id": "p_a",
                "citation_quote": "Параллельный ANALYZE выполняет план параллельно",
                "evidence_type": "direct",
                "source_kind": "documentation",
            },
            {
                "product_id": "p1",
                "param_id": "p_b",
                "citation_quote": "EXPLAIN ANALYZE поддерживает параллельный план",
                "evidence_type": "direct",
                "source_kind": "documentation",
            },
            {
                "product_id": "p1",
                "param_id": "p_c",
                "citation_quote": HEADER,
                "evidence_type": "derived",
                "source_kind": "inheritance",
            },
            {
                "product_id": "p1",
                "param_id": "p_d",
                "citation_quote": HEADER,
                "evidence_type": "derived",
                "source_kind": "inheritance",
            },
        ]
    )

    payload = support_quality(cells=cells, catalog=params, store=store, run_id="run-1")

    product = payload["concentration"]["products"]["p1"]
    assert product["cells_present"] == 4
    assert product["distinct_quotes"] == 3
    # Визитка держит 2 из 4 ячеек — доля названа числом, порога нет.
    assert product["top_quote_count"] == 2
    assert product["max_share"] == 0.5
    assert product["top_quote_source_kinds"] == {"inheritance": 2}
    assert payload["overlap"]["cells_flagged"] == 2


def test_empty_measurement_writes_no_file(tmp_path) -> None:
    """Нет presence-ячеек — нет файла: пустой артефакт читался бы как «всё в порядке»."""
    payload = support_quality(cells=[], catalog={}, store=_FakeStore([]), run_id="run-1")
    assert write_support_quality_artifact(payload, tmp_path / "sq.yaml") is None
    assert not (tmp_path / "sq.yaml").exists()
    assert support_quality_line(payload) is None


def test_store_without_quote_reader_does_not_pretend(tmp_path) -> None:
    """Стор без читателя цитат — не «цитат нет» на уровне замера, а материал не прочитан."""
    params = {"p_jit": _param("p_jit", "JIT-компиляция целых операторов")}

    payload = support_quality(
        cells=[_cell("p_jit")], catalog=params, store=object(), run_id="run-1"
    )

    # Замер концентрации честно видит ячейку без цитат (отдельный счётчик), а не выдумывает
    # поддержку; пересечение с предметом считается по нулю цитат — это `no_quotes`, не
    # `supported`.
    assert payload["concentration"]["cells_without_quotes"] == 1
    assert payload["overlap"]["cells_flagged"] == 0
