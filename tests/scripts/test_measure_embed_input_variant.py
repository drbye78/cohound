"""Пины прибора ``scripts/measure_embed_input_variant.py`` (без БД, эмбеддера и GPU).

Прибор опубликовал число оси ``EMBED_INPUT_FIELDS`` (O19), и у него уже были прогоны —
поэтому тесты фиксируют не «покрытие», а те свойства, поломка которых даёт **тихо
неверное число**:

* **``_subset`` берёт равномерный шаг, а не первые N.** Первые 2000 чанков
  ``postgrespro`` покрывают 168 документов из 15 188 (замер 16.11.2026) — такой срез
  даёт ``hit@K = 1.000`` на всех вариантах, потому что чужих документов в матрице нет;
  префикс, выданный за срез, превращает дымовое число в число оси;
* **``_MatrixIndex._top`` ранжирует ключом ``(-балл, chunk_id)``, как адаптер**: иначе
  варианты сравнивались бы через другое ядро, и «эффект заголовка» включал бы разницу
  ранжирования. Отдельно — путь полного перебора при ``nan``/``inf``: ``np.partition``
  по нефинитным баллам даёт пустую выдачу, то есть тихий ноль вместо ответа;
* **нулевая строка нормируется без деления на ноль**: ``nan`` в балле — это не «плохой
  чанк», а сломанная метрика всей ячейки;
* **``smoke_only``** в отчёте: без него метрики среза читаются как метрики оси;
* **``_resolve_variants``** отказывает на неизвестном имени, дубле и пустом списке —
  иначе дубль сравнивает вариант сам с собой и печатает нулевую разницу как результат;
* **``_write_cells_csv``**: ``delta = базовый − второй`` и **пуст**, если один ранг
  ``None``; ``None``, записанный как ``0``, врёт в диффе ранжирования.
* **Условия замера публикуются раздельно**: ``chunks`` — сколько чанков измерено (после
  среза), ``partition_chunks`` — размер партиции (только у ``source == "partition"``).
  Слитые в одно число, они утверждают, что база мерилась на 206 931 чанке там, где
  измерено 3 000 (замер 16.11.2026, ``--limit-chunks 3000``). Рядом — инвариант: у всех
  вариантов отчёта ``chunks`` совпадает, иначе их числа несравнимы и отчёт отказывает.
* **Режим питания объявлен, а не выведен**: ``--power-mode`` попадает в отчёт как есть
  (дефолт ``unknown`` печатается предупреждением), а ``ac_online``/``battery_status`` и
  счётчик ``SW Power Capping`` публикуются рядом как контроль. Счётчик — **пара «до/после»**,
  а не разница: разница теряет факт «до тоже был не ноль». Отсутствие файла/команды даёт
  ``None``, никогда ``0``. Вывода режима из признаков нет и тестом не закрепляется.

Синтетика: матрицы ``numpy``, строки ``_Row`` и ``tmp_path`` для CSV. Стенд
``~/.cohound``, ``torch``/``cuda`` и БД не трогаются.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from core.ports.vector_index import VectorQuery

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "measure_embed_input_variant.py"
)


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_measure_embed_input_variant", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MEASURE = _load_harness()


def _row(chunk_id: str, *, text: str = "", doc_id: str = "doc") -> Any:
    """Строка чанка: прибору нужен минимум для входа эмбеддера и ранжирования."""
    return MEASURE._Row(
        chunk_id=chunk_id,
        doc_id=doc_id,
        product_id="product",
        text=text or f"text {chunk_id}",
        heading_path="",
        context_prefix="",
        content_hash=f"hash-{chunk_id}",
    )


def _index(chunk_ids: list[str], matrix: Any) -> Any:
    return MEASURE._MatrixIndex(
        [_row(chunk_id) for chunk_id in chunk_ids],
        matrix,
        build_id="variant:test",
        dim=int(np.asarray(matrix).shape[1]),
    )


# --------------------------------------------------------------------------- _subset


def test_subset_takes_even_step_not_prefix() -> None:
    """Префикс вместо шага: срез из 2000 чанков = 168 документов ⇒ hit@K 1.000 у всех."""
    order = [f"chunk-{index:05d}" for index in range(1000)]

    subset = MEASURE._subset(order, 100)

    assert len(subset) == 100
    assert subset[0] == order[0]
    # Главное утверждение: это не первые 100 чанков.
    assert subset[-1] != order[99]
    assert len(set(subset)) == len(subset)
    positions = [order.index(chunk_id) for chunk_id in subset]
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)
    assert positions[-1] >= 900


@pytest.mark.parametrize("limit", [0, -5, 1000, 5000])
def test_subset_returns_everything_when_limit_is_not_a_cut(limit: int) -> None:
    """``limit=0``/``limit >= len`` — полный корпус, а не пустой список и не срез."""
    order = [f"chunk-{index:05d}" for index in range(1000)]

    assert MEASURE._subset(order, limit) == order


def test_subset_returns_a_copy() -> None:
    """Выдача не делит список с порядком корпуса: срез не имеет права править ``order``."""
    order = ["a", "b", "c"]

    subset = MEASURE._subset(order, 0)
    subset.append("d")

    assert order == ["a", "b", "c"]


# ------------------------------------------------------------------ _MatrixIndex._top


def test_top_ties_break_by_chunk_id() -> None:
    """Равный балл без tie-break: порядок вариантов зависел бы от порядка строк БД."""
    index = _index(["c", "a", "b"], [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]])

    hits = index.search(VectorQuery(product_ids=["product"], top_k=2, vector=[1.0, 0.0]))

    assert [hit.chunk_id for hit in hits] == ["a", "b"]
    assert hits[0].score == hits[1].score


def test_top_slices_to_top_k_and_keeps_descending_order() -> None:
    """Срез больше/меньше ``top_k``: лишние строки попадают в метрику как «найденные»."""
    index = _index(
        ["a", "b", "c", "d", "e"],
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [0.5, 0.5, 0.0], [0.0, 0.0, 0.5]],
    )

    two = index.search(VectorQuery(product_ids=["product"], top_k=2, vector=[1.0, 0.0, 0.0]))
    assert [hit.chunk_id for hit in two] == ["a", "d"]

    every = index.search(
        VectorQuery(product_ids=["product"], top_k=99, vector=[1.0, 0.0, 0.0])
    )
    assert len(every) == 5
    assert [hit.chunk_id for hit in every] == ["a", "d", "b", "c", "e"]
    scores = [hit.score for hit in every]
    assert scores == sorted(scores, reverse=True)


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_top_nonfinite_scores_scan_every_row(bad: float) -> None:
    """``np.partition`` по ``nan``/``inf`` даёт пустую выдачу: ноль вместо ответа."""
    index = _index(["a", "b", "c", "d", "e"], np.eye(5, 3, dtype=np.float32))
    scores = np.array([bad, 0.9, 0.8, 0.7, 0.6], dtype=np.float32)

    hits = index._top(scores, 2)

    assert len(hits) == 2
    assert len({hit.chunk_id for hit in hits}) == 2


def test_zero_row_scores_zero_not_nan() -> None:
    """Деление на нулевую норму: ``nan`` в балле ломает метрику всей ячейки."""
    index = _index(["zero", "a", "b"], [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])

    hits = index.search(VectorQuery(product_ids=["product"], top_k=3, vector=[1.0, 0.0, 0.0]))

    assert len(hits) == 3
    assert all(np.isfinite(hit.score) for hit in hits)
    by_id = {hit.chunk_id: hit.score for hit in hits}
    assert by_id["zero"] == 0.0


# --------------------------------------------------------------------- smoke_only


def test_smoke_only_flag_marks_a_cut_corpus() -> None:
    """Срез корпуса без флага: метрики дымового прогона публикуются как число оси."""
    full_order = [f"chunk-{index:05d}" for index in range(100)]

    assert MEASURE._is_smoke_only(full_order[:10], full_order) is True
    assert MEASURE._is_smoke_only(full_order, full_order) is False


# --------------------------------------------------------------- _resolve_variants


def test_resolve_variants_rejects_unknown_name_with_the_allowed_list() -> None:
    """Опечатка в имени варианта: иначе мерился бы не тот вход, что назван в отчёте."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._resolve_variants("text,headings")

    message = str(excinfo.value.code)
    assert "headings" in message
    for name in MEASURE.VARIANTS:
        assert name in message


def test_resolve_variants_rejects_duplicate() -> None:
    """Дубль: сравнение варианта с самим собой даёт нулевую разницу как результат."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._resolve_variants("text,text")

    assert "дважды" in str(excinfo.value.code)


@pytest.mark.parametrize("raw", ["", "  ", ",", " , "])
def test_resolve_variants_rejects_empty(raw: str) -> None:
    """Пустой список: мерить нечего, а молчаливый ``[]`` даёт отчёт без вариантов."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._resolve_variants(raw)

    assert "пуст" in str(excinfo.value.code)


# ---------------------------------------------------------------- _write_cells_csv


def test_write_cells_csv_delta_is_base_minus_other_and_blank_on_none(
    tmp_path: Path,
) -> None:
    """``None`` в ранге, ставший ``0``, и перевёрнутый знак — два способа соврать в диффе."""
    path = tmp_path / "cells.csv"
    cells = {
        "text": [
            {"cell": "p1", "rank": 3},
            {"cell": "p2", "rank": None},
            {"cell": "p3", "rank": 5},
        ],
        "heading": [
            {"cell": "p1", "rank": 5},
            {"cell": "p2", "rank": 2},
            {"cell": "p3", "rank": None},
        ],
    }

    MEASURE._write_cells_csv(path, ["text", "heading"], cells)

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == ["cell", "text", "heading", "delta"]
    assert rows[1] == ["p1", "3", "5", "-2"]
    # Хотя бы один ранг ``None`` ⇒ delta пуст, а не ``-2``/``0``.
    assert rows[2] == ["p2", "", "2", ""]
    assert rows[3] == ["p3", "5", "", ""]


# ------------------------------------- условия замера: срез и публикация (O19)


def _partition_stats(size: int) -> dict[str, Any]:
    """Статистика базового варианта ровно в той форме, что отдаёт ``_partition_matrix``."""
    return {
        "source": "partition",
        "partition_build_id": "pb_test",
        "composition_id": "cmp_test",
        "chunks": size,
        "partition_chunks": size,
        "seconds": 0.0,
        "chunks_per_s": None,
        "cache": None,
    }


def test_partition_slice_publishes_measured_chunks_not_partition_size() -> None:
    """Срез: ``chunks`` — 100 измеренных, ``partition_chunks`` — 1000 в сборке.

    Слитые в одно поле, они публикуют дымовое число как число оси.
    """
    partition_ids = [f"chunk-{index:05d}" for index in range(1000)]
    order = MEASURE._subset(partition_ids, 100)
    matrix = np.arange(1000 * 2, dtype=np.float32).reshape(1000, 2)
    stats = _partition_stats(1000)

    sliced_order, sliced_matrix = MEASURE._apply_partition_slice(
        order, partition_ids, matrix, stats
    )

    assert len(sliced_order) == 100
    assert stats["chunks"] == 100
    assert stats["partition_chunks"] == 1000
    # Порядок и матрица режутся вместе: строка ``i`` матрицы принадлежит ``sliced_order[i]``.
    positions = [partition_ids.index(chunk_id) for chunk_id in sliced_order]
    assert sliced_matrix.shape[0] == len(sliced_order)
    assert [int(row[0]) // 2 for row in sliced_matrix] == positions


def test_no_slice_publishes_equal_chunk_counts() -> None:
    """Без среза измерено всё: ``chunks == partition_chunks`` (их правит только срез)."""
    published = MEASURE._condition_fields(_partition_stats(206_931))

    assert published["chunks"] == published["partition_chunks"] == 206_931


def test_conditions_separate_measured_chunks_from_partition_size() -> None:
    """Публикация не склеивает два факта в одно поле (отчёт 16.11.2026: 206 931 / 3 000)."""
    stats = _partition_stats(206_931)
    stats["chunks"] = 3000

    published = MEASURE._condition_fields(stats)

    assert published["chunks"] == 3000
    assert published["partition_chunks"] == 206_931


def test_recomputed_variant_has_no_partition_chunks() -> None:
    """Пересчитанный вариант партицию не читал: поля ``partition_chunks`` у него нет."""
    published = MEASURE._condition_fields(
        {"source": "recomputed", "chunks": 3000, "seconds": 438.008, "chunks_per_s": 6.85}
    )

    assert published["chunks"] == 3000
    assert published["partition_chunks"] is None


def test_require_same_chunks_rejects_variants_measured_on_different_corpora() -> None:
    """База на 206 931 и вариант на 3 000: их числа несравнимы, отчёт обязан отказать."""
    reports = [
        {"variant": "text", "chunks": 206_931},
        {"variant": "heading", "chunks": 3000},
    ]

    with pytest.raises(SystemExit) as excinfo:
        MEASURE._require_same_chunks(reports)

    message = str(excinfo.value.code)
    assert "206931" in message
    assert "3000" in message
    assert "text" in message
    assert "heading" in message


def test_require_same_chunks_rejects_unknown_count() -> None:
    """``None`` (неизвестно, сколько измерено) — не «совпадает»: сравнивать нечем."""
    with pytest.raises(SystemExit):
        MEASURE._require_same_chunks(
            [{"variant": "text", "chunks": None}, {"variant": "heading", "chunks": 3000}]
        )


def test_require_same_chunks_accepts_equal_counts() -> None:
    """Одинаковое число измеренных чанков — условие сравнимости выполнено."""
    MEASURE._require_same_chunks(
        [{"variant": "text", "chunks": 3000}, {"variant": "heading", "chunks": 3000}]
    )


def test_partition_matrix_publishes_partition_size(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_partition_matrix`` публикует размер партиции отдельным полем, а не только ``chunks``.

    Проверяется сама функция, а не ``_condition_fields``: поле заполняется **здесь**, и
    удаление строки из неё оставляло бы ``partition_chunks = None`` у базового варианта —
    то есть размер сборки терялся бы молча, ровно тот дефект, ради которого поле заведено.
    """
    from types import SimpleNamespace

    vectors = np.zeros((3, 4), dtype=np.float32)
    data = SimpleNamespace(vectors=vectors, chunk_ids=["a", "b", "c"])
    view = SimpleNamespace(
        composition_id="comp-1", build_id_for=lambda product: f"pb_{product}"
    )

    calls: list[tuple[Any, ...]] = []

    def fake_partition(index_root: Any, package: str, build_id: str, product: str, **_: Any) -> Any:
        calls.append((package, build_id, product))
        return data

    monkeypatch.setattr("core.adapters.index_format.read_partition", fake_partition)
    monkeypatch.setattr("core.pipeline.composition.read_composition", lambda *a, **k: view)

    matrix, chunk_ids, stats = MEASURE._partition_matrix(Path("/nowhere"), "pkg", "comp-1", "prod")

    assert calls == [("pkg", "pb_prod", "prod")]
    assert chunk_ids == ["a", "b", "c"]
    assert matrix is vectors
    assert stats["chunks"] == 3
    assert stats["partition_chunks"] == 3
    assert stats["source"] == "partition"


def test_build_payload_rejects_incomparable_variants() -> None:
    """Путь сборки отчёта неотделим от проверки: несравнимые строки не выйдут наружу.

    Проверка стоит в ``_build_payload``, а не в ``main``, именно ради этого теста: вызов
    из ``main`` можно удалить, и отчёт вышел бы молча.
    """
    reports = [
        {"variant": "text", "chunks": 206_931},
        {"variant": "heading", "chunks": 3000},
    ]

    with pytest.raises(SystemExit):
        MEASURE._build_payload(reports, {"run_id": "r"}, peak_rss_bytes=None, peak_vram_bytes=None)


def test_build_payload_keeps_base_fields_and_variants() -> None:
    """Сборка не теряет поля условий и кладёт ``variants`` последним (порядок — часть формата)."""
    reports = [{"variant": "text", "chunks": 3000}, {"variant": "heading", "chunks": 3000}]

    payload = MEASURE._build_payload(
        reports, {"run_id": "r", "chunks_measured": 3000}, peak_rss_bytes=1, peak_vram_bytes=2
    )

    assert payload["run_id"] == "r"
    assert payload["chunks_measured"] == 3000
    assert payload["peak_rss_bytes"] == 1
    assert payload["peak_vram_bytes"] == 2
    assert payload["variants"] == reports
    assert list(payload)[-1] == "variants"


def test_load_average_published_as_measurement_condition() -> None:
    """Нагрузка хоста — часть условий, а не украшение отчёта.

    ``chunks_per_s`` на общем хосте без неё невоспроизводим: замер 16.11.2026 шёл при
    занятом хосте (рядом работали pytest и другой прибор), и без поля «6.85 чанк/с»
    читается как цена оси, а не как цена конкуренции за хост.
    """
    load = MEASURE._load_average()
    assert load is None or (len(load) == 3 and all(isinstance(v, float) for v in load))

    payload = MEASURE._build_payload(
        [{"variant": "text", "chunks": 3000}],
        {"run_id": "r"},
        peak_rss_bytes=1,
        peak_vram_bytes=None,
        load_average=[7.4, 6.5, 4.7],
    )
    assert payload["load_average"] == [7.4, 6.5, 4.7]
    # Ключ обязан быть в отчёте и когда счётчика нет: иначе поле «иногда есть» нельзя
    # отличить от «не измеряли» при разборе старых артефактов.
    payload = MEASURE._build_payload(
        [{"variant": "text", "chunks": 3000}],
        {"run_id": "r"},
        peak_rss_bytes=1,
        peak_vram_bytes=None,
    )
    assert "load_average" in payload
    assert payload["load_average"] is None


# ------------------------------------- условия питания: объявлено и наблюдено (O19)

_REQUIRED_ARGS = ["--db", "x.db", "--golden", "g.json", "--product", "p"]


def test_power_mode_defaults_to_unknown() -> None:
    """Дефолт — «не объявлено», а не подставленный режим: прибор питание не угадывает."""
    args = MEASURE._parse_args([*_REQUIRED_ARGS])

    assert args.power_mode == "unknown"
    assert args.power_mode == MEASURE.DEFAULT_POWER_MODE


@pytest.mark.parametrize("declared", ["ac-barrel", "ac-usbc", "battery", "AC-USB-C"])
def test_power_mode_reaches_the_report_verbatim(declared: str) -> None:
    """Объявленное значение идёт в отчёт **как есть**: ни нормализации, ни разбора.

    ``AC-USB-C`` не приводится к нижнему регистру, ``unknown`` не подменяется ничем:
    прибор не имеет права «улучшить» то, что объявил оператор.
    """
    args = MEASURE._parse_args([*_REQUIRED_ARGS, "--power-mode", declared])

    payload = MEASURE._build_payload(
        [{"variant": "text", "chunks": 1, "power": {}}],
        {"power_mode": args.power_mode},
        peak_rss_bytes=None,
        peak_vram_bytes=None,
    )

    assert payload["power_mode"] == declared


def test_power_snapshot_reads_ac_and_battery_from_substituted_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Прокси читаются из файлов, а не из констант хоста: тест не зависит от стенда."""
    ac = tmp_path / "online"
    ac.write_text("1\n", encoding="utf-8")
    battery = tmp_path / "status"
    battery.write_text("Charging\n", encoding="utf-8")
    monkeypatch.setattr(MEASURE, "AC_ONLINE_PATH", ac)
    monkeypatch.setattr(MEASURE, "BATTERY_STATUS_PATH", battery)

    snapshot = MEASURE._power_snapshot(with_gpu=False)

    assert snapshot["ac_online"] is True
    assert snapshot["battery_status"] == "Charging"
    # При ``cpu`` карта не опрашивается: поля есть и они ``None``, а не нули.
    assert all(snapshot[field] is None for field in MEASURE.GPU_FIELDS)


def test_power_snapshot_offline_is_false_not_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``0`` — измеренный факт «питания от сети нет»; он не имеет права стать ``None``."""
    ac = tmp_path / "online"
    ac.write_text("0", encoding="utf-8")
    monkeypatch.setattr(MEASURE, "AC_ONLINE_PATH", ac)
    monkeypatch.setattr(MEASURE, "BATTERY_STATUS_PATH", tmp_path / "no-battery")

    snapshot = MEASURE._power_snapshot(with_gpu=False)

    assert snapshot["ac_online"] is False
    assert snapshot["battery_status"] is None


def test_power_snapshot_missing_files_are_none_not_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Нет файла — «не измерено». ``0``/``False`` утверждали бы факт, которого не видели."""
    monkeypatch.setattr(MEASURE, "AC_ONLINE_PATH", tmp_path / "no-ac")
    monkeypatch.setattr(MEASURE, "BATTERY_STATUS_PATH", tmp_path / "no-battery")

    snapshot = MEASURE._power_snapshot(with_gpu=False)

    assert snapshot["ac_online"] is None
    assert snapshot["battery_status"] is None
    assert all(snapshot[field] is None for field in MEASURE.GPU_FIELDS)


def test_power_snapshot_without_nvidia_smi_is_none_not_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Отказ инструмента (нет команды, нет карты) — ``None``, а не падение и не ``0``."""
    monkeypatch.setattr(MEASURE, "AC_ONLINE_PATH", tmp_path / "no-ac")
    monkeypatch.setattr(MEASURE, "BATTERY_STATUS_PATH", tmp_path / "no-battery")
    monkeypatch.setattr(MEASURE, "NVIDIA_SMI", "cohound-no-such-nvidia-smi")

    snapshot = MEASURE._power_snapshot(with_gpu=True)

    assert all(snapshot[field] is None for field in MEASURE.GPU_FIELDS)


def test_parse_gpu_metrics_maps_unavailable_to_none() -> None:
    """``[N/A]`` мобильной карты — это «не измерено», а не «ноль ватт»."""
    measured = MEASURE._parse_gpu_metrics("1057, 15.11, 67.90\n")

    assert measured["sm_clock_mhz"] == 1057
    assert measured["power_draw_w"] == pytest.approx(15.11)
    assert measured["temperature_c"] == 67
    assert measured["sw_power_cap_us"] is None

    unavailable = MEASURE._parse_gpu_metrics("[N/A], [N/A], [N/A]")

    assert unavailable["sm_clock_mhz"] is None
    assert unavailable["power_draw_w"] is None
    assert unavailable["temperature_c"] is None


def test_sw_power_cap_counter_is_read_from_counters_not_from_the_flag() -> None:
    """Флаг ``Active`` — «ограничена сейчас»; счётчик — «сколько микросекунд за замер»."""
    text = (
        "    Clocks Event Reasons\n"
        "        SW Power Cap                                   : Not Active\n"
        "    Clocks Event Reasons Counters\n"
        "        SW Power Capping                               : 19983 us\n"
    )

    assert MEASURE._parse_sw_power_cap_us(text) == 19983


def test_sw_power_cap_zero_is_measured_zero_and_missing_line_is_none() -> None:
    """``0 us`` — измеренный ноль; отсутствие строки — ``None``: их нельзя путать."""
    assert MEASURE._parse_sw_power_cap_us("        SW Power Capping : 0 us") == 0
    assert MEASURE._parse_sw_power_cap_us("    Performance State : P0") is None


def test_throttle_counter_is_published_as_a_pair_not_a_difference() -> None:
    """Разница ``after − before`` теряет «до тоже был не ноль»: карта уже была ограничена."""
    before = {
        "ac_online": True,
        "battery_status": "Charging",
        "sm_clock_mhz": 1057,
        "power_draw_w": 15.11,
        "temperature_c": 67,
        "sw_power_cap_us": 20068,
    }
    after = {**before, "sw_power_cap_us": 20068}

    window = MEASURE._power_window(before, after)

    assert window["before"]["sw_power_cap_us"] == 20068
    assert window["after"]["sw_power_cap_us"] == 20068
    # Набор ключей **закрыт**: любая производная (как бы она ни называлась) — дефект,
    # потому что теряет факт «до тоже был не ноль». Перечисление известных имён ловило
    # бы только те, что придумал автор теста: мутация `delta_us` проходила мимо.
    assert set(window) == {"before", "after"}
    # Окно не делит словари со снимками: правка отчёта не правит замер.
    assert window["before"] is not before
    assert window["after"] is not after


def test_summary_warns_when_power_mode_is_not_declared(capsys: pytest.CaptureFixture[str]) -> None:
    """``unknown`` обязан быть виден: иначе число читается как «условия в порядке»."""
    payload = {
        "power_mode": "unknown",
        "variants": [{"variant": "text", "power": {"before": {}, "after": {}}}],
    }

    MEASURE._print_power_conditions(payload)

    out = capsys.readouterr().out
    assert "не объявлен" in out
    assert "ВНИМАНИЕ" in out
    assert "не привязаны к режиму питания" in out


def test_summary_prints_declared_mode_and_counter_pair_in_one_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Сводка: режим и признаки одной строкой, счётчик — парой, без предупреждения."""
    payload = {
        "power_mode": "ac-barrel",
        "variants": [
            {
                "variant": "text",
                "power": {
                    "before": {
                        "ac_online": True,
                        "battery_status": "Charging",
                        "sm_clock_mhz": 1057,
                        "power_draw_w": 15.11,
                        "temperature_c": 67,
                        "sw_power_cap_us": 19983,
                    },
                    "after": {"sw_power_cap_us": 20068},
                },
            }
        ],
    }

    MEASURE._print_power_conditions(payload)

    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 1
    assert "ac-barrel" in lines[0]
    assert "19983→20068" in lines[0]
    assert "ВНИМАНИЕ" not in out


def test_observed_battery_does_not_rewrite_the_declared_mode(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Расхождение объявленного с наблюдённым читает человек, а не прибор.

    ``Discharging`` при объявленном ``ac-barrel`` — повод разобраться, а не повод
    переписать режим: объявленное остаётся объявленным, наблюдённое печатается рядом.
    """
    payload = {
        "power_mode": "ac-barrel",
        "variants": [
            {"variant": "text", "power": {"before": {"battery_status": "Discharging"}, "after": {}}}
        ],
    }

    MEASURE._print_power_conditions(payload)

    out = capsys.readouterr().out
    assert "ac-barrel" in out
    assert "Discharging" in out


def test_declared_power_mode_never_derives_from_observed_signs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Режим берётся из аргумента, даже когда признаки говорят противоположное.

    USB-C и комплектный БП на этом хосте неразличимы, поэтому «угадать» режим нельзя:
    угаданное значение в отчёте — ложь о условии замера. Признаки публикуются рядом как
    контроль, и расхождение с объявленным читает человек.

    Тест намеренно подставляет признаки «от батареи», а объявляет `ac-barrel`: если
    когда-нибудь появится вывод режима из `_ac_online`/`_battery_status`, этот тест упадёт.
    """
    monkeypatch.setattr(MEASURE, "_ac_online", lambda: False)
    monkeypatch.setattr(MEASURE, "_battery_status", lambda: "Discharging")
    args = MEASURE._parse_args(["--db", "x.db", "--golden", "g.json", "--product", "p",
                                "--power-mode", "ac-barrel"])

    assert MEASURE._declared_power_mode(args) == "ac-barrel"

    # И дефолт — именно `unknown`, а не «выведенное» значение.
    args = MEASURE._parse_args(["--db", "x.db", "--golden", "g.json", "--product", "p"])
    assert MEASURE._declared_power_mode(args) == MEASURE.DEFAULT_POWER_MODE == "unknown"
