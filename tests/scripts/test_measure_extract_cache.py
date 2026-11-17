"""Тесты прибора цены `extract_cache` (`scripts/measure_extract_cache.py`, O32).

Проверяются свойства, поломка которых **меняет опубликованное число**, а не форма вывода:

* **размер записи** — конверт обязан быть ровно объявленного размера, иначе `fsync`
  синхронизирует не ту запись, и «цена записи» описывает другую величину;
* **что именно синхронизируется** — «запись без `fsync`» не имеет права звать `os.fsync`
  вовсе, а разложение каталога обязано синхронизировать **каталог**, иначе графа
  «код этого не делает» меряет то же, что уже входит в `put`;
* **прогрев исключён** — первый `fsync` на новой ФС несёт создание inode/журнала, и
  медиана, посчитанная с ним, описывает первое касание, а не запись;
* **контроль сходимости** — сумма частей против настоящего `put`; если контроль потерян,
  разложение публиковать нельзя;
* **рабочий кэш не трогается** — прибор пишет только в песочницу.

Реальный кэш, `state.db` и сеть не нужны: песочница в `tmp_path`.
"""

from __future__ import annotations

import importlib.util
import os
import stat
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_extract_cache.py"
    spec = importlib.util.spec_from_file_location("measure_extract_cache", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_extract_cache"] = module
    spec.loader.exec_module(module)
    return module


measure = _load_module()


# ---------------------------------------------------------------- размер записи


@pytest.mark.parametrize("size", [120, 5_169, 70_000])
def test_payload_has_exactly_the_requested_size(size: int) -> None:
    """Размер — условие замера: «примерно 5 КБ» меряет другую запись."""
    payload = measure._payload_of_size(size)
    assert len(payload.encode("utf-8")) == size


def test_payload_is_a_json_object() -> None:
    """Конверт кэша — JSON; если песочница пишет не JSON, длина перестаёт быть сравнимой."""
    import json

    assert isinstance(json.loads(measure._payload_of_size(5_169)), dict)


def test_payload_rejects_a_size_that_cannot_be_an_envelope() -> None:
    with pytest.raises(ValueError):
        measure._payload_of_size(1)


# ---------------------------------------------------------------- что синхронизируется


def _spy_fsync(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Считает вызовы `os.fsync` и запоминает, что за дескриптор был синхронизирован."""
    seen: list[int] = []
    real = os.fsync

    def _spy(fd: int) -> None:
        seen.append(os.fstat(fd).st_mode)
        real(fd)

    monkeypatch.setattr(measure.os, "fsync", _spy)
    return seen


def test_write_without_fsync_never_syncs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Иначе «запись без fsync» включала бы fsync, и вычитание дало бы отрицательную цену."""
    seen = _spy_fsync(monkeypatch)
    measure._write_once(tmp_path / "w", "a" * 64, measure._payload_of_size(1_000))
    assert seen == []


def test_put_syncs_the_regular_file_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Рабочий код синхронизирует файл (extract_cache.py:595), а не каталог."""
    seen = _spy_fsync(monkeypatch)
    result = measure._bench_put(tmp_path / "p", 1_000, repeats=3, warmup=1)
    assert len(seen) == 1 + 1 + 3  # проба подгонки размера + прогрев + измеренные
    assert all(stat.S_ISREG(mode) for mode in seen)
    assert result["fsync_file"]["repeats"] == 3


def test_dir_sync_measures_the_directory_and_nothing_else(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Графа «код НЕ делает» обязана мерить `fsync` каталога, а не второй `fsync` файла."""
    seen = _spy_fsync(monkeypatch)
    result = measure._bench_put(tmp_path / "p", 1_000, repeats=3, warmup=1, sync_dir=True)
    dirs = [mode for mode in seen if stat.S_ISDIR(mode)]
    # Проба подгонки размера идёт до обёртки, поэтому каталог синхронизируют только
    # прогрев и измеренные записи.
    assert len(dirs) == 1 + 3
    assert result["fsync_dir"]["repeats"] == 3
    assert result["fsync_dir"]["median_seconds"] is not None


def test_put_restores_the_module_os_after_the_probe(tmp_path: Path) -> None:
    """Прокси, оставшийся в модуле, подменил бы `os` всему процессу после замера."""
    from core.pipeline import extract_cache as cache_module

    before = cache_module.os
    measure._bench_put(tmp_path / "p", 1_000, repeats=2, warmup=1, sync_dir=True)
    assert cache_module.os is before


def test_put_without_fsync_is_the_paired_remainder(tmp_path: Path) -> None:
    """`put − fsync` из одной итерации: иначе части не складываются (остаток уходил в −10 %)."""
    result = measure._bench_put(tmp_path / "p", 1_000, repeats=5, warmup=1)
    assert result["without_fsync"]["repeats"] == 5
    assert result["without_fsync"]["median_seconds"] < result["median_seconds"]
    assert result["fsync_file"]["median_seconds"] > 0


def test_write_leaves_no_temporary_file(tmp_path: Path) -> None:
    """Временный файл убирается `os.replace`: иначе песочница мерила бы мусор."""
    name = "d" * 64
    measure._write_once(tmp_path / "w", name, measure._payload_of_size(1_000))
    shard = tmp_path / "w" / name[:2]
    assert [entry.name for entry in shard.iterdir()] == [f"{name}.json"]


def test_write_uses_the_sharded_layout_of_the_real_cache(tmp_path: Path) -> None:
    """Раскладка `{hash[:2]}/{hash}.json` — часть условия: 256 подкаталогов, не один."""
    name = "e" * 64
    measure._write_once(tmp_path / "w", name, measure._payload_of_size(1_000))
    assert (tmp_path / "w" / name[:2] / f"{name}.json").is_file()


# ---------------------------------------------------------------- прогрев и агрегаты


def test_bench_writes_discards_exactly_the_warmup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Медиана по последним `repeats`; первые `warmup` не попадают в выборку."""
    counter = {"n": 0}

    def _fake(parent, name, payload):
        counter["n"] += 1
        value = float(counter["n"])
        return {"write": value, "replace": value, "total": value}

    monkeypatch.setattr(measure, "_write_once", _fake)
    result = measure._bench_writes(tmp_path, "x", repeats=3, warmup=2)
    # Отброшены 1.0 и 2.0 — медиана из 3.0/4.0/5.0 обязана быть 4.0.
    assert result["total"]["repeats"] == 3
    assert result["total"]["median_seconds"] == 4.0
    assert result["warmup"] == 2


def test_empty_sample_is_none_not_zero() -> None:
    """«Не мерили» и «стоит ноль» — разные факты."""
    assert measure._aggregate([])["median_seconds"] is None
    assert measure._percentile_float([], 0.95) is None


def test_percentile_float_takes_a_ranked_value() -> None:
    """Перцентиль — наблюдённое значение, не интерполяция: при малом n важно."""
    values = [0.001, 0.002, 0.003, 0.9]
    assert measure._percentile_float(values, 0.95) == 0.9
    assert measure._percentile_float(values, 0.5) == 0.003


# ---------------------------------------------------------------- разложение целиком


def test_breakdown_publishes_conditions_and_a_control(tmp_path: Path) -> None:
    """Без условий (размер, прогрев, носитель) и контроля число нечитаемо."""
    root = tmp_path / "cache"
    root.mkdir()
    result = measure._write_breakdown(
        entry_bytes=1_000, repeats=4, warmup=1, sandbox_parent=tmp_path, root=root
    )
    assert result["entry_bytes_requested"] == 1_000
    assert result["entry_bytes_written"] == 1_000
    assert result["warmup"] == 1 and result["repeats"] == 4
    assert result["kernel"]
    assert set(result["components"]) == {
        "write_only",
        "put_without_fsync",
        "fsync_file",
        "fsync_dir",
        "full_put",
    }
    # Пробная запись (подгонка размера) + прогрев + измеренные: `put` молча не пишет
    # только при невалидном ключе, и счётчик — доказательство, что путь записи исполнялся.
    assert result["components"]["full_put"]["writes"] == 1 + 1 + 4
    assert result["components"]["fsync_file"]["repeats"] == 4
    assert result["components"]["fsync_dir"]["repeats"] == 4
    assert result["dir_fsync_in_code"] is False
    assert result["code_reference"].endswith("extract_cache.py:595")
    assert result["parts_sum_seconds"] is not None
    assert result["residual_seconds"] is not None
    assert result["durable_extra_seconds"] is not None


def test_breakdown_control_is_additive(tmp_path: Path) -> None:
    """`put` = (запись без `fsync` + `fsync`) с точностью до накладных расходов конверта.

    Ровно этот контроль ловит подмену пути: до правки 17.11.2026 `fsync` мерился вторым
    независимым циклом, и остаток ушёл в **−10 %** — части не складывались вовсе.
    """
    root = tmp_path / "cache"
    root.mkdir()
    result = measure._write_breakdown(
        entry_bytes=5_169, repeats=15, warmup=3, sandbox_parent=tmp_path, root=root
    )
    share = result["residual_share"]
    assert share is not None
    assert -0.5 < share < 0.5, f"остаток {share} — части не складываются, мерили не тот путь"


def test_breakdown_refuses_an_entry_smaller_than_the_envelope(tmp_path: Path) -> None:
    """Иначе «сумма частей сходится» было бы совпадением на другом размере записи."""
    root = tmp_path / "cache"
    root.mkdir()
    with pytest.raises(ValueError):
        measure._write_breakdown(
            entry_bytes=120, repeats=2, warmup=1, sandbox_parent=tmp_path, root=root
        )


def test_main_reports_an_unreachable_entry_size_as_a_usage_error(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    assert measure.main(["--root", str(root), "--bench-files", "0", "--entry-bytes", "120"]) == 2


def test_breakdown_does_not_touch_the_cache_root(tmp_path: Path) -> None:
    """Рабочий кэш — только чтение: иначе замер платит кэшем за число."""
    root = tmp_path / "cache"
    root.mkdir()
    (root / "sentinel.json").write_text("{}", encoding="utf-8")
    measure._write_breakdown(entry_bytes=1_000, repeats=3, warmup=1, sandbox_parent=tmp_path, root=root)
    assert [entry.name for entry in root.iterdir()] == ["sentinel.json"]


def test_breakdown_leaves_no_sandbox_behind(tmp_path: Path) -> None:
    """Песочница удаляется: 4 цикла по 5 записей на каждый прогон не должны копиться."""
    root = tmp_path / "cache"
    root.mkdir()
    before = {entry.name for entry in tmp_path.iterdir()}
    measure._write_breakdown(entry_bytes=1_000, repeats=2, warmup=1, sandbox_parent=tmp_path, root=root)
    assert {entry.name for entry in tmp_path.iterdir()} == before


# ---------------------------------------------------------------- CLI


def test_main_skips_the_breakdown_on_request(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    code = measure.main(
        ["--root", str(root), "--bench-files", "0", "--state-db", str(tmp_path / "nope.db"),
         "--skip-fsync"]
    )
    assert code == 0


def test_main_rejects_zero_repeats(tmp_path: Path) -> None:
    """`repeats=0` дал бы `None` под видом медианы — это отказ, а не замер."""
    root = tmp_path / "cache"
    root.mkdir()
    assert measure.main(["--root", str(root), "--bench-files", "0", "--fsync-repeats", "0"]) == 2


def test_main_rejects_a_negative_warmup(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    assert measure.main(["--root", str(root), "--bench-files", "0", "--fsync-warmup", "-1"]) == 2


def test_main_missing_root_is_exit_code_2(tmp_path: Path) -> None:
    assert measure.main(["--root", str(tmp_path / "absent"), "--bench-files", "0"]) == 2


def test_defaults_declare_the_measured_conditions() -> None:
    """Дефолты — это условия опубликованного числа, поэтому они проверяются."""
    args = measure._parse_args(["--root", "/x"])
    assert args.entry_bytes == 5_169  # p50 реального кэша (замер 15.11.2026)
    assert args.fsync_repeats == 50
    assert args.fsync_warmup == 20
    assert args.skip_fsync is False
