"""Тесты формата индексной сборки (02 §B.2, 09 §8.2): раскладка и ``is_committed``.

Проверяются: служебный ``_compositions`` не считается сборкой/партицией,
``is_committed`` требует все три артефакта **и** читаемый манифест, обе формы
``chunk_ids.jsonl`` (``partition-2`` с ``content_hash`` и исходная) читаются, а
несовпадение длин ``content_hashes``/``chunk_ids`` отвергается. Файлы создаются
вручную — тест не зависит от ``numpy``. Нейтрален (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.adapters import index_format
from core.util.index_layout import COMPOSITIONS_DIR

PKG = "pkg"
BUILD = "ib_1"
PRODUCT = "p1"


def _partition(root: Path, *, manifest: str | None = None) -> Path:
    """Создаёт каталог партиции с тремя артефактами (manifest по умолчанию валиден)."""
    target = index_format.build_root(root, PKG, BUILD, PRODUCT)
    target.mkdir(parents=True)
    (target / "vectors.fp16.npy").write_bytes(b"not-a-real-npy")
    (target / "chunk_ids.jsonl").write_text('{"chunk_id": "c1"}\n', encoding="utf-8")
    (target / "manifest.json").write_text(
        manifest if manifest is not None else json.dumps({"dim": 2}),
        encoding="utf-8",
    )
    return target


# ---------------------------------------------------------------------------
# _compositions — не сборка и не партиция
# ---------------------------------------------------------------------------


def test_list_build_ids_excludes_compositions(tmp_path: Path) -> None:
    (tmp_path / PKG / BUILD).mkdir(parents=True)
    compositions = index_format.compositions_dir(tmp_path, PKG)
    compositions.mkdir(parents=True)
    (compositions / "cb_x.json").write_text("{}", encoding="utf-8")

    assert index_format.list_build_ids(tmp_path, PKG) == [BUILD]
    assert compositions.name == COMPOSITIONS_DIR


def test_list_partitions_excludes_compositions(tmp_path: Path) -> None:
    build_dir = tmp_path / PKG / BUILD
    build_dir.mkdir(parents=True)
    (build_dir / PRODUCT).mkdir()
    (build_dir / COMPOSITIONS_DIR).mkdir()  # страховка от ошибочной раскладки

    assert index_format.list_partitions(tmp_path, PKG, BUILD) == [PRODUCT]


def test_compositions_dir_path(tmp_path: Path) -> None:
    assert index_format.compositions_dir(tmp_path, PKG) == tmp_path / PKG / "_compositions"


# ---------------------------------------------------------------------------
# is_committed
# ---------------------------------------------------------------------------


def test_is_committed_true_with_all_artifacts(tmp_path: Path) -> None:
    _partition(tmp_path)
    assert index_format.is_committed(tmp_path, PKG, BUILD, PRODUCT)


def test_is_committed_false_when_artifact_missing(tmp_path: Path) -> None:
    target = _partition(tmp_path)
    (target / "manifest.json").unlink()
    assert not index_format.is_committed(tmp_path, PKG, BUILD, PRODUCT)


def test_is_committed_false_when_manifest_unreadable(tmp_path: Path) -> None:
    _partition(tmp_path, manifest="{broken")
    assert not index_format.is_committed(tmp_path, PKG, BUILD, PRODUCT)


def test_is_committed_false_when_partition_absent(tmp_path: Path) -> None:
    assert not index_format.is_committed(tmp_path, PKG, BUILD, PRODUCT)


# ---------------------------------------------------------------------------
# Атомарность записи партиции (C10) — нужен numpy (write_partition)
# ---------------------------------------------------------------------------

try:  # необязательная зависимость (extra ``retrieval``)
    import numpy as np  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - зависит от окружения
    np = None  # type: ignore[assignment]

requires_numpy = pytest.mark.skipif(np is None, reason="numpy не установлен (extra retrieval)")


def _write(root: Path, chunk_ids: list[str], tag: float = 1.0) -> Path:
    return index_format.write_partition(
        root,
        PKG,
        BUILD,
        PRODUCT,
        [[tag, tag] for _ in chunk_ids],
        chunk_ids,
        {"dim": 2, "chunk_ids": chunk_ids},
    )


def _leftovers(root: Path) -> list[str]:
    """Служебные каталоги рядом с партицией: staging или отведённый «мусор»."""
    parent = index_format.build_root(root, PKG, BUILD, PRODUCT).parent
    return sorted(p.name for p in parent.iterdir() if p.name.startswith("."))


@requires_numpy
def test_rewrite_replaces_partition_and_leaves_no_debris(tmp_path: Path) -> None:
    """Перезапись существующей партиции не оставляет staging/мусора (C10)."""
    _write(tmp_path, ["c1", "c2"])
    target = _write(tmp_path, ["c9"])

    assert index_format.read_partition(tmp_path, PKG, BUILD, PRODUCT).chunk_ids == ["c9"]
    assert target.is_dir()
    assert _leftovers(tmp_path) == []


@requires_numpy
def test_write_partition_failure_keeps_previous_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Сбой финального ``os.replace`` возвращает прежнюю партицию, а не теряет её (C10)."""
    _write(tmp_path, ["c1", "c2"])
    real_replace = index_format.os.replace
    calls: list[tuple[str, str]] = []

    def flaky_replace(src, dst):  # noqa: ANN001, ANN202 - подмена сигнатуры os.replace
        calls.append((str(src), str(dst)))
        if len(calls) == 2:  # 1-й — увод прежней партиции, 2-й — вставка новой
            raise OSError("имитация сбоя replace")
        return real_replace(src, dst)

    monkeypatch.setattr(index_format.os, "replace", flaky_replace)

    with pytest.raises(OSError):
        _write(tmp_path, ["cX"])

    # Прежняя партиция на месте и читается целиком (не «наполовину удалена»).
    assert index_format.is_committed(tmp_path, PKG, BUILD, PRODUCT)
    assert index_format.read_partition(tmp_path, PKG, BUILD, PRODUCT).chunk_ids == ["c1", "c2"]
    assert _leftovers(tmp_path) == []


# ---------------------------------------------------------------------------
# ``content_hash`` в ``chunk_ids.jsonl`` (формат ``partition-2``, 16 §5.2)
# ---------------------------------------------------------------------------


def _rows(root: Path) -> list[dict[str, object]]:
    path = index_format.build_root(root, PKG, BUILD, PRODUCT) / "chunk_ids.jsonl"
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@requires_numpy
def test_write_partition_records_content_hashes_when_given(tmp_path: Path) -> None:
    """Переданные ``content_hashes`` попадают в строки рядом с ``chunk_id``."""
    index_format.write_partition(
        tmp_path,
        PKG,
        BUILD,
        PRODUCT,
        [[1.0, 0.0], [0.0, 1.0]],
        ["c1", "c2"],
        {"dim": 2},
        content_hashes=["h1", "h2"],
    )
    assert _rows(tmp_path) == [
        {"chunk_id": "c1", "content_hash": "h1"},
        {"chunk_id": "c2", "content_hash": "h2"},
    ]


@requires_numpy
def test_write_partition_without_content_hashes_keeps_legacy_form(tmp_path: Path) -> None:
    """Без ``content_hashes`` формат прежний: старая партиция остаётся читаемой."""
    _write(tmp_path, ["c1", "c2"])
    assert _rows(tmp_path) == [{"chunk_id": "c1"}, {"chunk_id": "c2"}]
    assert index_format.read_partition(tmp_path, PKG, BUILD, PRODUCT).chunk_ids == [
        "c1",
        "c2",
    ]


@requires_numpy
def test_write_partition_rejects_content_hashes_length_mismatch(tmp_path: Path) -> None:
    """Длина ``content_hashes`` обязана совпадать с ``chunk_ids`` (как у ``vectors``)."""
    with pytest.raises(ValueError):
        index_format.write_partition(
            tmp_path,
            PKG,
            BUILD,
            PRODUCT,
            [[1.0, 0.0], [0.0, 1.0]],
            ["c1", "c2"],
            {"dim": 2},
            content_hashes=["h1"],
        )


@requires_numpy
def test_reader_accepts_both_chunk_id_forms(tmp_path: Path) -> None:
    """Читатель не требует ``content_hash``: обе формы дают те же ``chunk_ids``."""
    _write(tmp_path, ["c1", "c2"])
    legacy = index_format.read_partition(tmp_path, PKG, BUILD, PRODUCT)
    index_format.write_partition(
        tmp_path,
        PKG,
        BUILD,
        PRODUCT,
        [[1.0, 0.0], [0.0, 1.0]],
        ["c1", "c2"],
        {"dim": 2},
        content_hashes=["h1", "h2"],
    )
    current = index_format.read_partition(tmp_path, PKG, BUILD, PRODUCT)
    assert current.chunk_ids == legacy.chunk_ids == ["c1", "c2"]
