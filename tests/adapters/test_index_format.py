"""Тесты формата индексной сборки (02 §B.2, 09 §8.2): раскладка и ``is_committed``.

Проверяются: служебный ``_compositions`` не считается сборкой/партицией и
``is_committed`` требует все три артефакта **и** читаемый манифест. Файлы создаются
вручную — тест не зависит от ``numpy``. Нейтрален (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path

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
