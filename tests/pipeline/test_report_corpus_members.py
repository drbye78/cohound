"""Члены контейнеров, не вошедшие в корпус: ``outputs/corpus_members.csv`` и сводка (B3).

Вопрос «почему этого файла из архива нет в корпусе» должен иметь ответ в ``outputs/``,
а не только в таблице прогона: строка с ``skip_reason`` — это видимый отказ, а не тихое
исчезновение. Тесты нейтральны (§1.3), источник — фейковый ``store`` с тем же контрактом,
что у ``StorePort.iter_corpus_members``.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

from core.domain import CorpusMember
from core.pipeline.corpus_members import (
    CSV_COLUMNS,
    corpus_members_summary,
    write_corpus_members_artifact,
)

RUN_ID = "run-members"


class _FakeStore:
    """Минимальный читатель членов контейнеров: ровно нужный артефакту метод."""

    def __init__(self, members: list[CorpusMember]) -> None:
        self.members = members

    def iter_corpus_members(self) -> Iterator[CorpusMember]:
        yield from self.members


def _member(
    member_path: str = "docs/manual.pdf",
    *,
    product_id: str = "p1",
    archive_content_hash: str = "sha256:archive-a",
    skip_reason: str | None = "member_too_large",
    **overrides: object,
) -> CorpusMember:
    base: dict[str, object] = {
        "archive_content_hash": archive_content_hash,
        "member_path": member_path,
        "product_id": product_id,
        "archive_url": f"https://docs.test/{product_id}/bundle.zip",
        "member_url": f"https://docs.test/{product_id}/bundle.zip!/{member_path}",
        "member_format": "pdf",
        "declared_size": 4096,
        "skip_reason": skip_reason,
        "depth": 1,
    }
    base.update(overrides)
    return CorpusMember(**base)  # type: ignore[arg-type]


def test_artifact_writes_only_rows_with_skip_reason(tmp_path: Path) -> None:
    """В таблице — только пропущенные члены: принятый документ там был бы шумом."""
    store = _FakeStore(
        [
            _member("docs/a.pdf", skip_reason="member_too_large", declared_size=99),
            _member("docs/b.pdf", skip_reason=None, doc_id="p1:doc-b"),
            _member("docs/c.pdf", skip_reason="unsupported_member_format"),
        ]
    )
    path = write_corpus_members_artifact(store, RUN_ID, tmp_path / "corpus_members.csv")

    assert path is not None
    with path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["member_path"] for row in rows] == ["docs/a.pdf", "docs/c.pdf"]
    assert tuple(rows[0]) == CSV_COLUMNS
    assert rows[0]["declared_size"] == "99"
    assert rows[1]["skip_reason"] == "unsupported_member_format"
    assert rows[0]["archive_url"] == "https://docs.test/p1/bundle.zip"
    assert rows[0]["depth"] == "1"


def test_artifact_is_deterministic_and_prints_unmeasured_size_as_empty(
    tmp_path: Path,
) -> None:
    """Порядок строк — ``(product_id, archive_content_hash, member_path)``; ``None`` ≠ ``0``."""
    store = _FakeStore(
        [
            _member("b/x.zip", declared_size=None),
            _member("a/x.zip"),
            _member("a/x.zip", product_id="p2", archive_content_hash="sha256:archive-b"),
        ]
    )
    target = tmp_path / "corpus_members.csv"
    path = write_corpus_members_artifact(store, RUN_ID, target)

    assert path == target
    with target.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [(row["product_id"], row["member_path"]) for row in rows] == [
        ("p1", "a/x.zip"),
        ("p1", "b/x.zip"),
        ("p2", "a/x.zip"),
    ]
    assert rows[1]["declared_size"] == ""
    assert rows[0]["declared_size"] == "4096"


def test_artifact_is_not_created_without_skipped_members(tmp_path: Path) -> None:
    """Пустой набор — не артефакт: файла нет, а не таблица с одной шапкой."""
    target = tmp_path / "corpus_members.csv"
    store = _FakeStore([_member(skip_reason=None, doc_id="p1:doc")])

    assert write_corpus_members_artifact(store, RUN_ID, target) is None
    assert not target.exists()


def test_rows_of_another_run_are_not_counted_in_shared_db() -> None:
    """Общая ``--db``: строка чужого прогона не выдаётся за пропуск этого.

    ``iter_corpus_members`` не фильтрует по прогону, поэтому отбор делает артефакт:
    свой ``run_id`` — да, чужой — нет, метки нет (``None``) — да, потому что терять
    измеренный факт из-за отсутствия метки нельзя.
    """
    store = _FakeStore(
        [
            _member("docs/a.pdf", run_id=RUN_ID),
            _member("docs/b.pdf", run_id="run-other"),
            _member("docs/legacy.pdf", run_id=None),
        ]
    )

    summary = corpus_members_summary(store, RUN_ID)

    assert summary["members_without_document"] == 2
    assert summary["by_product_reason"] == {"p1": {"member_too_large": 2}}


def test_summary_counts_by_product_and_reason_with_planned_zeroes() -> None:
    """Сводка: счёт по продуктам/причинам + нули для продуктов плана (симметрия)."""
    store = _FakeStore(
        [
            _member("docs/a.pdf", skip_reason="member_too_large"),
            _member("docs/b.pdf", skip_reason="member_too_large"),
            _member("docs/c.pdf", product_id="p2", skip_reason="archive_cycle"),
            _member("docs/d.pdf", skip_reason=None, doc_id="p1:doc-d"),
        ]
    )

    summary = corpus_members_summary(store, RUN_ID, products=["p1", "p2", "p3"])

    assert summary["members_without_document"] == 3
    assert summary["by_product"] == {"p1": 2, "p2": 1, "p3": 0}
    assert summary["by_reason"] == {"archive_cycle": 1, "member_too_large": 2}
    assert summary["by_product_reason"] == {
        "p1": {"member_too_large": 2},
        "p2": {"archive_cycle": 1},
    }


def test_summary_without_skipped_members_is_zeroed_but_not_empty() -> None:
    """Пропусков нет — словарь есть: нули по продуктам плана, причины не выдумываются."""
    summary = corpus_members_summary(
        _FakeStore([_member(skip_reason=None, doc_id="p1:doc")]),
        RUN_ID,
        products=["p1", "p2"],
    )

    assert summary == {
        "members_without_document": 0,
        "by_product": {"p1": 0, "p2": 0},
        "by_reason": {},
        "by_product_reason": {},
    }


def test_store_absent_method_broken_or_without_run_id_gives_empty_summary(
    tmp_path: Path,
) -> None:
    """Отчёт вторичен к фазе: нет источника — пустая сводка, а не исключение."""

    class _Broken:
        def iter_corpus_members(self) -> Iterator[CorpusMember]:
            raise RuntimeError("db closed")

    class _NoMethod:
        """Store без нужного метода (старый/чужой порт)."""

    expected = {
        "members_without_document": 0,
        "by_product": {},
        "by_reason": {},
        "by_product_reason": {},
    }
    for store in (None, _Broken(), _NoMethod()):
        assert corpus_members_summary(store, RUN_ID) == expected
        assert (
            write_corpus_members_artifact(store, RUN_ID, tmp_path / "x.csv") is None
        )
    assert not (tmp_path / "x.csv").exists()
    # Пустой ``run_id`` — «источника нет», а не повод читать чужую проекцию БД.
    live = _FakeStore([_member()])
    assert corpus_members_summary(live, "") == expected
    assert write_corpus_members_artifact(live, None, tmp_path / "y.csv") is None
    assert not (tmp_path / "y.csv").exists()