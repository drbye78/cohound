"""Частичный исход члена контейнера: законная форма **с обязательным свидетельством**.

Бинарная модель «ингестнут/пропущен» выдавливала ``PARTIAL_SUCCESS`` в «ингестнут» без
следа: член, прочитанный на одной странице из семи, выглядел в провенансе спуска так же,
как прочитанный целиком. Здесь проверяется обратное:

* частичный исход **законен** и виден в наблюдении (``members_partial`` с причинами);
* полный исход не подменяется частичным (счётчик не растёт на полных членах);
* частичный исход **без свидетельства** отвергается громко (``ValueError``), а не молча.

Данные нейтральны (§1.3); архив — настоящий zip, собранный ``zipfile`` в ``tmp_path``.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest

from core.adapters.container_any import container_for
from core.domain import ArchiveDescent
from core.pipeline.archive_descent import (
    ArchiveDescentWalker,
    DescentBudget,
    MemberOutcome,
)
from core.util.hashing import content_hash

ZIP_URL = "https://docs.test/files/manual.zip"
RUN_ID = "run-member-partial"


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _descend(tmp_path: Path, ingest: Any) -> tuple[list[Any], list[Any], list[str]]:
    """Один спуск в zip из трёх md-членов; ``ingest`` — обработчик члена."""
    archive = _zip({"a.md": b"a", "b.md": b"b", "c.md": b"c"})
    path = tmp_path / "archive.bin"
    path.write_bytes(archive)
    observations: list[Any] = []
    members: list[Any] = []
    errors: list[str] = []
    walker = ArchiveDescentWalker(
        product_id="p1",
        root_url=ZIP_URL,
        descent=ArchiveDescent.FULL,
        budget=DescentBudget(max_member_bytes=1024 * 1024),
        container_for=container_for,
        ingest_member=ingest,
        observations=observations,
        members=members,
        errors=errors,
        run_id=RUN_ID,
    )
    walker.descend(
        archive_path=path,
        archive_url=ZIP_URL,
        archive_content_hash=content_hash(archive),
    )
    return observations, members, errors


# --------------------------------------------------------------------------- #
# Валидация исхода
# --------------------------------------------------------------------------- #


def test_partial_outcome_with_evidence_is_legal() -> None:
    """Частичный член с документом и причиной — законный исход, не отказ."""
    outcome = MemberOutcome(
        doc_id="p1:hash",
        content_hash="h" * 64,
        partial=True,
        partial_reason="docling: PARTIAL_SUCCESS, получено страниц 1 из 7",
    )

    assert outcome.partial is True
    assert outcome.doc_id is not None
    assert outcome.skip_reason is None


def test_partial_without_document_is_rejected() -> None:
    """``partial`` без ``doc_id`` — «прочитал часть» без прочитанного: отказ, не тишина."""
    with pytest.raises(ValueError, match="без doc_id"):
        MemberOutcome(
            partial=True,
            partial_reason="docling: PARTIAL_SUCCESS",
        )


def test_partial_without_reason_is_rejected() -> None:
    """Частичный исход обязан называть причину (иначе это прилагательное, а не факт)."""
    with pytest.raises(ValueError, match="без partial_reason"):
        MemberOutcome(doc_id="p1:hash", partial=True)


def test_reason_without_partial_is_rejected() -> None:
    """Причина частичности при полном исходе — ложная провенанс-запись."""
    with pytest.raises(ValueError, match="partial=False"):
        MemberOutcome(doc_id="p1:hash", partial_reason="docling: PARTIAL_SUCCESS")


def test_ingested_and_skipped_are_mutually_exclusive() -> None:
    """Член не может быть одновременно ингестнут и пропущен."""
    with pytest.raises(ValueError, match="одновременно"):
        MemberOutcome(doc_id="p1:hash", skip_reason="extract_failed")


# --------------------------------------------------------------------------- #
# Наблюдение спуска
# --------------------------------------------------------------------------- #


def test_partial_members_are_counted_and_named(tmp_path: Path) -> None:
    """Частичный член виден в исходе спуска; полные в ``members_partial`` не попадают."""
    partial_reason = "docling: PARTIAL_SUCCESS, получено страниц 1 из 7"

    def _ingest(task: Any) -> MemberOutcome:
        # ``a.md`` — прочитан частично, остальные — целиком.
        is_partial = task.member_path == "a.md"
        return MemberOutcome(
            doc_id=f"p1:{task.member_path}",
            content_hash="h" * 64,
            partial=is_partial,
            partial_reason=partial_reason if is_partial else None,
        )

    observations, members, errors = _descend(tmp_path, _ingest)

    assert errors == []
    assert len(members) == 3
    # Член остаётся строкой провенанса с doc_id (свидетельство — сам документ).
    partial_rows = [row for row in members if row.member_path == "a.md"]
    assert partial_rows and partial_rows[0].doc_id == "p1:a.md"
    assert partial_rows[0].skip_reason is None

    assert len(observations) == 1
    outcome = observations[0].outcome
    assert outcome["members_ingested"] == 3
    assert outcome["members_partial"] == 1
    assert outcome["partial"] == {
        partial_reason: {"count": 1, "examples": ["a.md"]}
    }


def test_full_members_are_not_marked_partial(tmp_path: Path) -> None:
    """Полное чтение не подменяется частичным: счётчик нулевой и пустой гистограммой."""
    observations, _members, errors = _descend(
        tmp_path,
        lambda task: MemberOutcome(
            doc_id=f"p1:{task.member_path}", content_hash="h" * 64
        ),
    )

    assert errors == []
    outcome = observations[0].outcome
    assert outcome["members_ingested"] == 3
    assert outcome["members_partial"] == 0
    assert outcome["partial"] == {}
