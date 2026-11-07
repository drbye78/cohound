"""S2 плана 23: ключ кэша члена контейнера — хеш **его байтов**, и один ``ZipFile`` на архив.

Проверяется то, ради чего стадия существует: одинаковые байты члена не конвертируются
второй раз (в том числе в другом архиве и в другом прогоне), а разные члены одного архива
**никогда** не получают чужой текст. Прежний выводимый ключ (``stem`` временного файла
``member-N.bin``) — это номер по порядку обхода, а не содержимое: при включённом кэше он
обслужил бы второй член конвертом первого, а после смены байтов архива — прежним текстом.

Второй сюжет — форма чтения zip: ``open_member`` держит **один** ``ZipFile`` на архив
(индекс не перечитывается на каждого члена), ``list_members`` тел не распаковывает, а
архив закрывается на исчерпании и на брошенной итерации.

Данные нейтральны (§1.3): продуктов/вендоров нет, только «архив» и «член».
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest

from core.adapters.container_zip import ZipContainer
from core.domain import DocumentLevel
from core.pipeline.archive_descent import MemberTask
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.extract_consume import member_extract_request
from core.pipeline.extraction_runtime import _run_extract
from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.util.hashing import content_hash

LEVEL = DocumentLevel.TEXT
ARCHIVE_URL = "https://docs.test/files/a.zip"


class _FileExtractor:
    """Экстрактор-заглушка: текст = байты файла, версия фиксирована (штамп документа).

    Читает ``page.source_path`` — тот же носитель, что у настоящего адаптера (I19), поэтому
    подмена ключа кэша видна как подмена текста, а не как подмена способа доставки.
    """

    version = "extractor-member-1"
    supports_source_path = True

    def __init__(self) -> None:
        self.calls: list[str] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append(page.url)
        data = Path(page.source_path or "").read_bytes()
        return ExtractedDoc(
            text=data.decode("utf-8", errors="replace"),
            content_hash=content_hash(data),
            extractor_version=self.version,
        )


def _member_task(
    temp_path: Path,
    data: bytes,
    *,
    member_path: str = "doc.txt",
    archive_url: str = ARCHIVE_URL,
) -> MemberTask:
    """Задание члена: ``content_hash`` — хеш байтов, как его отдаёт спуск."""
    return MemberTask(
        product_id="p1",
        root_url=archive_url,
        archive_url=archive_url,
        archive_content_hash=content_hash(b"archive-bytes"),
        archive_path=str(temp_path.parent / "archive.bin"),
        member_path=member_path,
        member_url=f"{archive_url}!/{member_path}",
        temp_path=str(temp_path),
        suffix=".txt",
        declared_size=len(data),
        content_hash=content_hash(data),
        depth=1,
    )


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


# --------------------------------------------------------------------------- #
# (1) Ключ кэша члена — хеш байтов, а не номер временного файла
# --------------------------------------------------------------------------- #


def test_member_cache_key_is_bytes_hash_not_temp_name(tmp_path: Path) -> None:
    """Два разных члена с одним именем temp-файла не делят запись кэша.

    Сценарий — ровно дефект выводимого ключа: один и тот же ``member-1.bin`` (имя — номер
    обхода, а не содержимое) сначала содержит ``alpha``, потом ``beta``. С явным ключом
    вторая конверсия — промах и даёт ``beta``; с ключом-``stem`` она была бы попаданием и
    вернула бы текст ``alpha``, то есть чужой текст другого документа.
    """
    cache_root = tmp_path / "extract_cache"
    member_file = tmp_path / "member-1.bin"
    extractor = _FileExtractor()

    member_file.write_bytes(b"alpha body")
    alpha = _member_task(member_file, b"alpha body")
    alpha_request = member_extract_request(
        alpha, fetched_at="2026-01-01T00:00:00+00:00", lang=None, level=LEVEL,
        cache_root=str(cache_root),
    )
    assert alpha_request.cache_key == content_hash(b"alpha body")
    assert alpha_request.raw_path == str(member_file)
    first = _run_extract(extractor, alpha_request)
    assert first.text == "alpha body"

    # Тот же путь temp-файла, другие байты: ключ обязан идти за содержимым.
    member_file.write_bytes(b"beta body")
    beta = _member_task(member_file, b"beta body")
    beta_request = member_extract_request(
        beta, fetched_at="2026-01-01T00:00:00+00:00", lang=None, level=LEVEL,
        cache_root=str(cache_root),
    )
    assert beta_request.cache_key != alpha_request.cache_key
    second = _run_extract(extractor, beta_request)

    assert second.text == "beta body", "член получил текст другого члена (ключ = имя файла)"
    assert len(extractor.calls) == 2, "промах обязан дойти до экстрактора"

    # Каждый член — своя запись: по хешу байтов читается ровно свой текст.
    cache = ExtractionCache(cache_root)
    cached_alpha = cache.get(alpha_request.cache_key, extractor.version, LEVEL.value)
    cached_beta = cache.get(beta_request.cache_key, extractor.version, LEVEL.value)
    assert cached_alpha is not None and cached_alpha.text == "alpha body"
    assert cached_beta is not None and cached_beta.text == "beta body"


def test_same_member_bytes_in_two_archives_hit_one_cache_entry(tmp_path: Path) -> None:
    """Одинаковые байты члена из разных архивов — одна запись и ноль конверсий.

    Имя temp-файла (``member-9.bin``) и адрес архива другие, содержимое — то же: ключ
    обязан совпасть, иначе недельный тираж конвертирует повторно те же байты (ради этого
    стадия и делается).
    """
    cache_root = tmp_path / "extract_cache"
    extractor = _FileExtractor()

    first_file = tmp_path / "member-1.bin"
    first_file.write_bytes(b"shared body")
    first_task = _member_task(first_file, b"shared body")
    first_request = member_extract_request(
        first_task, fetched_at="2026-01-01T00:00:00+00:00", lang=None, level=LEVEL,
        cache_root=str(cache_root),
    )
    first = _run_extract(extractor, first_request)
    assert first.text == "shared body"
    assert len(extractor.calls) == 1

    # Другой архив, другой номер temp-файла, те же байты.
    other_file = tmp_path / "member-9.bin"
    other_file.write_bytes(b"shared body")
    other_task = _member_task(
        other_file,
        b"shared body",
        member_path="nested/copy.txt",
        archive_url="https://docs.test/files/b.zip",
    )
    other_request = member_extract_request(
        other_task, fetched_at="2026-02-02T00:00:00+00:00", lang=None, level=LEVEL,
        cache_root=str(cache_root),
    )
    assert other_request.cache_key == first_request.cache_key
    reused = _run_extract(extractor, other_request)

    assert reused.text == "shared body"
    assert len(extractor.calls) == 1, "повторные байты не должны дойти до экстрактора"


def test_member_cache_key_survives_extractor_version_invalidation(tmp_path: Path) -> None:
    """Конверт другого движка — промах: ключ включает версию экстрактора, а не только байты."""
    cache_root = tmp_path / "extract_cache"
    member_file = tmp_path / "member-1.bin"
    member_file.write_bytes(b"body")

    old = _FileExtractor()
    task = _member_task(member_file, b"body")
    request = member_extract_request(
        task, fetched_at="2026-01-01T00:00:00+00:00", lang=None, level=LEVEL,
        cache_root=str(cache_root),
    )
    _run_extract(old, request)

    class _Newer(_FileExtractor):
        version = "extractor-member-2"

    newer = _Newer()
    _run_extract(newer, request)

    assert len(old.calls) == 1
    assert len(newer.calls) == 1, "конверт прежнего движка не обслуживает новый движок"


# --------------------------------------------------------------------------- #
# (2) Один ZipFile на архив; листинг тел не читает; архив закрывается
# --------------------------------------------------------------------------- #


def test_zip_open_member_opens_one_archive_for_all_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``open_member`` не перечитывает central directory на каждого члена (O(n) вместо O(n²))."""
    archive_path = tmp_path / "bundle.zip"
    archive_path.write_bytes(
        _zip({"a.txt": b"aaa", "b.txt": b"bbb", "c.txt": b"ccc"})
    )

    real_zipfile = zipfile.ZipFile
    constructed: list[zipfile.ZipFile] = []
    opened: list[str] = []

    class _Spy(real_zipfile):  # type: ignore[misc]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            constructed.append(self)

        def open(self, name: Any, *args: Any, **kwargs: Any) -> Any:
            opened.append(name)
            return super().open(name, *args, **kwargs)

    monkeypatch.setattr(zipfile, "ZipFile", _Spy)
    port = ZipContainer()

    listing = port.list_members(str(archive_path))
    assert [m.path for m in listing.members] == ["a.txt", "b.txt", "c.txt"]
    assert opened == [], "листинг обязан читать только индекс, а не тела членов"

    for member, expected in zip(
        listing.members, (b"aaa", b"bbb", b"ccc"), strict=True
    ):
        assert b"".join(port.open_member(str(archive_path), member.path)) == expected

    # Один экземпляр на листинг + один общий на все тела членов.
    assert len(constructed) == 2, (
        f"ожидался один ZipFile на архив, открыто: {len(constructed)}"
    )

    # Конец обхода (его зовёт ``ArchiveDescentWalker``) закрывает хендлы.
    port.close()
    assert port._open == {}
    assert all(archive.fp is None for archive in constructed), "архивы обязаны быть закрыты"


def test_zip_concurrent_member_readers_share_one_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Два одновременных читателя одного архива — один ``ZipFile``, закрытие не под живым.

    Счётчик, а не «закрыть после генератора»: порт не запрещает параллельное чтение
    членов, поэтому первый завершившийся читатель не вправе закрыть архив второму.
    """
    archive_path = tmp_path / "bundle.zip"
    archive_path.write_bytes(_zip({"a.txt": b"aaa", "b.txt": b"bbb"}))

    real_zipfile = zipfile.ZipFile
    constructed: list[zipfile.ZipFile] = []

    class _Spy(real_zipfile):  # type: ignore[misc]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            constructed.append(self)

    monkeypatch.setattr(zipfile, "ZipFile", _Spy)
    port = ZipContainer()
    first = port.open_member(str(archive_path), "a.txt")
    second = port.open_member(str(archive_path), "b.txt")
    assert next(first) == b"aaa"
    assert next(second) == b"bbb"

    first.close()  # брошенный первый читатель не имеет права закрыть архив второму
    assert b"".join(second) == b""
    assert len(constructed) == 1
    second.close()
    port.close()
    assert constructed[0].fp is None


def test_zip_member_iteration_abandonment_does_not_leak_reader(tmp_path: Path) -> None:
    """Брошенная итерация возвращает ссылку: хендл не остаётся занятым живым читателем."""
    archive_path = tmp_path / "bundle.zip"
    archive_path.write_bytes(_zip({"a.txt": b"aaa"}))

    port = ZipContainer()
    stream = port.open_member(str(archive_path), "a.txt")
    assert next(stream) == b"aaa"
    stream.close()

    assert port._open, "архив ещё не закрыт: закрытие — конец обхода, а не конец члена"
    assert all(readers == 0 for _archive, readers in port._open.values()), (
        "ссылка брошенного читателя обязана вернуться в ``finally``"
    )
    port.close()
    assert port._open == {}