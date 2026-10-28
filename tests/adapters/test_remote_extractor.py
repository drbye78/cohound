"""Тесты извлечения **по артефакту** в шве core→edge (B1/B2/B3; 10 §5.4).

Сеть отсутствует: ``RemoteExtractor`` работает поверх тестового двойника
``ContentGateway`` (ops ``extract/upload|submit|collect|abort``), который **собирает
байты из чанков**, как настоящий staging на edge. Это позволяет проверять то, что
нельзя проверить «на глаз»:

* ядро передаёт ровно те байты артефакта и адресует их их же ``content_hash`` (I19);
* файл читается с диска чанками (перезапись файла после создания ``RawPage`` меняет
  отправленное — буфера в памяти нет);
* страница идёт прежним путём ``/v1/fetch`` (один вызов, без загрузки);
* идентичность экстрактора приходит **от edge**, а не от прокси (B2 — иначе ``pb``
  под ``split`` разошёлся бы с ``local`` по построению);
* ``level``/``device`` едут в ``extract/submit`` (B3), ``pending`` опрашивается,
  ``error`` виден как сбой, дедлайн снимает загрузку ``abort``-ом.

Фикстуры категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from core.adapters.remote_extractor import (
    DEFAULT_UPLOAD_CHUNK_BYTES,
    RemoteExtractor,
)
from core.domain.enums import DocumentLevel, PdfDevice
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractionRoute, RawPage
from core.util.hashing import content_hash

_TEXT = "# clean markdown"
_EDGE_EXTRACTOR = "docling-doc-3:full/cpu/docling-2.71.0"


class _Edge:
    """Двойник edge-операций ``extract/*``: staging собирается из чанков, как на edge.

    :param text: очищенный markdown, который edge «извлёк».
    :param identity: идентичность экстрактора edge (то же пространство имён, что local).
    :param statuses: очередь статусов ``collect``; пусто — всегда ``done``.
    :param accepted: ответ ``extract/submit`` (``accepted``).
    :param complete: подмена ``complete`` в ответе ``upload`` (проверка неполной загрузки).
    """

    schema_version = "1"

    def __init__(
        self,
        *,
        text: str = _TEXT,
        identity: str | None = _EDGE_EXTRACTOR,
        statuses: list[str] | None = None,
        always_pending: bool = False,
        accepted: bool = True,
        complete: bool | None = None,
        capabilities: Mapping[str, Any] | None = None,
    ) -> None:
        self.calls: list[tuple[str, Mapping[str, Any]]] = []
        self.received: dict[str, dict[int, bytes]] = {}
        self.declared_total: dict[str, int] = {}
        self.text = text
        self.identity = identity
        self._statuses = list(statuses or [])
        self._always_pending = always_pending
        self._accepted = accepted
        self._complete = complete
        self._capabilities = dict(capabilities or {})

    # -- ContentGateway -------------------------------------------------------- #

    def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        self.calls.append((op, dict(payload)))
        if op == "extract/upload":
            return self._upload(payload)
        if op == "extract/submit":
            return {
                "content_hash": payload["content_hash"],
                "accepted": self._accepted,
                "extractor_version": self.identity,
                "error": None if self._accepted else "artefact rejected",
            }
        if op == "extract/collect":
            return self._collect(payload)
        if op == "extract/abort":
            self.received.pop(str(payload["content_hash"]), None)
            return {"content_hash": payload["content_hash"], "aborted": True, "removed_bytes": 0}
        raise AssertionError(f"неожидаемая операция шва: {op}")

    def capabilities(self) -> Mapping[str, Any]:
        return dict(self._capabilities)

    # -- наблюдения ------------------------------------------------------------ #

    def ops(self) -> list[str]:
        """Последовательность операций шва (в порядке вызова)."""
        return [op for op, _ in self.calls]

    def payloads(self, op: str) -> list[Mapping[str, Any]]:
        """Payload-ы одной операции (по порядку)."""
        return [payload for name, payload in self.calls if name == op]

    def artifact(self, digest: str) -> bytes:
        """Байты, собранные из принятых чанков (как это сделал бы edge)."""
        chunks = self.received.get(digest, {})
        return b"".join(chunks[index] for index in sorted(chunks))

    # -- операции -------------------------------------------------------------- #

    def _upload(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        digest = str(payload["content_hash"])
        bucket = self.received.setdefault(digest, {})
        bucket[int(payload["chunk_no"])] = base64.b64decode(payload["data_b64"])
        total = int(payload["chunk_total"])
        self.declared_total[digest] = total
        received_bytes = sum(len(block) for block in bucket.values())
        complete = len(bucket) == total if self._complete is None else self._complete
        return {
            "content_hash": digest,
            "received_chunks": len(bucket),
            "chunk_total": total,
            "received_bytes": received_bytes,
            "complete": complete,
            "expires_at": None,
        }

    def _collect(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        digest = str(payload["content_hash"])
        if self._always_pending:
            status = "pending"
        elif self._statuses:
            status = self._statuses.pop(0)
        else:
            status = "done"
        if status == "done":
            return {
                "content_hash": digest,
                "status": "done",
                "text": {
                    "content": self.text,
                    "hash": content_hash(self.text),
                    "extractor_version": self.identity,
                    "page_count": 3,
                },
                "extractor_version": self.identity,
                "error": None,
            }
        return {
            "content_hash": digest,
            "status": status,
            "text": None,
            "extractor_version": self.identity,
            "error": "office reader failed" if status == "error" else None,
        }


def _page(tmp_path: Path, data: bytes) -> RawPage:
    """Документная страница с retained-файлом (как её видит воркер шва).

    ``content`` пуст: ровно так ядро отдаёт документ воркеру (``_run_extract``),
    поэтому тест доказывает чтение **с диска**, а не из буфера памяти.
    """
    path = tmp_path / "doc.pdf"
    path.write_bytes(data)
    return RawPage(
        url="https://docs.test/files/doc.pdf",
        content=b"",
        source_path=str(path),
        fetched_at="T",
        content_type="application/pdf",
    )


def _extractor(edge: _Edge, **kwargs: Any) -> RemoteExtractor:
    """Прокси с нулевой паузой опроса: тест не платит за реальные секунды."""
    kwargs.setdefault("poll_interval_s", 0.0)
    return RemoteExtractor(edge, **kwargs)


# ---------------------------------------------------------------------------
# B1: загрузка по артефакту
# ---------------------------------------------------------------------------


def test_supports_source_path_enables_container_members() -> None:
    """Член контейнера — файл на диске; экстрактор обязан его принимать (Д-1.3)."""
    assert RemoteExtractor(_Edge()).supports_source_path is True
    assert isinstance(RemoteExtractor(_Edge()), ExtractorPort)


def test_document_is_uploaded_in_chunks_matching_artifact_bytes(tmp_path: Path) -> None:
    """Артефакт режется по объявленному edge размеру чанка; байты — те же, адрес — их хеш."""
    data = bytes(range(256)) * 41  # 10 496 байт
    chunk_bytes = 4096
    edge = _Edge(capabilities={"extract": {"max_upload_chunk_bytes": chunk_bytes}})

    doc = _extractor(edge).extract(_page(tmp_path, data))

    digest = content_hash(data)
    uploads = edge.payloads("extract/upload")
    assert len(uploads) == 3  # 4096 + 4096 + 2304
    assert [payload["chunk_no"] for payload in uploads] == [0, 1, 2]
    assert all(payload["chunk_total"] == 3 for payload in uploads)
    assert all(payload["content_hash"] == digest for payload in uploads)
    assert all(payload["size"] == len(data) for payload in uploads)
    # Каждый чанк — ровно свой кусок артефакта (не «весь файл в каждом»).
    for index, payload in enumerate(uploads):
        assert base64.b64decode(payload["data_b64"]) == data[
            index * chunk_bytes : (index + 1) * chunk_bytes
        ]
    # Edge собрал ровно те байты, которые ретенировало ядро (I19).
    assert edge.artifact(digest) == data
    assert doc.error is None
    assert doc.text == _TEXT
    assert doc.content_hash == content_hash(_TEXT)  # пересчитан ядром, не взят у edge


def test_chunk_size_falls_back_to_declared_constant_without_capabilities(
    tmp_path: Path,
) -> None:
    """Старый edge без ``capabilities.extract`` — откат на объявленную константу модуля."""
    data = b"x" * (DEFAULT_UPLOAD_CHUNK_BYTES + 1)
    edge = _Edge(capabilities={"crawler_version": "edge-1"})

    _extractor(edge).extract(_page(tmp_path, data))

    uploads = edge.payloads("extract/upload")
    assert len(uploads) == 2  # константа модуля, а не «один чанк на всё»
    assert all(payload["chunk_total"] == 2 for payload in uploads)
    assert edge.artifact(content_hash(data)) == data


def test_file_is_read_from_disk_not_from_memory_buffer(tmp_path: Path) -> None:
    """Источник — файл: перезапись после создания ``RawPage`` меняет отправленное."""
    page = _page(tmp_path, b"%PDF-1.7 original bytes")
    replaced = b"%PDF-1.7 bytes written after the page was built"
    assert page.source_path is not None
    Path(page.source_path).write_bytes(replaced)

    edge = _Edge()
    _extractor(edge).extract(page)

    digest = content_hash(replaced)
    assert edge.artifact(digest) == replaced
    assert edge.payloads("extract/upload")[0]["size"] == len(replaced)


def test_incomplete_upload_is_a_visible_failure_and_aborts(tmp_path: Path) -> None:
    """Неполная загрузка не превращается в «пустой текст» и снимает staging."""
    edge = _Edge(complete=False)

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert doc.error is not None
    assert "не завершён" in doc.error
    assert "extract/abort" in edge.ops()
    assert "extract/submit" not in edge.ops()  # незавершённый артефакт не просят разбирать


# ---------------------------------------------------------------------------
# Прежний путь для страниц: /v1/fetch
# ---------------------------------------------------------------------------


def test_html_page_still_uses_fetch_and_uploads_nothing() -> None:
    """HTML — байты в ответе ``fetch``; второго обмена (загрузки) нет."""

    class _FetchEdge(_Edge):
        def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
            if op == "fetch":
                self.calls.append((op, dict(payload)))
                return {
                    "final_url": payload["url"],
                    "text": {
                        "content": _TEXT,
                        "hash": content_hash(_TEXT),
                        "extractor_version": "trafilatura-html-2",
                    },
                    "error": None,
                }
            return super().call(op, payload)

    edge = _FetchEdge()
    page = RawPage(
        url="https://docs.test/article",
        content=b"<html>page</html>",
        fetched_at="T",
        content_type="text/html",
    )

    doc = _extractor(edge).extract(page)

    assert edge.ops() == ["fetch"]
    assert edge.payloads("fetch")[0]["extract"] == ExtractionRoute.AUTO.value
    assert doc.text == _TEXT
    assert doc.extractor_version == "trafilatura-html-2"


def test_document_route_without_bytes_falls_back_to_fetch() -> None:
    """Без байтов загружать нечего: маршрут остаётся прежним (``/v1/fetch``)."""

    class _FetchEdge(_Edge):
        def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
            if op == "fetch":
                self.calls.append((op, dict(payload)))
                return {"text": {"content": _TEXT, "extractor_version": "docling-x"}}
            return super().call(op, payload)

    edge = _FetchEdge()
    page = RawPage(url="https://docs.test/files/doc.pdf", content=b"", fetched_at="T")

    _extractor(edge).extract(page, ExtractionRoute.DOCUMENT)

    assert edge.ops() == ["fetch"]


# ---------------------------------------------------------------------------
# B2: идентичность экстрактора — от edge, а не от прокси
# ---------------------------------------------------------------------------


def test_extractor_version_comes_from_edge_not_from_proxy(tmp_path: Path) -> None:
    """``pb`` под ``split`` обязан совпасть с ``local``: версия — из ответа edge."""
    edge = _Edge()

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert doc.extractor_version == _EDGE_EXTRACTOR
    assert doc.extractor_version != "remote-extractor-1"
    assert doc.page_count == 3
    assert edge.payloads("extract/submit")[0]["extractor_declared"] is None


def test_missing_edge_identity_stays_empty_instead_of_named_proxy(tmp_path: Path) -> None:
    """Edge не назвал экстрактор — поле пустое, а не имя прокси (ложный провенанс)."""
    edge = _Edge(identity=None)

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert doc.extractor_version == ""


def test_edge_declaring_no_artifact_protocol_is_a_visible_failure(
    tmp_path: Path,
) -> None:
    """Edge объявил ``supports_artifact=false`` — сбой назван, байты не отправляются.

    Тихий откат на ``/v1/fetch`` вернул бы пере-фетч (и фетч синтетического URL члена
    контейнера) — то есть ровно тот дефект, который закрывает маршрут по артефакту.
    """
    edge = _Edge(
        capabilities={
            "extract": {
                "supports_artifact": False,
                "max_upload_chunk_bytes": 4096,
                "levels": ["full"],
                "devices": ["cpu"],
            }
        }
    )

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert doc.error is not None
    assert "supports_artifact" in doc.error
    assert edge.ops() == []  # ни загрузки, ни фетча: отказ назван до передачи байтов


def test_document_level_off_refuses_without_transferring_bytes(tmp_path: Path) -> None:
    """``document_level=off``: отказ до передачи артефакта, текст — как у local."""
    edge = _Edge()

    doc = _extractor(edge, document_level=DocumentLevel.OFF).extract(
        _page(tmp_path, b"%PDF-1.7 data")
    )

    assert doc.error is not None
    assert "document_level=off" in doc.error
    assert edge.ops() == []


# ---------------------------------------------------------------------------
# Опрос collect, сбои и дедлайн
# ---------------------------------------------------------------------------


def test_pending_is_polled_until_done(tmp_path: Path) -> None:
    edge = _Edge(statuses=["pending", "pending"])

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert len(edge.payloads("extract/collect")) == 3
    assert doc.error is None
    assert doc.text == _TEXT


def test_edge_error_is_a_visible_failure(tmp_path: Path) -> None:
    edge = _Edge(statuses=["error"])

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert doc.text == ""
    assert doc.error is not None
    assert "office reader failed" in doc.error


def test_unknown_status_is_not_waited_on(tmp_path: Path) -> None:
    """``unknown`` — «работы нет», а не «ещё идёт»: иначе ядро ждало бы до дедлайна."""
    edge = _Edge(statuses=["unknown"])

    doc = _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    assert doc.error is not None
    assert "unknown" in doc.error
    assert len(edge.payloads("extract/collect")) == 1


def test_deadline_aborts_upload_and_names_the_reason(tmp_path: Path) -> None:
    """Истёкший дедлайн обязан снять загрузку: брошенный staging не остаётся на edge."""
    edge = _Edge(always_pending=True)  # collect никогда не терминален
    data = b"%PDF-1.7 data"

    doc = _extractor(edge, document_timeout=0.02).extract(_page(tmp_path, data))

    assert doc.error is not None
    assert "дедлайн" in doc.error
    aborts = edge.payloads("extract/abort")
    assert [payload["content_hash"] for payload in aborts] == [content_hash(data)]
    assert content_hash(data) not in edge.received  # staging убран


# ---------------------------------------------------------------------------
# B3: объявленные оси едут в submit
# ---------------------------------------------------------------------------


def test_declared_level_and_device_are_sent_to_edge(tmp_path: Path) -> None:
    """Уровень и устройство — объявленный вход, а не настройка edge по умолчанию."""
    edge = _Edge()

    _extractor(
        edge,
        document_level=DocumentLevel.LAYOUT_TABLES,
        pdf_device=PdfDevice.CUDA,
    ).extract(_page(tmp_path, b"%PDF-1.7 data"))

    submit = edge.payloads("extract/submit")[0]
    assert submit["level"] == DocumentLevel.LAYOUT_TABLES.value
    assert submit["device"] == PdfDevice.CUDA.value
    assert submit["format"] == "pdf"
    assert submit["filename_hint"] == "doc.pdf"


def test_default_level_and_device_are_the_declared_defaults(tmp_path: Path) -> None:
    """Ось не объявлена — едет объявленный дефолт (``full``/``cpu``), не догадка."""
    edge = _Edge()

    _extractor(edge).extract(_page(tmp_path, b"%PDF-1.7 data"))

    submit = edge.payloads("extract/submit")[0]
    assert submit["level"] == DocumentLevel.FULL.value
    assert submit["device"] == PdfDevice.CPU.value


@pytest.mark.parametrize(
    "url",
    [
        "https://docs.test/files/handbook.pdf",
        "https://docs.test/files/archive.zip!/handbook.pdf",
    ],
)
def test_container_member_url_is_recognised_as_document(tmp_path: Path, url: str) -> None:
    """Член контейнера: URL синтетический, формат — из его собственного имени."""
    path = tmp_path / "member-0.bin"
    path.write_bytes(b"%PDF-1.7 member")
    page = RawPage(
        url=url,
        content=b"",
        source_path=str(path),
        fetched_at="T",
        content_type="application/pdf",
    )
    edge = _Edge()

    doc = _extractor(edge).extract(page, ExtractionRoute.DOCUMENT)

    assert doc.error is None
    assert "extract/upload" in edge.ops()
    assert edge.artifact(content_hash(b"%PDF-1.7 member")) == b"%PDF-1.7 member"
