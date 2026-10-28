"""Извлечение по артефакту в DMZ: приём чанками, сборка, разбор, уборка (B1/B2/B3; 10 §5.4).

Сеть не используется: сервис тестируется на инъецированной фабрике экстракторов, а шов —
через ``route_request``/``EdgeGateway`` (чистые функции, порт не открывается). Docling не
нужен: экстрактор подменяется, но его ``version`` собирается **той же** функцией ядра
(``docling_extractor_version``), поэтому проверка B2 (идентичность в пространстве имён
ядра) остаётся настоящей, а не тавтологией.

Что доказывается:

* приём чанками идемпотентен по ``content_hash``, ``received_bytes`` считается по файлам;
* расхождение ``chunk_total``/``size`` между чанками — видимый отказ, не тихая склейка;
* сборка сверяет **размер и хэш** и называет ожидаемое/фактическое; память при сборке не
  растёт с размером артефакта (замер ``tracemalloc``, не обещание);
* ``submit`` асинхронен (``pending`` до готовности), ``collect`` различает
  ``pending``/``done``/``error``/``unknown`` — последнее для незагруженного артефакта;
* ``abort`` и TTL-уборка освобождают диск и **сообщают числа**;
* ``device=cuda`` без устройства и ``level=off`` — отказ с числами/объяснением.
"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
import tracemalloc
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest

from core.adapters.extract_document import docling_extractor_version
from core.domain.enums import DocumentLevel, PdfDevice
from core.edge.extract_service import ArtifactExtractService, require_pdf_device
from core.edge.server import (
    DEFAULT_MAX_BODY_BYTES,
    EdgeError,
    EdgeGateway,
    max_upload_chunk_bytes,
    route_request,
)
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.util.hashing import content_hash

#: Версия «настоящего» документного экстрактора — тем же вызовом, что у ядра (B2).
_LEVEL = DocumentLevel.LAYOUT_TABLES
_DEVICE = PdfDevice.CPU
_VERSION = docling_extractor_version(_LEVEL, _DEVICE)

#: Артефакт для большинства тестов: содержимое не важно, важна его идентичность.
_PAYLOAD = b"artifact bytes for extraction" * 7


# ---------------------------------------------------------------------------
# Стенды
# ---------------------------------------------------------------------------


class _FakeDocumentExtractor:
    """Экстрактор-стенд: не грузит docling, но называет **ту же** версию (B2).

    Прочитанные байты запоминаются: тест доказывает, что ``submit`` извлёк ровно принятые
    байты (и ничего не досочинил).
    """

    version = _VERSION
    supports_source_path = True

    def __init__(self, *, text: str = "# neutral document", error: str | None = None) -> None:
        self._text = text
        self._error = error
        self.paths: list[tuple[str, str | None]] = []
        self.payloads: list[bytes] = []
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()

    def extract_path(
        self, path: str, *, filename_hint: str | None = None
    ) -> ExtractedDoc:
        self.paths.append((path, filename_hint))
        self.payloads.append(Path(path).read_bytes())
        self.entered.set()
        self.gate.wait(timeout=10)
        if self._error is not None:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(b""),
                extractor_version=self.version,
                error=self._error,
            )
        return ExtractedDoc(
            text=self._text,
            content_hash=content_hash(self._text),
            extractor_version=self.version,
            page_count=3,
        )


class _Factory:
    """Фабрика-стенд: отдаёт один и тот же экстрактор и запоминает объявленные оси."""

    def __init__(self, extractor: _FakeDocumentExtractor) -> None:
        self._extractor = extractor
        self.calls: list[tuple[str, str]] = []

    def __call__(self, level: str, device: str) -> _FakeDocumentExtractor:
        self.calls.append((level, device))
        return self._extractor


class _Clock:
    """Часы-стенд: TTL проверяется сдвигом времени, а не ожиданием."""

    def __init__(self) -> None:
        self._now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def logs() -> list[str]:
    return []


@pytest.fixture
def extractor() -> _FakeDocumentExtractor:
    return _FakeDocumentExtractor()


@pytest.fixture
def factory(extractor: _FakeDocumentExtractor) -> _Factory:
    return _Factory(extractor)


@pytest.fixture
def service(
    tmp_path: Path, factory: _Factory, clock: _Clock, logs: list[str]
) -> Iterator[ArtifactExtractService]:
    """Сервис на временном staging; TTL 60 с, часы стенда, лог в список.

    ``device_guard`` подменён: тесту не нужна CUDA, а отказ устройства проверяется
    отдельно — на настоящей функции с подменённым пробником.
    """
    instance = ArtifactExtractService(
        tmp_path / "extract",
        extractor_factory=factory,
        ttl_s=60,
        workers=1,
        clock=clock,
        log=logs.append,
        device_guard=_guard_without_cuda,
    )
    try:
        yield instance
    finally:
        instance.close()


# ---------------------------------------------------------------------------
# Вспомогательное
# ---------------------------------------------------------------------------


def _guard_without_cuda(device: PdfDevice | str) -> None:
    """Стенд проверки устройства: ``cuda`` отвергается с числами (как настоящий отказ)."""
    if PdfDevice(device) is not PdfDevice.CUDA:
        return
    raise EdgeError(
        "extract: device=cuda, но CUDA-устройство недоступно: "
        "torch=не установлен, cuda_available=False, device_vram_mb=нет устройства"
    )


def _chunks(data: bytes, size: int) -> list[bytes]:
    """Нарезает артефакт на чанки объявленного размера (последний — короче)."""
    return [data[index : index + size] for index in range(0, len(data), size)]


def _upload(
    service: ArtifactExtractService,
    data: bytes,
    *,
    slot: str | None = None,
    chunk_size: int = 64,
) -> list[dict[str, Any]]:
    """Загружает артефакт чанками; возвращает ответы ``upload`` по порядку."""
    identity = slot or content_hash(data)
    parts = _chunks(data, chunk_size) or [b""]
    return [
        service.upload(
            {
                "content_hash": identity,
                "size": len(data),
                "chunk_no": index,
                "chunk_total": len(parts),
                "data_b64": base64.b64encode(part).decode("ascii"),
            }
        )
        for index, part in enumerate(parts)
    ]


def _wait_status(
    service: ArtifactExtractService,
    slot: str,
    expected: set[str],
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Опрашивает ``collect`` до ожидаемого статуса (или падает по таймауту)."""
    deadline = time.monotonic() + timeout
    seen: dict[str, Any] = {"status": "unknown"}
    while time.monotonic() < deadline:
        seen = service.collect({"content_hash": slot})
        if seen["status"] in expected:
            return seen
        time.sleep(0.005)
    raise AssertionError(f"статус {seen['status']!r} не сменился на {expected}: {seen}")


def _gateway(service: ArtifactExtractService | None, **kwargs: Any) -> EdgeGateway:
    """Гейтвей без сети: краулер/экстрактор-заглушки (extract/* идут в сервис)."""

    class _Crawler:
        version = "fake-crawler-1"

        def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
            return RawPage(url=url, content=b"", fetched_at="t")

    class _Extractor:
        version = "fake-html-1"

        def extract(
            self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
        ) -> ExtractedDoc:
            return ExtractedDoc(text="", content_hash="h", extractor_version=self.version)

    return EdgeGateway(
        _Crawler(), _Extractor(), extract_service=service, **kwargs
    )


# ---------------------------------------------------------------------------
# B1: приём чанками
# ---------------------------------------------------------------------------


def test_upload_accepts_chunks_and_reports_actual_state(
    service: ArtifactExtractService,
) -> None:
    """Чанки принимаются по одному; ``received_bytes``/``complete`` — по файлам."""
    responses = _upload(service, _PAYLOAD, chunk_size=64)

    assert [item["received_chunks"] for item in responses] == list(
        range(1, len(responses) + 1)
    )
    final = responses[-1]
    assert final["complete"] is True
    assert final["received_bytes"] == len(_PAYLOAD)
    assert final["chunk_total"] == len(responses)
    assert final["content_hash"] == content_hash(_PAYLOAD)
    # expires_at вычислен от объявленного TTL, а не «примерно».
    assert final["expires_at"] is not None
    staged = sorted(
        path.name for path in (service.staging_dir / final["content_hash"]).iterdir()
    )
    assert all(name.endswith(".part") or name == "meta.json" for name in staged)


def test_repeat_upload_of_the_same_chunk_is_idempotent(
    service: ArtifactExtractService,
) -> None:
    """Повтор чанка адресует ту же staging-область (идентичность — ``content_hash``)."""
    slot = content_hash(_PAYLOAD)
    first = _upload(service, _PAYLOAD, slot=slot, chunk_size=64)[0]
    replacement = b"replacement chunk bytes"
    again = service.upload(
        {
            "content_hash": slot,
            "size": len(_PAYLOAD),
            "chunk_no": 0,
            "chunk_total": first["chunk_total"],
            "data_b64": base64.b64encode(replacement).decode("ascii"),
        }
    )
    # Область та же: остальные чанки на месте, изменился только повторённый.
    assert again["received_chunks"] == first["chunk_total"]
    assert again["received_bytes"] == len(_PAYLOAD) - 64 + len(replacement)
    assert (service.staging_dir / slot / "chunk_00000000.part").read_bytes() == replacement


def test_chunk_total_mismatch_between_chunks_is_refused(
    service: ArtifactExtractService,
) -> None:
    """Расходящийся ``chunk_total`` — отказ с обоими числами, а не склейка двух наборов."""
    slot = content_hash(_PAYLOAD)
    baseline = _upload(service, _PAYLOAD, slot=slot, chunk_size=64)[0]

    with pytest.raises(EdgeError) as error:
        service.upload(
            {
                "content_hash": slot,
                "size": len(_PAYLOAD),
                "chunk_no": 1,
                "chunk_total": baseline["chunk_total"] + 3,
                "data_b64": base64.b64encode(b"x").decode("ascii"),
            }
        )
    message = str(error.value)
    assert str(baseline["chunk_total"]) in message
    assert str(baseline["chunk_total"] + 3) in message


def test_size_mismatch_between_chunks_is_refused(
    service: ArtifactExtractService,
) -> None:
    """Расходящийся ``size`` означает, что чанки принадлежат разным артефактам."""
    slot = content_hash(_PAYLOAD)
    baseline = _upload(service, _PAYLOAD, slot=slot, chunk_size=64)[0]

    with pytest.raises(EdgeError) as error:
        service.upload(
            {
                "content_hash": slot,
                "size": len(_PAYLOAD) + 1,
                "chunk_no": 1,
                "chunk_total": baseline["chunk_total"],
                "data_b64": base64.b64encode(b"x").decode("ascii"),
            }
        )
    assert "size" in str(error.value)


def test_content_hash_is_validated_as_a_directory_name(
    service: ArtifactExtractService,
) -> None:
    """``content_hash`` адресует каталог: имя выхода за staging отвергается."""
    with pytest.raises(EdgeError):
        service.upload(
            {
                "content_hash": "../escape",
                "size": 1,
                "chunk_no": 0,
                "chunk_total": 1,
                "data_b64": "YQ==",
            }
        )


def test_invalid_base64_is_a_named_refusal(service: ArtifactExtractService) -> None:
    """Мусор в ``data_b64`` — отказ, а не «тихо склеенные» байты."""
    with pytest.raises(EdgeError) as error:
        service.upload(
            {
                "content_hash": content_hash(_PAYLOAD),
                "size": len(_PAYLOAD),
                "chunk_no": 0,
                "chunk_total": 1,
                "data_b64": "not base64 at all %%%",
            }
        )
    assert "base64" in str(error.value)


# ---------------------------------------------------------------------------
# B1: сборка и сверка хэша
# ---------------------------------------------------------------------------


def test_submit_refuses_unknown_artifact(service: ArtifactExtractService) -> None:
    """Незагруженный артефакт — ``accepted=false`` с причиной, а не пустая работа."""
    result = service.submit(
        {"content_hash": content_hash(b"never uploaded"), "size": 10, "level": "layout_tables"}
    )
    assert result["accepted"] is False
    assert "не загружен" in str(result["error"])


def test_submit_refuses_assembled_size_mismatch(
    service: ArtifactExtractService,
) -> None:
    """Собранный размер обязан совпасть с объявленным; отказ называет оба числа."""
    _upload(service, _PAYLOAD)
    result = service.submit(
        {
            "content_hash": content_hash(_PAYLOAD),
            "size": len(_PAYLOAD) + 5,
            "level": "layout_tables",
        }
    )
    assert result["accepted"] is False
    assert str(len(_PAYLOAD)) in str(result["error"])
    assert str(len(_PAYLOAD) + 5) in str(result["error"])


def test_submit_refuses_hash_mismatch_and_names_both_hashes(
    service: ArtifactExtractService,
) -> None:
    """Байты сверяются с ``content_hash``: расхождение называет ожидаемое и фактическое."""
    claimed = content_hash(b"some other bytes")
    _upload(service, _PAYLOAD, slot=claimed)

    result = service.submit(
        {"content_hash": claimed, "size": len(_PAYLOAD), "level": "layout_tables"}
    )

    assert result["accepted"] is False
    message = str(result["error"])
    assert claimed in message
    assert hashlib.sha256(_PAYLOAD).hexdigest() in message


def test_assembly_memory_does_not_grow_with_artifact_size(
    service: ArtifactExtractService,
) -> None:
    """Сборка потоковая: пик Python-памяти много меньше артефакта (замер, не обещание).

    Артефакт 4 МиБ нарезан на чанки по 256 КиБ. Если бы сборка читала файл целиком
    (``read_bytes``) или клеила ``b"".join(...)``, пик был бы сопоставим с размером
    артефакта; блочное копирование по 1 МиБ обязано уложиться в половину.
    """
    data = bytes(range(256)) * (16 * 1024)  # 4 МиБ
    _upload(service, data, chunk_size=256 * 1024)

    tracemalloc.start()
    try:
        result = service.submit(
            {"content_hash": content_hash(data), "size": len(data), "level": "layout_tables"}
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result["accepted"] is True
    assert peak < len(data) // 2, f"пик {peak} байт сопоставим с артефактом {len(data)}"


# ---------------------------------------------------------------------------
# B2/B3: submit → асинхронный разбор → collect
# ---------------------------------------------------------------------------


def test_submit_returns_immediately_and_collect_reports_pending_then_done(
    service: ArtifactExtractService,
    extractor: _FakeDocumentExtractor,
    factory: _Factory,
) -> None:
    """``submit`` асинхронен: текст появляется в ``collect`` после работы, а не в ответе."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    extractor.gate.clear()  # работа не может закончиться, пока тест не отпустит

    submitted = service.submit(
        {"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"}
    )
    assert submitted["accepted"] is True
    assert submitted["extractor_version"] == _VERSION
    assert extractor.entered.wait(timeout=10), "работа не началась"

    pending = service.collect({"content_hash": slot})
    assert pending["status"] == "pending"
    assert pending["text"] is None

    extractor.gate.set()
    done = _wait_status(service, slot, {"done"})
    assert done["status"] == "done"
    assert done["extractor_version"] == _VERSION
    assert done["text"] is not None
    assert done["text"]["content"] == "# neutral document"
    assert done["text"]["hash"] == content_hash("# neutral document")
    assert done["text"]["page_count"] == 3
    # B3: оси из запроса доехали до фабрики экстрактора.
    assert factory.calls == [("layout_tables", "cpu")]
    # `submit` извлёк ровно принятые байты — и ничего не досочинил.
    assert extractor.payloads == [_PAYLOAD]


def test_chunks_are_consumed_after_assembly_but_artifact_is_kept(
    service: ArtifactExtractService,
) -> None:
    """Чанки удаляются после успешной сборки: собранный файл и есть артефакт."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    service.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"})
    _wait_status(service, slot, {"done"})

    names = sorted(path.name for path in (service.staging_dir / slot).iterdir())
    assert "artifact.bin" in names
    assert not [name for name in names if name.endswith(".part")]

    # Повторный submit перепроверяет собранный файл, а не требует чанков заново.
    again = service.submit(
        {"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"}
    )
    assert again["accepted"] is True


def test_collect_unknown_for_never_uploaded_artifact(
    service: ArtifactExtractService,
) -> None:
    """Незагруженный артефакт — ``unknown``, а не ``pending``: ядро не ждёт вечно."""
    result = service.collect({"content_hash": content_hash(b"nothing")})
    assert result["status"] == "unknown"
    assert result["text"] is None


def test_collect_unknown_for_staged_but_never_submitted_artifact(
    service: ArtifactExtractService,
) -> None:
    """Загруженные чанки без заказа — тоже «работы нет», а не «ещё идёт»."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    assert service.collect({"content_hash": slot})["status"] == "unknown"


def test_collect_error_after_extractor_error(
    tmp_path: Path, clock: _Clock, logs: list[str]
) -> None:
    """Сбой извлечения — ``status=error`` с текстом и без ``text``."""
    extractor = _FakeDocumentExtractor(error="docling: synthetic failure")
    instance = ArtifactExtractService(
        tmp_path / "extract",
        extractor_factory=_Factory(extractor),
        clock=clock,
        log=logs.append,
        device_guard=lambda device: None,
    )
    try:
        slot = content_hash(_PAYLOAD)
        _upload(instance, _PAYLOAD)
        assert instance.submit(
            {"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"}
        )["accepted"]
        result = _wait_status(instance, slot, {"error"})
    finally:
        instance.close()
    assert result["status"] == "error"
    assert "synthetic failure" in str(result["error"])
    assert result["text"] is None


def test_collect_error_when_extractor_raises(
    tmp_path: Path, clock: _Clock, logs: list[str]
) -> None:
    """Исключение экстрактора не теряется: оно становится ``status=error``."""

    class _Raising(_FakeDocumentExtractor):
        def extract_path(
            self, path: str, *, filename_hint: str | None = None
        ) -> ExtractedDoc:
            raise RuntimeError("boom in converter")

    instance = ArtifactExtractService(
        tmp_path / "extract",
        extractor_factory=_Factory(_Raising()),
        clock=clock,
        log=logs.append,
        device_guard=lambda device: None,
    )
    try:
        slot = content_hash(_PAYLOAD)
        _upload(instance, _PAYLOAD)
        instance.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"})
        result = _wait_status(instance, slot, {"error"})
    finally:
        instance.close()
    assert "boom in converter" in str(result["error"])


def test_collect_refuses_result_of_another_level(
    service: ArtifactExtractService,
) -> None:
    """Текст другого уровня — другой провенанс: под запрошенной осью он не отдаётся."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    service.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"})
    _wait_status(service, slot, {"done"})

    result = service.collect({"content_hash": slot, "level": "full"})
    assert result["status"] == "error"
    assert "layout_tables" in str(result["error"])
    assert "full" in str(result["error"])


def test_level_off_is_refused_with_explanation(service: ArtifactExtractService) -> None:
    """``level=off`` не извлекает: запрос противоречит оси, и это сказано словами."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    with pytest.raises(EdgeError) as error:
        service.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "off"})
    assert "off" in str(error.value)


def test_device_cuda_without_device_is_refused(
    tmp_path: Path,
    factory: _Factory,
    clock: _Clock,
    logs: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``device=cuda`` без устройства — отказ с числами, а не тихий CPU (P9/R5).

    Проверяется **настоящий** ``require_pdf_device``: подменён только пробник CUDA, чтобы
    исход не зависел от машины. Тихий фолбэк на CPU прошёл бы молча — и ``extractor_version``
    назвал бы устройство, на котором извлечение не исполнялось.
    """
    import core.edge.extract_service as extract_service_module

    monkeypatch.setattr(extract_service_module, "_cuda_available", lambda: False)
    monkeypatch.setattr(extract_service_module, "_cuda_total_vram_mb", lambda: None)
    instance = ArtifactExtractService(
        tmp_path / "extract",
        extractor_factory=factory,
        clock=clock,
        log=logs.append,
    )
    try:
        slot = content_hash(_PAYLOAD)
        _upload(instance, _PAYLOAD)
        with pytest.raises(EdgeError) as error:
            instance.submit(
                {
                    "content_hash": slot,
                    "size": len(_PAYLOAD),
                    "level": "layout_tables",
                    "device": "cuda",
                }
            )
    finally:
        instance.close()
    message = str(error.value)
    # Отказ называет факты, а не «недоступно»: устройство, torch, объём карты.
    assert "cuda_available=False" in message
    assert "device_vram_mb=" in message
    # Ни один экстрактор не построен: работа не начиналась.
    assert factory.calls == []


def test_require_pdf_device_refuses_cuda_with_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Настоящая проверка устройства: ``cuda`` без карты — отказ с числами."""
    import core.edge.extract_service as extract_service_module

    monkeypatch.setattr(extract_service_module, "_cuda_available", lambda: False)
    monkeypatch.setattr(extract_service_module, "_cuda_total_vram_mb", lambda: None)

    assert require_pdf_device(PdfDevice.CPU) is None
    with pytest.raises(EdgeError) as error:
        require_pdf_device(PdfDevice.CUDA)
    message = str(error.value)
    assert "cuda_available=False" in message
    assert "device_vram_mb=" in message
    assert "torch=" in message


def test_declared_extractor_mismatch_is_reported(
    service: ArtifactExtractService, logs: list[str]
) -> None:
    """Ядро объявило другой экстрактор — шов отвечает **фактическим** (B2), и это видно."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    result = service.submit(
        {
            "content_hash": slot,
            "size": len(_PAYLOAD),
            "level": "layout_tables",
            "extractor_declared": "remote-extractor-1",
        }
    )
    assert result["accepted"] is True
    assert result["extractor_version"] == _VERSION
    assert any("extractor_declared" in line for line in logs)
    _wait_status(service, slot, {"done"})


# ---------------------------------------------------------------------------
# abort и TTL-уборка
# ---------------------------------------------------------------------------


def test_abort_removes_staging_area_and_reports_bytes(
    service: ArtifactExtractService,
) -> None:
    """``abort`` освобождает диск сразу и сообщает, сколько освободил."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)

    result = service.abort({"content_hash": slot})

    assert result["aborted"] is True
    # Освобождено не меньше, чем весит артефакт: к байтам добавляется учёт (meta.json).
    assert result["removed_bytes"] >= len(_PAYLOAD)
    assert not (service.staging_dir / slot).exists()
    # Повторный abort — не ошибка, а честное «убирать нечего».
    assert service.abort({"content_hash": slot}) == {
        "content_hash": slot,
        "aborted": False,
        "removed_bytes": 0,
    }


def test_reupload_after_done_clears_previous_result(
    service: ArtifactExtractService,
) -> None:
    """Новый чанк снимает и собранный артефакт, и прошлый результат: они про другой набор.

    Иначе ``collect`` отдавал бы текст, полученный из прежнего набора байтов, под
    staging-областью, которую прямо сейчас переписывают, — то есть отвечал бы ``done``
    за состояние, которого больше нет.
    """
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD, chunk_size=64)
    service.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"})
    _wait_status(service, slot, {"done"})

    service.upload(
        {
            "content_hash": slot,
            "size": len(_PAYLOAD),
            "chunk_no": 0,
            "chunk_total": 4,
            "data_b64": base64.b64encode(_PAYLOAD[:64]).decode("ascii"),
        }
    )

    assert service.collect({"content_hash": slot})["status"] == "unknown"
    assert not (service.staging_dir / slot / "artifact.bin").exists()


def test_abort_while_job_runs_does_not_publish_result(
    service: ArtifactExtractService,
    extractor: _FakeDocumentExtractor,
) -> None:
    """Работа, пережившая ``abort``, не публикует результат: поколение не то."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    extractor.gate.clear()
    service.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"})
    assert extractor.entered.wait(timeout=10)

    aborted = service.abort({"content_hash": slot})
    extractor.gate.set()
    time.sleep(0.05)  # работа завершилась и попыталась записать результат

    assert aborted["aborted"] is True
    assert not (service.staging_dir / slot).exists()
    assert service.collect({"content_hash": slot})["status"] == "unknown"


def test_ttl_purge_removes_stale_upload_and_reports_numbers(
    service: ArtifactExtractService, clock: _Clock, logs: list[str]
) -> None:
    """Брошенная загрузка убирается по объявленному TTL; факт уборки виден в числах."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    assert service.purge_expired() == {"removed_entries": 0, "removed_bytes": 0}

    clock.advance(service.ttl_s + 1)
    purged = service.purge_expired()

    assert purged["removed_entries"] == 1
    assert purged["removed_bytes"] >= len(_PAYLOAD)
    assert not (service.staging_dir / slot).exists()
    assert any("TTL purge" in line and "1 staging area" in line for line in logs)


def test_ttl_purge_keeps_fresh_upload(service: ArtifactExtractService, clock: _Clock) -> None:
    """Молодая загрузка не срезается: TTL — про брошенное, а не про текущее."""
    _upload(service, _PAYLOAD)
    clock.advance(service.ttl_s - 1)
    assert service.purge_expired()["removed_entries"] == 0


def test_ttl_purge_skips_area_with_running_job(
    service: ArtifactExtractService,
    extractor: _FakeDocumentExtractor,
    clock: _Clock,
) -> None:
    """Передача/разбор в работе TTL не срезает: запись живая, пока её держит работа."""
    slot = content_hash(_PAYLOAD)
    _upload(service, _PAYLOAD)
    extractor.gate.clear()
    service.submit({"content_hash": slot, "size": len(_PAYLOAD), "level": "layout_tables"})
    assert extractor.entered.wait(timeout=10)
    try:
        clock.advance(service.ttl_s * 10)
        assert service.purge_expired()["removed_entries"] == 0
        assert (service.staging_dir / slot).is_dir()
    finally:
        extractor.gate.set()
    _wait_status(service, slot, {"done"})


# ---------------------------------------------------------------------------
# Шов: маршрутизация, capabilities, отсутствие сервиса
# ---------------------------------------------------------------------------


def test_capabilities_declare_extract_block(service: ArtifactExtractService) -> None:
    """``capabilities.extract`` объявляет размер чанка и исполни&#1084;ые оси."""
    capabilities = _gateway(service).capabilities()["extract"]

    assert capabilities["supports_artifact"] is True
    assert capabilities["max_upload_chunk_bytes"] == max_upload_chunk_bytes(
        DEFAULT_MAX_BODY_BYTES
    )
    assert capabilities["levels"] == sorted(
        {"text", "text_layer", "layout", "layout_tables", "full"}
    )
    assert capabilities["devices"] == ["cpu", "cuda"]
    assert capabilities["staging_ttl_s"] == service.ttl_s


def test_max_upload_chunk_bytes_derives_from_body_limit() -> None:
    """Объявленный чанк выводится из лимита тела: base64 + обвязка укладываются в бюджет."""
    for limit in (DEFAULT_MAX_BODY_BYTES, 2 * DEFAULT_MAX_BODY_BYTES, 262_144):
        chunk = max_upload_chunk_bytes(limit)
        encoded = 4 * ((chunk + 2) // 3)  # длина base64 ровно для chunk байт
        assert encoded <= limit
        # Число — следствие формулы, а не круглое значение «на всякий случай».
        assert chunk % 3 == 0
    assert max_upload_chunk_bytes(DEFAULT_MAX_BODY_BYTES) != max_upload_chunk_bytes(
        2 * DEFAULT_MAX_BODY_BYTES
    )
    # Реальная настройка узла тоже видна в capabilities.
    assert (
        _gateway(None, max_body_bytes=262_144).capabilities()["extract"][
            "max_upload_chunk_bytes"
        ]
        == max_upload_chunk_bytes(262_144)
    )


def test_declared_chunk_size_actually_fits_the_body_limit(
    service: ArtifactExtractService,
) -> None:
    """Объявленный размер чанка проходит транспорт: 413 на нём — дефект объявления.

    Это проверка того самого обещания ``capabilities.extract.max_upload_chunk_bytes``:
    ядро, взявшее число **отсюда**, не должно получить отказ по лимиту тела. Чанк
    максимального объявленного размера уезжает в base64 внутри конверта и обязан
    уложиться в ``DEFAULT_MAX_BODY_BYTES``.
    """
    chunk = max_upload_chunk_bytes(DEFAULT_MAX_BODY_BYTES)
    data = bytes(range(256)) * (chunk // 256)
    payload = {
        "content_hash": content_hash(data),
        "size": len(data),
        "chunk_no": 0,
        "chunk_total": 1,
        "data_b64": base64.b64encode(data).decode("ascii"),
    }
    # Тело POST — сам payload (конверт описывает ответ; запросный конверт — форма
    # клиента). Проверяем и обёрнутую форму: запас на обвязку — часть формулы.
    body = json.dumps(payload).encode()
    enveloped = json.dumps(
        {
            "schema_version": "1",
            "request_id": "0" * 32,
            "status": "request",
            "payload": payload,
            "error": None,
        }
    ).encode()

    assert len(body) <= DEFAULT_MAX_BODY_BYTES
    assert len(enveloped) <= DEFAULT_MAX_BODY_BYTES
    status, envelope = route_request(
        _gateway(service), "POST", "/v1/extract/upload", body
    )
    assert status == 200, envelope
    assert envelope["status"] == "ok"
    assert envelope["payload"]["complete"] is True
    assert envelope["payload"]["received_bytes"] == len(data)


def test_without_service_extract_ops_are_errors_and_supports_artifact_false() -> None:
    """Узел без сервиса честно объявляет ``supports_artifact=false`` и отказывает."""
    gateway = _gateway(None)
    capabilities = gateway.capabilities()["extract"]
    assert capabilities["supports_artifact"] is False
    assert capabilities["levels"] == [] and capabilities["devices"] == []

    envelope = gateway.envelope("extract/collect", {"content_hash": "a"})
    assert envelope["status"] == "error"
    assert "not available" in envelope["error"]


def test_extract_ops_are_routed_post_only(service: ArtifactExtractService) -> None:
    """Маршрутизация: четыре операции POST, GET по ним — 405, неизвестная — 404."""
    gateway = _gateway(service)
    slot = content_hash(_PAYLOAD)

    status, envelope = route_request(
        gateway,
        "POST",
        "/v1/extract/upload",
        b'{"content_hash": "' + slot.encode() + b'", "size": 1, "chunk_no": 0, '
        b'"chunk_total": 1, "data_b64": "YQ=="}',
    )
    assert status == 200 and envelope["status"] == "ok"
    assert envelope["payload"]["received_chunks"] == 1

    for op in ("extract/upload", "extract/submit", "extract/collect", "extract/abort"):
        code, _ = route_request(gateway, "GET", f"/v1/{op}")
        assert code == 405, op
    code, _ = route_request(gateway, "POST", "/v1/extract/teleport", b"{}")
    assert code == 404


def test_extract_request_validation_precedes_service_availability() -> None:
    """Форма запроса проверяется до «сервис недоступен»: клиент чинит свою ошибку."""
    gateway = _gateway(None)
    envelope = gateway.envelope("extract/submit", {"content_hash": "a", "size": 1})
    assert envelope["status"] == "error"
    assert "level" in envelope["error"]


def test_extract_service_exception_becomes_status_error_without_traceback(
    service: ArtifactExtractService,
) -> None:
    """Сбой сервиса — конверт ``status=error`` с сообщением, без внутренностей."""
    gateway = _gateway(service)
    envelope = gateway.envelope("extract/collect", {"content_hash": "../nope"})
    assert envelope["status"] == "error"
    assert "content_hash" in envelope["error"]


def test_gateway_close_closes_extract_service() -> None:
    """Закрытие узла доходит до сервиса (иначе потоки очереди остались бы висеть)."""
    calls: list[str] = []

    class _Service:
        levels: ClassVar[list[str]] = ["full"]
        devices: ClassVar[list[str]] = ["cpu"]
        ttl_s = 10

        def close(self) -> None:
            calls.append("closed")

        def collect(self, payload: Mapping[str, Any]) -> dict[str, Any]:
            return {"content_hash": "a", "status": "unknown"}

    gateway = _gateway(_Service())  # type: ignore[arg-type]
    gateway.close()
    assert calls == ["closed"]
