"""Извлечение **по артефакту** в DMZ: приём чанками, сборка, асинхронный разбор (B1/B2/B3).

Зачем подпротокол, а не «fetch по URL» (07 §8). Получая ``url``, edge сам ходит в сеть;
это (а) пере-фетчит документ, который ядро уже сохранило, (б) раздваивает провенанс
(что прочитано — edge или ядро), (в) недоступно членам контейнеров, которые уже файлы на
диске. Поэтому ядро присылает **свои** байты: идентичность артефакта — ``content_hash``,
а edge извлекает ровно их и в сеть не ходит вовсе.

Почему чанками. Ответ шва ограничен (``MAX_RESPONSE_BYTES``), а артефакт корпуса — нет:
документ на сотни МБ в один запрос не влезает. Значит, шов перестаёт быть stateless —
и это не «чанкование», а новый подпротокол: идентификатор загрузки (здесь — сам
``content_hash``), порядок чанков, идемпотентность повтора, сборка из частичного набора
и уборка брошенных загрузок. Всё это живёт здесь.

Границы:

* **Байты не проходят через RAM целиком.** Чанк декодируется из base64 блоками по
  256 КиБ (граница кратна 4 — блок не разрывает кодированную тройку), сборка идёт
  блоками по 1 МиБ, хэш считается по ходу записи. Приём 250 МБ артефакта не требует
  250 МБ памяти — ни на одном шаге.
* **Никаких сетевых вызовов.** ``submit`` разбирает только принятые байты.
* **I19.** Наружу уходит контент, не evidence: ядро пересчитывает хэш очищенного текста
  само. Хэш здесь считается для целостности передачи и для проверки адреса артефакта.
* **Идентичность экстрактора — в пространстве имён ядра** (B2). ``extractor_version``
  берётся у фактически исполняющего адаптера (``docling-doc-3:<level>/<device>/
  docling-<build>/<policy>``), а не подменяется строкой вида ``remote-extractor-1``: ``pb``
  (партиция) включает наблюдаемую версию, поэтому своя строка развела бы сборки
  ``split`` и ``local`` при байт-в-байт одинаковом тексте. Условие паритета — та же
  строка, а не «тот же текст».
* **Жизнь состояния короткая и объявленная.** Staging-область адресуется ``content_hash``
  и удаляется извлечением, ``abort``-ом или TTL-уборкой (TTL — значение конфигурации,
  а не константа кода); факт уборки печатается с числами.

Слой: edge импортирует ``adapters``/``config``/``ports``/``util``; ``store``/``pipeline``/
``cli`` не импортируются (I18/I19). Проверка устройства (P9) поэтому написана здесь, а не
позаимствована у ``core.cli.wiring``: семантика та же (``cuda`` без устройства — отказ с
числами, не тихий CPU), а зависимость на composition root ядра развернула бы шов.
Нейтрален (§1.3).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import queue
import re
import shutil
import sys
import threading
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from core.domain.enums import DocumentLevel, PdfDevice, PdfPipeline
from core.edge.seam_schema import EXTRACT_LEVEL_VALUES, PDF_DEVICE_VALUES
from core.edge.server import EdgeAdapterError, EdgeError, EdgeRequestError
from core.util.gpu import cuda_available, cuda_total_vram_mb
from core.util.hashing import content_hash as hash_bytes

__all__ = [
    "ArtifactExtractService",
    "artifact_extractor_factory",
    "require_pdf_device",
]

#: Имя staging-области — сам ``content_hash``, но только если это безопасное имя.
#:
#: Идентификатор артефакта — SHA-256 hex (64 символа), однако шов не диктует ядру
#: алгоритм: принимаем «слово из безопасных символов» и отвергаем всё остальное, потому
#: что имя каталога строится из этого значения. Точка в алфавит не входит намеренно:
#: ``..`` — не хэш, а попытка выйти за staging-каталог.
_HASH_NAME = re.compile(r"^[0-9A-Za-z_-]{1,128}$")

#: Имена внутри staging-области (а не «как получится»: их читает уборка).
_CHUNK_PATTERN = "chunk_{:08d}.part"
_META_NAME = "meta.json"
_ARTIFACT_NAME = "artifact.bin"
_RESULT_NAME = "result.json"

#: Размер блока чтения/записи при сборке, байт. 1 МиБ — компромисс между числом
#: syscall-ов и пиком памяти: файл на 250 МБ проходит через буфер в 1 МиБ.
_IO_BLOCK = 1 << 20

#: Размер блока декодирования base64, байт. Кратен 4 — граница блока не разрывает
#: кодированную тройку, поэтому блоки декодируются независимо (``validate=True`` работает
#: на каждом блоке, а не «на всём сразу»).
_B64_BLOCK = 1 << 18

def _utcnow() -> datetime:
    """Текущее время UTC (инъекция часов делает TTL проверяемым без ожидания)."""
    return datetime.now(UTC)


def _iso(moment: datetime) -> str:
    """ISO-8601 представление момента (то, что видно в ``expires_at``/``meta.json``)."""
    return moment.astimezone(UTC).isoformat()


def _parse_iso(text: Any) -> datetime | None:
    """Разбирает ISO-8601 из ``meta.json``; ``None`` — запись нечитаема/битая."""
    if not isinstance(text, str) or not text:
        return None
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# P9: устройство извлечения — отказ вместо тихого CPU
# ---------------------------------------------------------------------------


def _cuda_available() -> bool:
    """Доступно ли CUDA-устройство — общая проба (:mod:`core.util.gpu`), не копия.

    Копия в DMZ была бы вторым ответом на вопрос «есть ли карта»: ядро и шов разошлись бы
    молча, и отказ ядра сочетался бы с тихой работой на CPU в edge. Оставлено приватным
    именем, потому что проба — деталь этого модуля, а не часть его API.
    """
    return cuda_available()

def _cuda_total_vram_mb() -> int | None:
    """Полный объём видеопамяти устройства 0, МБ; ``None`` — устройства нет."""
    return cuda_total_vram_mb()

def require_pdf_device(device: PdfDevice | str) -> None:
    """``device=cuda`` без CUDA-устройства — отказ с числами, а не тихий CPU-фолбэк (P9/R5).

    Ось ``device`` — **объявленный** вход: узел, объявивший ``cuda``, обязан исполниться на
    GPU либо отказать. Тихая подстановка ``cpu`` была бы ложным провенансом:
    ``extractor_version`` назвала бы устройство, на котором извлечение не исполнялось —
    тот же класс дефекта, что чтение модели оценщика из окружения (§7.4).

    Сообщение несёт факты, по которым оператор починит вход, а не «недоступно»: версию
    установленного ``torch`` (из метаданных дистрибутива — без импорта: импорт ради отказа
    стоил бы сотни МБ), ``cuda_available`` и объём карты.
    """
    if PdfDevice(device) is not PdfDevice.CUDA:
        return
    if _cuda_available():
        return
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _dist_version

    try:
        torch_version = _dist_version("torch")
    except PackageNotFoundError:  # pragma: no cover — окружение без torch
        torch_version = "не установлен"
    total = _cuda_total_vram_mb()
    raise EdgeError(
        "extract: device=cuda, но CUDA-устройство недоступно: "
        f"torch={torch_version}, cuda_available=False, "
        f"device_vram_mb={total if total is not None else 'нет устройства'}. "
        "Объявите device=cpu либо установите CUDA-колёса и драйвер. Тихая подмена "
        "устройства запрещена: extractor_version назвал бы GPU, на котором извлечение "
        "не исполнялось"
    )


# ---------------------------------------------------------------------------
# B3: экстрактор по оси уровня
# ---------------------------------------------------------------------------


def artifact_extractor_factory(level: str, device: str) -> Any:
    """Строит экстрактор для **объявленных** осей: ``level`` (B3) и ``device`` (P9).

    Уровень не «подразумевается»: он приходит в запросе и определяет фактический
    конвейер — ``text_layer`` идёт в pdfium (текстовый слой без моделей, P7), остальные
    значения лестницы — в docling соответствующего уровня. Это и есть Д-1.4: ось
    ``document_level`` действует на шве так же, как в ядре, поэтому текст под ``split``
    совпадает с ``local``.

    Недоступность механизма — видимый отказ: тихая подмена (pdfium вместо docling или
    наоборот) выдала бы чужой ``extractor_version`` за объявленный.

    :raises EdgeAdapterError: механизм не установлен.
    """
    device_value = PdfDevice(device)
    if str(level) == PdfPipeline.TEXT_LAYER.value:
        from core.adapters.extract_pdf_text import PdfiumDocumentExtractor

        text_layer: Any = PdfiumDocumentExtractor()
        if not text_layer.available:
            raise EdgeAdapterError(
                "extract: level=text_layer требует pypdfium2: установите extras "
                "'extract' либо объявите уровень лестницы docling. Тихая подмена "
                "механизма запрещена — extractor_version описал бы движок, которого не было"
            )
        return text_layer
    from core.adapters.extract_document import DoclingDocumentExtractor

    documents: Any = DoclingDocumentExtractor(
        level=DocumentLevel(level), pdf_device=device_value
    )
    if not documents.available:
        raise EdgeAdapterError(
            "extract: docling не установлен на этом edge-узле: установите extras "
            "'extract'/'extract-pdf'. Пустой текст вместо отказа выдал бы «документ "
            "прочитан» за факт"
        )
    return documents


# ---------------------------------------------------------------------------
# Сервис: staging + очередь работ
# ---------------------------------------------------------------------------


class ArtifactExtractService:
    """Staging артефактов и асинхронное извлечение по ``content_hash`` (07 §8).

    Состояние — **файлы**, не объекты Python: чанки, собранный артефакт, метаданные и
    результат лежат в ``{staging}/{content_hash}/``. Поэтому перезапуск узла не теряет
    принятые байты, а уборка — это удаление каталога, а не обход словаря.

    Асинхронность: ``submit`` принимает заказ и **сразу** отвечает (ядро не держит
    соединение на время разбора документа, которое измеряется минутами). Работа идёт в
    потоках-демонах, число которых объявлено осью ``workers``; ``collect`` опрашивает
    результат.

    :param staging_dir: каталог staging (свойство конфигурации edge, не ``/tmp``).
    :param extractor_factory: ``(level, device) -> extractor`` с ``extract_path`` и
        ``version``; инъецируется, чтобы тесты не грузили docling.
    :param ttl_s: TTL брошенной загрузки, сек (объявленное значение конфигурации).
    :param workers: число одновременных извлечений (ось из конфигурации).
    :param clock: часы (детерминизм TTL-проверок и ``expires_at``).
    :param log: приёмник строк наблюдаемости (по умолчанию ``stderr`` — тот же канал,
        которым edge сообщает о сбоях пулов).
    :param device_guard: проверка исполнимости ``device`` (по умолчанию
        :func:`require_pdf_device`); инъецируется, чтобы тест не требовал CUDA.
    """

    def __init__(
        self,
        staging_dir: str | Path,
        *,
        extractor_factory: Callable[[str, str], Any] = artifact_extractor_factory,
        ttl_s: int = 3600,
        workers: int = 1,
        clock: Callable[[], datetime] | None = None,
        log: Callable[[str], None] | None = None,
        device_guard: Callable[[PdfDevice | str], None] = require_pdf_device,
    ) -> None:
        self._root = Path(staging_dir)
        self._factory = extractor_factory
        self._ttl_s = int(ttl_s)
        self._workers = max(1, int(workers))
        self._clock = clock or _utcnow
        self._log = log or _stderr_log
        self._device_guard = device_guard
        self._queue: queue.Queue[tuple[str, str] | None] = queue.Queue()
        self._lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._extractors: dict[tuple[str, str], tuple[Any, threading.Lock, str]] = {}
        self._active: set[str] = set()
        self._threads: list[threading.Thread] = []
        self._closed = False

    # -- публичные свойства ----------------------------------------------------

    @property
    def staging_dir(self) -> Path:
        """Каталог staging (для наблюдаемости и проверок состава)."""
        return self._root

    @property
    def ttl_s(self) -> int:
        """TTL брошенной загрузки, сек (объявляется в ``capabilities.extract``)."""
        return self._ttl_s

    @property
    def levels(self) -> list[str]:
        """Уровни, которые узел **исполнит** (значение оси ``level``, 07 §8).

        ``off`` в списке отсутствует намеренно: как значение оси он допустим (схема
        публикует его), но извлечения не производит — запрос с ним получает отказ с
        объяснением. Объявлять его «поддерживаемым» значило бы пообещать текст, которого
        не будет.
        """
        return sorted(EXTRACT_LEVEL_VALUES - {DocumentLevel.OFF.value})

    @property
    def devices(self) -> list[str]:
        """Устройства, объявленные осью ``device`` (P9): ``cpu``/``cuda``."""
        return sorted(PDF_DEVICE_VALUES)

    # -- операции шва ----------------------------------------------------------

    def upload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Принимает **один** чанк артефакта и сообщает состояние передачи.

        Повтор того же ``chunk_no`` перезаписывает чанк: идентичность загрузки — сам
        ``content_hash``, поэтому повтор идемпотентен и не создаёт вторую область.

        Расхождение ``chunk_total``/``size`` между чанками — **ошибка**, а не тихая
        склейка: склеенный из разных наборов файл имел бы размер, не совпадающий с
        объявленным, и хэш, не совпадающий с ``content_hash``, — то есть отказ всё равно
        случился бы, но позже и без указания на настоящую причину.

        ``received_bytes`` считается по **фактическим файлам** на диске, а не по сумме
        того, что «должно было прийти»: иначе объявленная полнота расходилась бы с
        содержимым staging-области.
        """
        self.purge_expired()
        slot = self._slot(payload)
        area = self._area(slot)
        size = self._required_int(payload, "size")
        chunk_no = self._required_int(payload, "chunk_no")
        chunk_total = self._required_int(payload, "chunk_total")
        meta = self._read_json(area / _META_NAME) or {}
        self._check_consistency(meta, size=size, chunk_total=chunk_total)

        area.mkdir(parents=True, exist_ok=True)
        chunk_path = area / _CHUNK_PATTERN.format(chunk_no)
        staging = area / f"{chunk_path.name}.tmp"
        self._decode_b64_to_file(payload.get("data_b64"), staging)
        os.replace(staging, chunk_path)
        # Новый чанк делает предыдущее состояние недействительным целиком: собранный файл
        # описывал другой набор байтов (а `submit` сверяет хэш именно его), а результат
        # прошлой работы — текст, полученный из того набора. Поэтому снимаются оба, и
        # `collect` до нового `submit` честно отвечает `unknown`, а не отдаёт старый текст
        # под изменяющейся staging-областью.
        (area / _ARTIFACT_NAME).unlink(missing_ok=True)
        (area / _RESULT_NAME).unlink(missing_ok=True)
        self._write_json(
            area / _META_NAME,
            {
                "content_hash": slot,
                "size": size,
                "chunk_total": chunk_total,
                "created_at": meta.get("created_at") or _iso(self._clock()),
                "updated_at": _iso(self._clock()),
                "job": None,
            },
        )
        received = self._received_chunks(area, chunk_total)
        complete = len(received) == chunk_total or (area / _ARTIFACT_NAME).is_file()
        expiration = self._clock() + _seconds(self._ttl_s)
        return {
            "content_hash": slot,
            "received_chunks": len(received),
            "chunk_total": chunk_total,
            # Только байты артефакта (чанки + собранный файл): `meta.json` — учёт, а не
            # принятые данные, и включать его значило бы отвечать «принято больше, чем
            # ядро прислало».
            "received_bytes": self._payload_bytes(area),
            "complete": complete,
            "expires_at": _iso(expiration),
        }

    def submit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Заказывает извлечение принятого артефакта и отвечает **сразу**.

        Перед постановкой работы: все чанки на месте, склеенный размер равен объявленному,
        хэш собранного файла равен ``content_hash``. Последнее — I19-дисциплина в обратную
        сторону: байты пришли из ядра, но edge не верит на слово, потому что от
        идентичности артефакта зависит адрес staging-области и ключ кэша ядра.

        Отказ называет **ожидаемое и фактическое** — иначе «не совпало» не отличить от
        «не доехало».

        ``extractor_version`` в ответе — идентичность экстрактора, который **будет**
        исполнять работу (B2): ядро сравнивает её со своей объявленной. Если ядро
        прислало ``extractor_declared`` и оно не совпало, это видно предупреждением: шов
        не подменяет свою идентичность ожидаемой.
        """
        self.purge_expired()
        slot = self._slot(payload)
        size = self._required_int(payload, "size")
        level = str(payload.get("level") or "").strip()
        if level == DocumentLevel.OFF.value:
            raise EdgeRequestError(
                "extract: level=off не извлекает документы — запрос противоречит оси "
                "(ядро не должно присылать артефакт на извлечение при off)"
            )
        device = str(payload.get("device") or PdfDevice.CPU.value).strip()
        self._device_guard(device)

        area = self._area(slot)
        meta = self._read_json(area / _META_NAME)
        if meta is None:
            return self._refused(
                slot, "артефакт не загружен: staging-область не найдена (чанки не приходили)"
            )
        _, error = self._ensure_artifact(area, meta, slot, size)
        if error is not None:
            return self._refused(slot, error)

        # Экстрактор строится уже здесь: (а) ответ обязан назвать его идентичность (B2),
        # (б) недоступность механизма — отказ на постановке, а не асинхронная ошибка.
        version = self._extractor_for(level, device)[2]
        declared = payload.get("extractor_declared")
        if isinstance(declared, str) and declared and declared != version:
            self._log(
                "edge extract: extractor_declared расходится с исполняющим: "
                f"объявлено {declared!r}, исполняет {version!r} (B2: ядро увидит "
                "фактическую идентичность, а не ожидаемую)"
            )

        token = _new_token()
        self._write_json(
            area / _META_NAME,
            {
                **(meta or {}),
                "updated_at": _iso(self._clock()),
                "job": {
                    "token": token,
                    "level": level,
                    "device": device,
                    "format": payload.get("format"),
                    "filename_hint": payload.get("filename_hint"),
                    "lang": payload.get("lang"),
                    "extractor_declared": declared,
                    "extractor_version": version,
                    "submitted_at": _iso(self._clock()),
                },
            },
        )
        (area / _RESULT_NAME).unlink(missing_ok=True)
        self._enqueue(slot, token)
        return {
            "content_hash": slot,
            "accepted": True,
            "extractor_version": version,
            "error": None,
        }

    def collect(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Возвращает состояние работы по артефакту.

        Четыре исхода различаются не для полноты, а чтобы ядро не ждало вечно:
        ``pending`` — работа заказана и идёт; ``done`` — текст готов; ``error`` — работа
        упала и причина названа; ``unknown`` — работы **нет** (артефакт не загружали или
        его уже убрали). Отсутствие работы нельзя выдавать за ``pending``: это ожидание
        события, которого не будет.

        Если ядро передало ``level``, оно обязано совпасть с уровнем, которым извлечён
        результат: текст другого уровня — другой провенанс, и отдавать его под
        запрошенной осью значило бы соврать об условиях измерения.
        """
        self.purge_expired()
        slot = self._slot(payload)
        area = self._area(slot)
        if not area.is_dir():
            return self._collected(slot, "unknown")
        meta = self._read_json(area / _META_NAME) or {}
        job = meta.get("job") if isinstance(meta.get("job"), Mapping) else None
        result = self._read_json(area / _RESULT_NAME)
        if result is None:
            if not job:
                return self._collected(slot, "unknown")
            return self._collected(
                slot, "pending", extractor_version=job.get("extractor_version")
            )
        requested_level = payload.get("level")
        actual_level = result.get("level")
        if requested_level is not None and requested_level != actual_level:
            return self._collected(
                slot,
                "error",
                extractor_version=result.get("extractor_version"),
                error=(
                    f"результат получен уровнем {actual_level!r}, запрошен "
                    f"{requested_level!r}: текст другого уровня — другой провенанс"
                ),
            )
        status = str(result.get("status") or "error")
        return {
            "content_hash": slot,
            "status": status if status in {"done", "error"} else "error",
            "text": result.get("text") if status == "done" else None,
            "extractor_version": result.get("extractor_version"),
            "error": result.get("error"),
        }

    def abort(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Убирает staging-область и незавершённую работу; сообщает, сколько байт освободил.

        ``abort`` — не дубль TTL, а штатный путь: ядро знает, что загрузка не нужна,
        и не должно держать диск до истечения TTL. Работа, уже бегущая в потоке, не
        прерывается (docling не умеет отменяться извне), но её результат не запишется:
        поколение работы (``token``) больше не совпадёт — запись увидит чужой/исчезнувший
        ``meta.json``.
        """
        self.purge_expired()
        slot = self._slot(payload)
        area = self._area(slot)
        if not area.is_dir():
            return {"content_hash": slot, "aborted": False, "removed_bytes": 0}
        removed = self._area_bytes(area)
        with self._lock:
            self._active.discard(slot)
        shutil.rmtree(area, ignore_errors=True)
        return {"content_hash": slot, "aborted": not area.exists(), "removed_bytes": removed}

    # -- TTL-уборка -----------------------------------------------------------

    def purge_expired(self) -> dict[str, int]:
        """Удаляет staging-записи старше объявленного TTL; возвращает числа.

        Вызывается на каждой операции (дёшево: один список каталогов) и на закрытии узла.
        Так брошенная загрузка не остаётся навсегда — иначе «новый подпротокол» получил
        бы вторую жизнь в виде мусора на диске DMZ.

        Запись считается живой, пока в ней идёт работа (``self._active``): TTL считается от
        последнего обращения к области, а не от её создания, и не должен срезать передачу,
        которую ядро ещё шлёт.

        Факт уборки **виден**: числа возвращаются вызывающему и печатаются в канал
        наблюдаемости edge (``stderr`` — тем же, куда edge пишет о сбоях пулов).
        """
        removed_entries = 0
        removed_bytes = 0
        if not self._root.is_dir():
            return {"removed_entries": 0, "removed_bytes": 0}
        deadline_offset = _seconds(self._ttl_s)
        for area in sorted(self._root.iterdir()):
            if not area.is_dir():
                continue
            with self._lock:
                if area.name in self._active:
                    continue
            meta = self._read_json(area / _META_NAME) or {}
            stamp = _parse_iso(meta.get("updated_at")) or _mtime(area)
            if stamp is None or self._clock() - stamp <= deadline_offset:
                continue
            removed_bytes += self._area_bytes(area)
            shutil.rmtree(area, ignore_errors=True)
            removed_entries += 1
        if removed_entries:
            self._log(
                f"edge extract: TTL purge removed {removed_entries} staging area(s), "
                f"{removed_bytes} bytes"
            )
        return {"removed_entries": removed_entries, "removed_bytes": removed_bytes}

    # -- жизненный цикл -------------------------------------------------------

    def close(self) -> None:
        """Останавливает потоки-воркеры и убирает брошенные загрузки.

        Ждать текущего извлечения нельзя: разбор большого документа измеряется минутами, а
        остановка узла — секундами. Потоки демонические, поэтому процесс не держится за
        них; заказанная, но не начатая работа останется в очереди и будет снята
        TTL-уборкой при следующем старте (состояние лежит на диске, а не в памяти).

        Перед остановкой выполняется та же TTL-уборка, что на каждой операции: узел,
        который уходит штатно, не оставляет после себя недокачанный артефакт. Она видима
        (числа в канале наблюдаемости) и не трогает области, которые держит работа.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            threads = list(self._threads)
        self.purge_expired()
        for _ in threads:
            self._queue.put(None)

    # -- очередь работ --------------------------------------------------------

    def _enqueue(self, slot: str, token: str) -> None:
        """Ставит работу в очередь, поднимая воркеры при первой задаче.

        Очередь не ограничивается числом: ограничение — по **ресурсу** (``workers``
        одновременных извлечений), а не по числу принятых заказов. Заказ дешёв (это файл
        на диске), извлечение — дорого; отказ на постановке не защитил бы память, зато
        потерял бы уже принятые байты.
        """
        with self._lock:
            if self._closed:
                raise EdgeError("extract service is closed")
            self._active.add(slot)
            if not self._threads:
                for index in range(self._workers):
                    thread = threading.Thread(
                        target=self._worker_loop,
                        name=f"edge-extract-{index}",
                        daemon=True,
                    )
                    thread.start()
                    self._threads.append(thread)
        self._queue.put((slot, token))

    def _worker_loop(self) -> None:
        """Цикл воркера: берёт заказ, разбирает, повторяет до ``None``-сигнала."""
        while True:
            item = self._queue.get()
            if item is None:
                return
            slot, token = item
            try:
                self._run_job(slot, token)
            finally:
                with self._lock:
                    self._active.discard(slot)

    def _run_job(self, slot: str, token: str) -> None:
        """Одна работа: экстрактор по оси уровня → разбор файла → результат на диск.

        Результат пишется **только этим поколением** работы: если ядро успело
        сделать ``abort`` и заказать заново, старая работа не перезапишет новую.
        """
        area = self._area(slot)
        meta = self._read_json(area / _META_NAME) or {}
        job = meta.get("job") if isinstance(meta.get("job"), Mapping) else None
        if not job or job.get("token") != token:
            return
        level = str(job.get("level") or "")
        device = str(job.get("device") or PdfDevice.CPU.value)
        version = str(job.get("extractor_version") or "")
        started = _iso(self._clock())
        try:
            extractor, lock, version = self._extractor_for(level, device)
            hint = self._filename_hint(job)
            with lock:
                doc = extractor.extract_path(
                    str(area / _ARTIFACT_NAME), filename_hint=hint
                )
            error = getattr(doc, "error", None)
            if error:
                payload: dict[str, Any] = {
                    "status": "error",
                    "error": str(error),
                    "level": level,
                    "extractor_version": version,
                    "text": None,
                }
            else:
                payload = {
                    "status": "done",
                    "error": None,
                    "level": level,
                    "extractor_version": str(
                        getattr(doc, "extractor_version", None) or version
                    ),
                    "text": self._text_block(doc, version),
                }
        except Exception as exc:  # noqa: BLE001 - сбой обязан стать видимым результатом
            payload = {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "level": level,
                "extractor_version": version,
                "text": None,
            }
        payload["started_at"] = started
        payload["finished_at"] = _iso(self._clock())
        # Токен сверяется **до** записи: если ядро успело сделать `abort` и заказать
        # работу заново, эта работа не имеет права публиковать свой результат (иначе
        # `collect` между двумя работами увидел бы текст от прежнего поколения). Молчание
        # здесь — не потеря факта: результат той работы никому не обещан.
        current = self._read_json(area / _META_NAME) or {}
        if (current.get("job") or {}).get("token") != token:
            return
        self._write_json(area / _RESULT_NAME, payload)
        # Завершение работы — тоже обращение к области: TTL брошенной загрузки не должен
        # срезать готовый результат в тот момент, когда ядро ещё не успело его забрать.
        self._write_json(
            area / _META_NAME, {**current, "updated_at": _iso(self._clock())}
        )

    def _extractor_for(
        self, level: str, device: str
    ) -> tuple[Any, threading.Lock, str]:
        """Возвращает экстрактор (кэш по осям), его замок и идентичность.

        Кэш нужен потому, что построение конвертера дороже документа: веса грузятся один
        раз на пару ``(level, device)``. Замок на пару — потому что конвертер не объявлен
        потокобезопасным: два документа одного уровня идут последовательно, а разные
        уровни могут идти параллельно (``workers`` > 1).

        Идентичность берётся у экстрактора (``.version``) — единственный источник: строка
        версии внутри шва разошлась бы с ядром при первом же апгрейде движка (B2).
        """
        key = (str(level), str(device))
        with self._cache_lock:
            entry = self._extractors.get(key)
            if entry is None:
                extractor = self._factory(str(level), str(device))
                version = str(getattr(extractor, "version", "") or "")
                if not version:
                    raise EdgeAdapterError(
                        "extract: экстрактор не назвал версию (extractor_version) — "
                        "идентичность обязана быть в пространстве имён ядра (B2)"
                    )
                entry = (extractor, threading.Lock(), version)
                self._extractors[key] = entry
        return entry

    # -- сборка артефакта -----------------------------------------------------

    def _ensure_artifact(
        self, area: Path, meta: Mapping[str, Any], expected_hash: str, expected_size: int
    ) -> tuple[Path | None, str | None]:
        """Гарантирует собранный и **проверенный** артефакт; ``(path, None)`` или причина.

        Сборка потоковая: чанки читаются блоками по 1 МиБ, хэш считается по ходу записи,
        поэтому файл на 250 МБ не поднимается в память ни целиком, ни «наполовину».

        Чанки после успешной сборки удаляются: собранный файл — тот же артефакт, и держать
        оба значило бы удвоить диск (500 МБ на документ в 250 МБ). Поэтому повторный
        ``submit`` находит не чанки, а собранный файл и **перепроверяет** его хэш и размер.
        """
        artifact = area / _ARTIFACT_NAME
        chunk_total = int(meta.get("chunk_total") or 0)
        chunks = [area / _CHUNK_PATTERN.format(index) for index in range(chunk_total)]
        missing = [index for index, path in enumerate(chunks) if not path.is_file()]
        if missing:
            if artifact.is_file():
                return self._verify_artifact(artifact, expected_hash, expected_size)
            head = ", ".join(str(index) for index in missing[:5])
            tail = "" if len(missing) <= 5 else f" и ещё {len(missing) - 5}"
            return None, (
                f"артефакт не собран: отсутствуют чанки {head}{tail} "
                f"из {chunk_total}"
            )

        staging = area / f"{_ARTIFACT_NAME}.tmp"
        digest = hashlib.sha256()
        total = 0
        try:
            with open(staging, "wb") as out:
                for chunk in chunks:
                    with open(chunk, "rb") as source:
                        while block := source.read(_IO_BLOCK):
                            digest.update(block)
                            out.write(block)
                            total += len(block)
        except OSError as exc:
            staging.unlink(missing_ok=True)
            return None, f"сборка артефакта не удалась: {exc}"

        actual_hash = digest.hexdigest()
        if total != expected_size:
            staging.unlink(missing_ok=True)
            # Чанки сохраняются: недостающий/лишний байт приходит повтором конкретного
            # чанка, и повторная сборка начнётся с тех же файлов.
            return None, (
                f"размер собранного артефакта {total} байт не совпадает с объявленным "
                f"{expected_size} байт"
            )
        if actual_hash != expected_hash:
            staging.unlink(missing_ok=True)
            return None, (
                f"content_hash не совпал: ожидался {expected_hash}, фактический "
                f"{actual_hash} (байты не те, под которыми заявлен артефакт)"
            )
        os.replace(staging, artifact)
        for chunk in chunks:
            chunk.unlink(missing_ok=True)
        return artifact, None

    def _verify_artifact(
        self, artifact: Path, expected_hash: str, expected_size: int
    ) -> tuple[Path | None, str | None]:
        """Перепроверяет ранее собранный артефакт (чанки уже израсходованы)."""
        digest = hashlib.sha256()
        total = 0
        try:
            with open(artifact, "rb") as source:
                while block := source.read(_IO_BLOCK):
                    digest.update(block)
                    total += len(block)
        except OSError as exc:
            return None, f"собранный артефакт не читается: {exc}"
        actual_hash = digest.hexdigest()
        if total != expected_size or actual_hash != expected_hash:
            # Файл не соответствует адресу — это мусор под чужим именем; его удаляем,
            # чтобы следующий submit не повторил ту же ошибку.
            artifact.unlink(missing_ok=True)
            return None, (
                f"собранный артефакт не соответствует content_hash: ожидался "
                f"{expected_hash} / {expected_size} байт, фактический {actual_hash} / "
                f"{total} байт"
            )
        return artifact, None

    # -- вспомогательное -------------------------------------------------------

    @staticmethod
    def _refused(slot: str, error: str) -> dict[str, Any]:
        """Отказ ``submit`` в терминах контракта (``accepted=false`` + причина)."""
        return {
            "content_hash": slot,
            "accepted": False,
            "extractor_version": None,
            "error": error,
        }

    @staticmethod
    def _collected(
        slot: str,
        status: str,
        *,
        extractor_version: Any = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Минимальный ответ ``collect`` для не-``done`` исходов."""
        return {
            "content_hash": slot,
            "status": status,
            "text": None,
            "extractor_version": extractor_version,
            "error": error,
        }

    @staticmethod
    def _filename_hint(job: Mapping[str, Any]) -> str:
        """Имя с расширением для конвертера: retained-файл называется иначе.

        Артефакт лежит как ``artifact.bin`` (имя — не хэш и не формат): формат выводится
        движком из суффикса, поэтому шов передаёт объявленный ядром ``filename_hint`` либо
        ``format``, а без них — самый частый документный формат корпуса.
        """
        hint = job.get("filename_hint")
        if isinstance(hint, str) and Path(hint).suffix:
            return hint
        declared = job.get("format")
        if isinstance(declared, str) and declared.strip():
            suffix = declared.strip().lstrip(".").lower()
            return f"document.{suffix}"
        return "document.pdf"

    @staticmethod
    def _text_block(doc: Any, version: str) -> dict[str, Any]:
        """Блок ``text`` ответа ``collect`` — тот же состав, что ``fetch.text`` (07 §3).

        Частичность и счётчики едут вместе с текстом: ядро не может вывести «прочитана
        часть документа» из markdown-строки, а без этого признака шов терял бы факт,
        который локальный путь сохраняет (парность профилей).
        """
        content = getattr(doc, "text", "") or ""
        content_str = str(content)
        partial = bool(getattr(doc, "partial", False))
        page_count = getattr(doc, "page_count", None)
        pages_converted = getattr(doc, "pages_converted", None)
        return {
            "content": content_str,
            "hash": hash_bytes(content_str),
            "extractor_version": str(getattr(doc, "extractor_version", None) or version),
            "page_count": page_count if isinstance(page_count, int) else None,
            "pages_converted": (
                pages_converted if isinstance(pages_converted, int) else None
            ),
            "partial": partial,
            "partial_reason": (
                str(getattr(doc, "partial_reason", "") or "") or None if partial else None
            ),
        }

    def _slot(self, payload: Mapping[str, Any]) -> str:
        """Идентификатор артефакта из запроса и его проверка как **имени** каталога."""
        value = payload.get("content_hash")
        text = str(value).strip() if isinstance(value, str) else ""
        if not _HASH_NAME.match(text):
            raise EdgeRequestError(
                "extract: content_hash обязан быть словом из [0-9A-Za-z_-] длиной 1..128 "
                f"(получено {value!r}): он адресует staging-область "
                f"({self._root})"
            )
        return text

    def _area(self, slot: str) -> Path:
        """Каталог staging-области артефакта (может ещё не существовать)."""
        return self._root / slot

    @staticmethod
    def _required_int(payload: Mapping[str, Any], field: str) -> int:
        """Обязательное целое поле; неверное — отказ запроса, а не сюрприз в сборке."""
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise EdgeRequestError(
                f"extract: поле {field} обязано быть неотрицательным целым (получено {value!r})"
            )
        return value

    @staticmethod
    def _check_consistency(
        meta: Mapping[str, Any], *, size: int, chunk_total: int
    ) -> None:
        """Сверяет объявленные ``size``/``chunk_total`` с уже принятыми чанками.

        Расхождение — отказ с обоими числами: иначе чанки разных наборов склеились бы в
        файл, чей размер и хэш заведомо не совпадут с объявленными, и настоящая причина
        (передача смешала два артефакта) осталась бы невидимой.
        """
        stored_total = meta.get("chunk_total")
        if isinstance(stored_total, int) and stored_total != chunk_total:
            raise EdgeRequestError(
                f"extract: chunk_total={chunk_total} расходится с уже принятым "
                f"chunk_total={stored_total} для этого content_hash — передача смешивает "
                "два набора чанков"
            )
        stored_size = meta.get("size")
        if isinstance(stored_size, int) and stored_size != size:
            raise EdgeRequestError(
                f"extract: size={size} расходится с уже принятым size={stored_size} для "
                "этого content_hash — чанки принадлежат разным артефактам"
            )

    @staticmethod
    def _received_chunks(area: Path, chunk_total: int) -> list[int]:
        """Номера фактически присутствующих чанков (по файлам, а не по счётчику)."""
        return [
            index
            for index in range(chunk_total)
            if (area / _CHUNK_PATTERN.format(index)).is_file()
        ]

    @staticmethod
    def _decode_b64_to_file(text: Any, path: Path) -> int:
        """Потоково декодирует base64 в файл; возвращает число записанных байт.

        Блоками, а не ``b64decode`` целиком: тело запроса уже прочитано транспортом (оно
        ограничено ``max_body_bytes``), но декодированная копия не обязана появляться в
        памяти — она сразу ложится на диск. ``validate=True`` на каждом блоке превращает
        мусор в понятный отказ, а не в «тихо склеенные» байты.
        """
        if not isinstance(text, str):
            path.unlink(missing_ok=True)
            raise EdgeRequestError("extract/upload: data_b64 обязан быть строкой base64")
        written = 0
        try:
            with open(path, "wb") as handle:
                for start in range(0, len(text), _B64_BLOCK):
                    block = base64.b64decode(text[start : start + _B64_BLOCK], validate=True)
                    handle.write(block)
                    written += len(block)
        except (binascii.Error, ValueError) as exc:
            path.unlink(missing_ok=True)
            raise EdgeRequestError(
                f"extract/upload: data_b64 не является корректным base64: {exc}"
            ) from exc
        return written

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        """Читает JSON-файл состояния; ``None`` — файла нет/он нечитаем."""
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return dict(data) if isinstance(data, Mapping) else None

    def _write_json(self, path: Path, data: Mapping[str, Any]) -> None:
        """Пишет JSON атомарно (tmp + ``os.replace``) и **никогда** не роняет работника.

        Сбой записи означает, что области уже нет (``abort``/TTL). Результат такой работы
        никому не обещан, поэтому ошибка глушится — но не молча: она становится строкой
        наблюдаемости.
        """
        staging = path.with_name(f"{path.name}.tmp")
        try:
            staging.write_text(
                json.dumps(data, ensure_ascii=False, sort_keys=True), encoding="utf-8"
            )
            os.replace(staging, path)
        except OSError as exc:
            self._log(f"edge extract: не удалось записать {path}: {exc}")

    @staticmethod
    def _payload_bytes(area: Path) -> int:
        """Байты **артефакта** в staging-области: чанки и собранный файл (без учёта)."""
        total = 0
        try:
            entries = list(area.iterdir())
        except OSError:
            return 0
        for entry in entries:
            name = entry.name
            if not (name.startswith("chunk_") or name == _ARTIFACT_NAME):
                continue
            try:
                if entry.is_file():
                    total += entry.stat().st_size
            except OSError:
                continue
        return total

    @staticmethod
    def _area_bytes(area: Path) -> int:
        """Все байты staging-области (то, что освободит ``removed_bytes``)."""
        total = 0
        try:
            entries = list(area.iterdir())
        except OSError:
            return 0
        for entry in entries:
            try:
                if entry.is_file():
                    total += entry.stat().st_size
            except OSError:
                continue
        return total


def _stderr_log(message: str) -> None:
    """Канал наблюдаемости edge по умолчанию (``stderr``, как у планировщика)."""
    sys.stderr.write(f"{message}\n")


def _new_token() -> str:
    """Токен поколения работы: различает повторный ``submit`` после ``abort``."""
    return uuid.uuid4().hex


def _seconds(value: int) -> timedelta:
    """``timedelta`` из секунд (одно место, где объявленный TTL становится временем)."""
    return timedelta(seconds=int(value))


def _mtime(area: Path) -> datetime | None:
    """Время последнего изменения каталога — резерв, если ``meta.json`` нечитаем."""
    try:
        return datetime.fromtimestamp(area.stat().st_mtime, tz=UTC)
    except OSError:
        return None
