"""Remote-прокси экстрактора — ``RemoteExtractor`` (W13/T13.3; 07 §4, §8).

Реализация ``ExtractorPort`` в ядре для профиля ``split``: разбор недоверенных
байтов (docling/trafilatura) исполняется в DMZ, ядро вызывает его через
``ContentGateway``. Контракт порта **не меняется** (07 §4).

**Два маршрута, один порт.**

* **По артефакту** — ``POST /v1/extract/{upload,submit,collect,abort}``
  (``contracts/seam/extract.schema.json``, 07 §8). Применяется к документным
  форматам: ядро уже ретенировало байты в ``corpus_raw`` и передаёт их **чанками**,
  а edge извлекает ровно эти байты и **не ходит в сеть**. Что это чинит сразу:
  пере-фетч (байты не берутся второй раз, провенанс не раздваивается), члены
  контейнеров (у члена синтетический ``{archive}!/{member}`` URL — фетчить нечего,
  но файл на диске есть), ось ``crawl.document_level``/``crawl.pdf_device`` (едут в
  ``submit``) и идентичность экстрактора (edge называет свой экстрактор в том же
  пространстве имён, поэтому ``pb`` под ``split`` совпадает с ``local``).
* **По URL** — ``POST /v1/fetch`` (форма протокола 07 §3): прежний путь для страниц.
  HTML разбирает trafilatura на edge, байты приезжают в том же ответе, второго
  обмена нет — он дешевле потоковой загрузки и остаётся дефолтом для не-документов.

**Идентичность артефакта — ``content_hash`` байтов.** Хеш считается тем же
:func:`core.util.hashing.content_hash` (SHA-256), которым ядро называет retained-файл
(``corpus_raw/{product_id}/{sha256}.bin``, I19), поэтому staging-адрес на edge — это
ровно идентификатор того артефакта, который ядро записало, а повторная загрузка тех же
байтов адресует ту же область (идемпотентность повтора).

**Файл читается с диска, а не поднимается в RAM.** Источник байтов —
``page.source_path`` (retained-файл), иначе ``page.content``. Файл читается чанками, и
каждый чанк кодируется в base64 **по одному**: ``b64`` всего артефакта не
материализуется никогда (иначе 250-MB PDF стоил бы ~333 MB RSS в двух копиях).
Размер чанка — из ``capabilities.extract.max_upload_chunk_bytes`` (аддитивный блок
шва); при отсутствии блока (edge старше контракта) — откат на
:data:`DEFAULT_UPLOAD_CHUNK_BYTES`.

I19 на обоих маршрутах: ядро **пересчитывает** ``content_hash`` очищенного текста само
(``extracted_doc_from_response``), хэшу edge не доверяет.

**Семейство формата.** Публикуемый payload шва не несёт ``ExtractedDoc.family``
(расширять контракт ради поля чанкера не требуется): прокси выводит его тем же реестром,
которым руководствовался edge — ``textual_family(page.url, page.content_type)``. Правило
одно, поэтому решение о чанкере в ``split`` совпадает с ``local``; EPUB/HTML/docling
остаются ``None`` (markdown-совместимое семейство).
Нейтрален (§1.3).
"""

from __future__ import annotations

import base64
import hashlib
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from core.adapters.remote_gateway import (
    RemoteContentError,
    extracted_doc_from_response,
)
from core.domain.enums import DocumentLevel, PdfDevice
from core.ports.gateway import ContentGateway
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchMode, RawPage
from core.util.document_formats import (
    document_content_type,
    document_suffix,
    textual_family,
)
from core.util.hashing import content_hash

__all__ = [
    "DEFAULT_EXTRACT_DEADLINE_S",
    "DEFAULT_POLL_INTERVAL_S",
    "DEFAULT_UPLOAD_CHUNK_BYTES",
    "RemoteExtractor",
]

#: Размер чанка загрузки по умолчанию (байты) — **откат**, а не норма.
#:
#: Норма — объявленный ``capabilities.extract.max_upload_chunk_bytes``: размер чанка
#: ограничен не ядром, а тем, что edge готов принять одним запросом (его лимит тела,
#: его буфер сборки). Значение действует только против edge, который блока
#: ``capabilities.extract`` ещё не отдаёт (контракт ``extract/*`` аддитивен): без отката
#: такой edge не получил бы артефакт вовсе, хотя его ``/v1/extract/*`` может быть уже
#: реализован. 4 MiB — не потолок артефакта (артефакт любого размера режется на чанки),
#: а цена одного запроса: 4 MiB сырых байтов → 5.3 MiB base64 в JSON.
DEFAULT_UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024

#: Потолок ожидания извлечения по умолчанию (секунды) — **откат**, а не норма.
#:
#: Норма — объявленная ось ``crawl.document_timeout_s``: она приходит параметром
#: ``document_timeout`` из composition root, а её дефолт
#: (``core.pipeline.crawl_frontier.DEFAULT_DOCUMENT_TIMEOUT_S`` = 1800 с) — прежний
#: зашитый потолок одной задачи экстракции. Адаптер не импортирует ``core.pipeline``
#: (матрица слоёв 01 §1), поэтому значение повторено здесь откатом; менять его можно
#: только вместе с осью.
DEFAULT_EXTRACT_DEADLINE_S = 1800.0

#: Пауза между опросами ``extract/collect`` (секунды). Извлечение документа — секунды/
#: минуты, поэтому опрос не должен быть ни busy-loop, ни источником задержки.
DEFAULT_POLL_INTERVAL_S = 2.0

#: Читаемый блок при потоковом хешировании файла (байты).
_HASH_READ_CHUNK = 1024 * 1024


def _opt_str(value: Any) -> str | None:
    """Непустая строка либо ``None`` (необязательные поля ответа шва)."""
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _filename_hint(url: str) -> str | None:
    """Имя файла из URL — подсказка формата для edge (``None``, если имени нет).

    Для члена контейнера URL синтетический (``{archive}!/{member}``), но basename
    последнего сегмента — это имя члена, и оно несёт суффикс формата.
    """
    path = urlsplit(url).path
    name = Path(unquote(path)).name
    return name or None


def _declared_format(page: RawPage) -> str | None:
    """Документный формат из **реестра** :mod:`core.util.document_formats`.

    Суффикс сильнее ``Content-Type`` (правило реестра: ``report.pdf``, отданный как
    ``text/plain``, остаётся PDF). ``None`` — формат не объявлен (документный маршрут
    выбран probe-ом выше по течению, формат edge определит сам).
    """
    suffix = document_suffix(page.url)
    if suffix is not None:
        return suffix.lstrip(".")
    mime_suffix = document_content_type(page.content_type)
    return mime_suffix.lstrip(".") if mime_suffix else None


@dataclass(frozen=True)
class _ArtifactSource:
    """Источник байтов артефакта: retained-файл (``corpus_raw``) либо буфер памяти.

    Файл предпочтительнее по построению: ``source_path`` задаётся ядром именно для
    того, чтобы тело документа не поднималось в RAM воркера. ``content`` — откат для
    вызовов без retention-файла (probe-страница, ручной путь).
    """

    path: Path | None = None
    content: bytes = b""

    @classmethod
    def of(cls, page: RawPage) -> _ArtifactSource | None:
        """Источник байтов страницы либо ``None``, когда байтов нет вовсе."""
        if page.source_path:
            path = Path(page.source_path)
            try:
                if path.is_file():
                    return cls(path=path)
            except OSError:  # путь есть, файла нет — переходим к буферу
                pass
        if page.content:
            return cls(content=page.content)
        return None

    @property
    def size(self) -> int:
        """Размер артефакта в байтах (``chunk_total`` и ``size`` в запросе загрузки)."""
        if self.path is None:
            return len(self.content)
        return self.path.stat().st_size

    def digest(self) -> str:
        """``content_hash`` артефакта — SHA-256, тот же, что у retained-файла (I19).

        Файл хешируется потоково (:data:`_HASH_READ_CHUNK`): хеш не требует тела в RAM.
        """
        if self.path is None:
            return content_hash(self.content)
        hasher = hashlib.sha256()
        with self.path.open("rb") as handle:
            for block in iter(lambda: handle.read(_HASH_READ_CHUNK), b""):
                hasher.update(block)
        return hasher.hexdigest()

    def chunks(self, chunk_bytes: int) -> Iterator[bytes]:
        """Чанки артефакта: файл читается с диска по одному блоку за шаг.

        Пустой артефакт даёт один пустой чанк: схема требует ``chunk_total >= 1``, а
        «ноль чанков» edge не отличил бы от незавершённой загрузки.
        """
        size = self.size
        if size <= 0:
            yield b""
            return
        if self.path is None:
            for start in range(0, size, chunk_bytes):
                yield self.content[start : start + chunk_bytes]
            return
        with self.path.open("rb") as handle:
            while True:
                block = handle.read(chunk_bytes)
                if not block:
                    return
                yield block


class RemoteExtractor:
    """``ExtractorPort`` через DMZ-шов. Drop-in для реального экстрактора (07 §4)."""

    #: Ядро передаёт **локальные байты** (``page.source_path``) — в том числе для членов
    #: контейнеров: их URL синтетический, фетчить его нельзя, а файл на диске есть.
    supports_source_path = True

    def __init__(
        self,
        gateway: ContentGateway,
        *,
        version: str = "remote-extractor-1",
        document_level: DocumentLevel | str = DocumentLevel.FULL,
        pdf_device: PdfDevice | str = PdfDevice.CPU,
        document_timeout: float | None = None,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        upload_chunk_bytes: int | None = None,
    ) -> None:
        """``version`` — идентичность **прокси**, а не экстрактора (B2).

        Идентичность экстрактора приходит в ответе ``extract/collect`` и попадает в
        ``ExtractedDoc.extractor_version``: подставить сюда ``remote-extractor-1`` значило
        бы развести ``pb`` под ``split`` и ``local`` по построению (07 §8). ``version``
        остаётся для маршрута ``/v1/fetch``, где edge не называет экстрактор отдельным
        полем, и для ключа ``extract_cache`` (у прокси нет сетевого способа узнать версию
        до первого извлечения).

        ``document_level``/``pdf_device`` — **объявленные** оси (``crawl.document_level``
        и ``crawl.pdf_device``), а не чтение окружения: они едут в ``extract/submit``,
        поэтому смена уровня меняет результат под ``split`` так же, как под ``local``, а
        «ядро объявило ``cuda``, edge извлёк на CPU» перестаёт быть невидимым.
        ``document_timeout`` — объявленная ось ``crawl.document_timeout_s`` либо ``None``
        (действует :data:`DEFAULT_EXTRACT_DEADLINE_S`): это **общий дедлайн** ожидания
        извлечения (submit + опрос ``collect``), после которого загрузка снимается
        ``extract/abort``.
        """
        self._gateway = gateway
        self.version = version
        self._level = DocumentLevel(document_level)
        self._device = PdfDevice(pdf_device)
        self._timeout = (
            DEFAULT_EXTRACT_DEADLINE_S
            if document_timeout is None
            else float(document_timeout)
        )
        self._poll_interval = max(0.0, float(poll_interval_s))
        self._declared_chunk_bytes = (
            DEFAULT_UPLOAD_CHUNK_BYTES
            if upload_chunk_bytes is None
            else int(upload_chunk_bytes)
        )
        if self._declared_chunk_bytes <= 0:
            raise ValueError("upload_chunk_bytes должен быть положительным")
        # Кеш объявленного блока ``capabilities.extract``: один вызов шва на прокси, а не
        # на документ. Гонка нескольких потоков пула безвредна (результат один и тот же).
        self._extract_caps: Mapping[str, Any] | None = None

    # -- свойства --------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Доступен ли транспорт шва (``httpx`` или внедрённый ``transport``)."""
        return bool(getattr(self._gateway, "available", True))

    # -- контракт ExtractorPort ----------------------------------------------- #

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """Извлечь документ: по артефакту (``extract/*``) либо по URL (``fetch``).

        Маршрут выбирается **форматом страницы**, а не наличием байтов: ``source_path``
        есть и у обычной HTML-страницы (воркер шва всегда ретенирует байты), поэтому
        «есть файл» не означает «это документ». Документным считается маршрут
        ``DOCUMENT`` (его резолвит pipeline) либо объявленный реестром
        :mod:`core.util.document_formats` суффикс/``Content-Type`` — то же правило, по
        которому локальный композит выбирает docling, иначе ``split`` и ``local``
        выбрали бы разные движки для одного артефакта. Явный маршрут ``HTML`` сильнее
        реестра (как у локального композита): текста документа там не ждут.
        """
        resolved_route = ExtractionRoute(route)
        source = self._artifact_source(page, resolved_route)
        if source is None:
            return self._extract_by_url(page, resolved_route)
        if self._extract_capabilities().get("supports_artifact") is False:
            # Edge сам объявил, что подпротокола ``extract/*`` у него нет. Тихий откат на
            # ``/v1/fetch`` вернул бы ровно тот дефект, который закрывается (пере-фетч,
            # раздвоение провенанса), а для члена контейнера — фетч синтетического URL.
            return self._failure(
                "edge объявил extract.supports_artifact=false: извлечение по артефакту "
                "недоступно, адрес артефакта — его content_hash",
                None,
            )
        if self._level is DocumentLevel.OFF:
            # Ось объявила «документы не извлекать». Гнать ради этого артефакт по шву —
            # платить передачей за отказ, который известен до неё; текст отказа — тот же,
            # что у локального docling-адаптера (``document_level=off``), чтобы профили
            # расходились местом исполнения, а не формулировкой.
            return self._failure(
                "document_level=off: документы не извлекаются", None
            )
        return self._extract_by_artifact(page, source)

    # -- маршрут по URL (прежний, /v1/fetch) ------------------------------------ #

    def _extract_by_url(self, page: RawPage, route: ExtractionRoute) -> ExtractedDoc:
        """``/v1/fetch`` → ``ExtractedDoc`` (повторный хэш, I19).

        Байты повторно не передаются: edge сам выполняет fetch по ``url``
        (форма протокола 07 §3). ``url``/``fetch``/``lang`` берутся из ``page``; маршрут
        извлечения — из аргумента ``route`` (резолвится в pipeline). ``wait_for`` не
        переносится: ``RawPage`` его не несёт.
        """
        fetch = None if page.fetch_mode is None else FetchMode(page.fetch_mode)
        payload = {
            "url": page.url,
            # ``fetch=None`` — ось получения не наблюдалась (ручная подача артефакта):
            # выдать ``http`` значило бы объявить провенанс, которого не было. Шов
            # принимает ``null`` (``fetch`` — опциональная enum-ось, 07 §3).
            "fetch": None if fetch is None else fetch.value,
            "lang": page.lang,
            "extract": route.value,
            "wait_for": None,
        }
        response = self._call("fetch", payload)
        doc = extracted_doc_from_response(response, version=self.version)
        family = textual_family(page.url, page.content_type)
        if family is None or doc.error is not None:
            return doc
        return replace(doc, family=family)

    # -- маршрут по артефакту (/v1/extract/*) ---------------------------------- #

    def _artifact_source(
        self, page: RawPage, route: ExtractionRoute
    ) -> _ArtifactSource | None:
        """Источник байтов, если страница идёт маршрутом «по артефакту», иначе ``None``."""
        if route is ExtractionRoute.HTML:
            return None
        if not self._document_declared(page, route):
            return None
        return _ArtifactSource.of(page)

    @staticmethod
    def _document_declared(page: RawPage, route: ExtractionRoute) -> bool:
        """Документный ли формат страницы (правило реестра, не эвристика по расширению).

        ``DOCUMENT`` — объявленный маршрут (pipeline уже решил: суффикс/``Content-Type``
        документа). Иначе — тот же реестр, что у локального композита: суффикс
        документа сильнее ``Content-Type``, текстовые форматы и EPUB документами **не**
        являются (у них свои читатели в ядре).
        """
        if route is ExtractionRoute.DOCUMENT:
            return True
        return bool(document_suffix(page.url) or document_content_type(page.content_type))

    def _extract_by_artifact(
        self, page: RawPage, source: _ArtifactSource
    ) -> ExtractedDoc:
        """``upload`` → ``submit`` → опрос ``collect`` по идентичности артефакта.

        Порядок и уборка:

        1. хеш и размер считаются **от источника** (I19: адрес артефакта — его байты);
        2. байты уходят чанками; неполная загрузка — видимый сбой **и** ``abort``
           (иначе брошенная загрузка заняла бы диск до TTL);
        3. ``submit`` несёт объявленные оси (уровень/устройство) и формат;
        4. ``collect`` опрашивается до терминального статуса в пределах **общего**
           дедлайна; по истечении — ``abort`` и сбой «дедлайн».

        **Сбой извлечения** (неполная загрузка, отказ ``submit``, ``status=error|unknown``,
        дедлайн) виден как ``ExtractedDoc.error`` — так же, как у локальных адаптеров:
        исключение потеряло бы запись об артефакте вместо видимого пропуска. Транспортный
        сбой шва (``GatewayTransportError``) по-прежнему пробрасывается — ровно как на
        маршруте ``/v1/fetch``: недоступность edge — не «плохой документ».
        """
        digest = source.digest()
        size = source.size
        upload_error = self._upload(source, digest, size)
        if upload_error is not None:
            self._abort(digest)
            return self._failure(upload_error, identity=None)

        deadline = time.monotonic() + self._timeout
        submitted = self._call(
            "extract/submit",
            {
                "content_hash": digest,
                "size": size,
                "level": self._level.value,
                "device": self._device.value,
                "format": _declared_format(page),
                "filename_hint": _filename_hint(page.url),
                "lang": page.lang,
                # Ожидание ядра не объявляется: сборка экстрактора edge (``docling-<build>``)
                # известна только edge, а подставленное число было бы ложным провенансом.
                "extractor_declared": None,
            },
        )
        identity = _opt_str(submitted.get("extractor_version"))
        if submitted.get("accepted") is False:
            self._abort(digest)
            reason = _opt_str(submitted.get("error")) or "причина не названа"
            return self._failure(f"edge не принял артефакт в извлечение: {reason}", identity)

        collected = self._collect(digest, deadline)
        if collected is None:
            self._abort(digest)
            return self._failure(
                f"дедлайн извлечения {self._timeout:g} с истёк: артефакт "
                f"{digest[:12]}… снят с edge (extract/abort)",
                identity,
            )
        return self._doc_from_collect(collected, digest, identity)

    def _doc_from_collect(
        self, collected: Mapping[str, Any], digest: str, identity: str | None
    ) -> ExtractedDoc:
        """Терминальный ответ ``collect`` → ``ExtractedDoc`` (текст пере-хешируется, I19).

        Идентичность экстрактора берётся **из ответа edge** (B2): вложенное
        ``text.extractor_version`` — тот же носитель, что и в ``/v1/fetch``; поле
        верхнего уровня контракта имеет приоритет. Если edge идентичность не назвал,
        поле остаётся пустым — подставлять ``remote-extractor-1`` значило бы выдать
        прокси за извлекатель и развести ``pb`` с ``local``.
        """
        answered = _opt_str(collected.get("content_hash"))
        if answered is not None and answered != digest:
            return self._failure(
                "ответ edge относится к другому артефакту: "
                f"content_hash={answered!r} вместо {digest!r}",
                identity,
            )
        status = str(collected.get("status") or "")
        edge_version = _opt_str(collected.get("extractor_version")) or identity
        if status == "error":
            reason = _opt_str(collected.get("error")) or "причина не названа"
            return self._failure(f"edge не извлёк артефакт: {reason}", edge_version)
        if status == "unknown":
            return self._failure(
                "edge не знает артефакт (status=unknown): работа не найдена",
                edge_version,
            )
        if status != "done":
            # Схема перечисляет статусы закрытым набором; новый/пустой — не «ожидание»,
            # иначе ядро ждало бы его до дедлайна, приняв незнание за работу.
            return self._failure(
                f"edge вернул неизвестный статус извлечения {status!r}",
                edge_version,
            )
        text_block = collected.get("text")
        if not isinstance(text_block, Mapping):
            return self._failure(
                "edge вернул status=done без поля text (текста нет) — сбой содержимого",
                edge_version,
            )
        merged = dict(text_block)
        inner = _opt_str(text_block.get("extractor_version"))
        resolved = edge_version or inner
        if resolved:
            merged["extractor_version"] = resolved
        return extracted_doc_from_response({"text": merged}, version=resolved or "")

    # -- транспорт шва --------------------------------------------------------- #

    def _call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Один вызов шва; не-``Mapping`` ответ — ошибка содержимого."""
        response = self._gateway.call(op, payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError(
                f"{op}: payload ответа edge не является отображением"
            )
        return response

    def _upload(
        self, source: _ArtifactSource, digest: str, size: int
    ) -> str | None:
        """Загрузить артефакт чанками; ``None`` — загрузка принята целиком.

        Источник читается по одному чанку, base64 считается **на чанк**: строка ``b64``
        всего артефакта не материализуется никогда. ``chunk_total`` объявляется заранее
        (схема требует его в каждом чанке), поэтому расхождение с фактически прочитанным
        видно как сбой, а не как тихая склейка.
        """
        chunk_bytes = self._upload_chunk_bytes()
        chunk_total = max(1, (size + chunk_bytes - 1) // chunk_bytes)
        sent = 0
        read = 0
        last: Mapping[str, Any] | None = None
        for block in source.chunks(chunk_bytes):
            if sent >= chunk_total:
                # Источник вырос между определением размера и чтением: лишний чанк не
                # отправляется (он вышел бы за объявленный ``chunk_total``), но факт
                # виден вызывающему как расхождение.
                break
            last = self._call(
                "extract/upload",
                {
                    "content_hash": digest,
                    "size": size,
                    "chunk_no": sent,
                    "chunk_total": chunk_total,
                    "data_b64": base64.b64encode(block).decode("ascii"),
                },
            )
            sent += 1
            read += len(block)
        if sent != chunk_total or read != size:
            return (
                f"источник отдал {sent} чанков/{read} байт вместо объявленных "
                f"{chunk_total}/{size}: артефакт изменился между замером и чтением"
            )
        if last is None:  # pragma: no cover — при chunk_total >= 1 цикл исполнится
            return "upload артефакта: источник не дал ни одного чанка"
        received = last.get("received_chunks")
        edge_total = last.get("chunk_total", chunk_total)
        if last.get("complete") is not True or received != chunk_total or edge_total != chunk_total:
            return (
                f"upload артефакта не завершён: edge принял {received!r} из {chunk_total} "
                f"чанков (complete={last.get('complete')!r})"
            )
        received_bytes = last.get("received_bytes")
        if isinstance(received_bytes, int) and received_bytes != size:
            return (
                f"upload артефакта: edge принял {received_bytes} байт вместо {size} — "
                "собранный на edge артефакт не тот, что ретенировало ядро"
            )
        return None

    def _upload_chunk_bytes(self) -> int:
        """Размер чанка: ``capabilities.extract.max_upload_chunk_bytes`` → откат.

        ``capabilities`` спрашивается один раз на прокси (кеш
        :attr:`_extract_caps`). Недоступность/старый edge — откат на объявленную
        константу, а не отказ: контракт ``extract/*`` аддитивен, и блок в
        ``capabilities`` мог появиться позже самих операций.
        """
        value = self._extract_capabilities().get("max_upload_chunk_bytes")
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
        return self._declared_chunk_bytes

    def _extract_capabilities(self) -> Mapping[str, Any]:
        """Блок ``capabilities.extract`` (``{}``, если edge его не объявил).

        Блок — аддитивное расширение контракта: старый edge его не отдаёт, и это не
        ошибка, а факт, из которого берутся откаты (размер чанка, ``supports_artifact``).
        Сбой самого запроса ``capabilities`` тоже не роняет извлечение: он лишь означает,
        что объявленных возможностей нет.
        """
        if self._extract_caps is None:
            try:
                capabilities = self._gateway.capabilities()
            except Exception:  # noqa: BLE001 — capabilities не обязательны для извлечения
                capabilities = {}
            block = (
                capabilities.get("extract") if isinstance(capabilities, Mapping) else None
            )
            self._extract_caps = dict(block) if isinstance(block, Mapping) else {}
        return self._extract_caps

    def _collect(self, digest: str, deadline: float) -> Mapping[str, Any] | None:
        """Опрашивать ``extract/collect`` до терминального статуса или дедлайна.

        ``pending`` — работа идёт (повтор после паузы); ``done``/``error``/``unknown`` —
        терминальны и возвращаются вызывающему как есть. ``None`` — дедлайн истёк
        (ядро обязано снять загрузку: незавершённая работа иначе остаётся на edge).
        """
        while True:
            collected = self._call(
                "extract/collect",
                {"content_hash": digest, "level": self._level.value},
            )
            if str(collected.get("status") or "") != "pending":
                return collected
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(self._poll_interval, remaining))

    def _abort(self, digest: str) -> None:
        """Снять staging-область/незавершённую работу артефакта (best-effort).

        Это **уборка**, а не исход: её сбой не должен ни подменять исходную ошибку, ни
        бросаться наружу. TTL на edge — страховка, а не единственный механизм (схема
        ``extract/abort``), поэтому явный вызов всё равно нужен.
        """
        try:
            self._call("extract/abort", {"content_hash": digest})
        except Exception:  # noqa: BLE001 — уборка не подменяет исходную ошибку
            return

    def _failure(self, message: str, identity: str | None) -> ExtractedDoc:
        """Видимый сбой извлечения (``ExtractedDoc.error``), а не исключение.

        ``extractor_version`` — только **наблюдённая** идентичность edge; ``None``
        означает «edge не назвал экстрактор» и остаётся пустой строкой, а не именем
        прокси (B2).
        """
        return ExtractedDoc(
            text="",
            content_hash=content_hash(""),
            extractor_version=identity or "",
            error=message,
        )
