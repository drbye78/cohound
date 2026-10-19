"""Контент-адресуемый кэш экстракции, переживающий прогоны (Ш5; I19).

Экстракция документных форматов (PDF/Office) — доминирующая стоимость цикла: ``corpus_docs``
живёт в **per-run** БД, поэтому новый недельный прогон пере-извлекает все документы даже при
байт-в-байт идентичном сырье. Conditional fetch (``304``) экономит только скачивание. Этот
модуль экономит собственно извлечение: результат кладётся на диск под ключом
``(content_hash ретенированных байтов, extractor_version, level)`` и переиспользуется
следующим прогоном.

**Раскладка.** ``{root}/{content_hash[:2]}/{content_hash}.json`` — без центрального индекса
и без блокировок: путь выводится из ключа, поэтому разные процессы пишут в разные файлы, а
совпадающие ключи дают идентичное содержимое. Писатель не блокирует читателя: запись
атомарна (временный файл в том же каталоге → :func:`os.replace`), поэтому убитый процесс
оставляет либо старый, либо новый конверт, но не половину.

**Конверт** (JSON, UTF-8) несёт ``format`` (версия формата), ``content_hash`` (ключ — хеш
ретенированных байтов), ``extractor_version``, ``level``, ``text``, ``page_count`` и
``doc_content_hash`` — хеш **извлечённого текста** (поле ``ExtractedDoc.content_hash``).
Два хеша различны по смыслу и оба обязательны: ключ адресует сырьё, ``doc_content_hash``
восстанавливает ровно тот ``ExtractedDoc``, который вернул экстрактор (по нему pipeline
делает дедуп и строит ``doc_id``). Несовпадение ``content_hash``/``extractor_version``/
``level`` с запросом — **промах**, а не ошибка: устаревший конверт не должен никогда
обслуживаться. Несовпадение версии формата и битый/нечитаемый JSON — тоже промах
(считается в :meth:`ExtractionCache.stats`), но не исключение.

**Кэшируются только успешные извлечения** (``error is None`` и непустой ``text``). Сбой
парсера часто транзиентен или зависит от окружения (дефект OCR, нехватка памяти, отсутствие
системной библиотеки); закэшированный сбой заморозил бы плохой результат на все будущие
циклы, тогда как повторная попытка в другом окружении может дать текст. Поэтому ошибки не
сохраняются вовсе.

**I19.** Модуль сам не читает ``corpus_raw`` и не может проверить, что ключ соответствует
фактическим байтам: верификация — обязанность вызывающего (в pipeline ключ считается из
байтов, только что прочитанных из retained-файла, и хит принимается лишь при совпадении).
Кэш лишь гарантирует, что обслуживает конверт ровно под запрошенный ключ.

**Детерминизм.** :meth:`get` — чистая функция аргументов и состояния диска: ни сети, ни
LLM, ни часов. Счётчики :meth:`stats` — наблюдаемость экземпляра, на решение не влияют.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from core.ports.types import ExtractedDoc

__all__ = ["ENVELOPE_FORMAT", "ExtractionCache"]

#: Версия формата конверта. Входит в конверт: будущая ревизия схемы не будет обслужена
#: старым читателем (для него это промах), поэтому несовместимая запись безопасна.
ENVELOPE_FORMAT = 1

#: Допустимые символы ключа: полный hex (SHA-256). Заодно исключает ``/``, ``..`` и
#: прочие пути-ловушки: ключ — имя файла, а не произвольная строка.
_HASH_CHARS = frozenset("0123456789abcdefABCDEF")

#: Минимальная длина ключа: два символа нужны для шарда ``content_hash[:2]``.
_MIN_KEY_LEN = 2


def _as_text(value: Any) -> str:
    """Приводит ``StrEnum`` (``DocumentLevel``) к строке; прочее — ``str``."""
    return str(getattr(value, "value", value))


def _valid_key(content_hash: Any) -> bool:
    """Ключ пригоден как имя файла: непустой hex длиной ≥ 2 (без ``/``/``..``)."""
    if not isinstance(content_hash, str) or len(content_hash) < _MIN_KEY_LEN:
        return False
    return all(char in _HASH_CHARS for char in content_hash)


class ExtractionCache:
    """Дисковый кэш результатов экстракции (см. модульный docstring).

    :param root: корень кэша; конструктор **не** создаёт каталог и не делает I/O.
    :param max_bytes: бюджет размера (сумма ``*.json`` под ``root``); ``None`` — без
        вытеснения. После ``put``, превысившего бюджет, самые старые по ``mtime`` записи
        удаляются, пока сумма не вернётся в бюджет. Индекса нет: каталог сканируется
        только в момент превышения. Запись, которая одна больше бюджета, тоже вытесняется
        (кэш остаётся в бюджете, пусть и пустым).

    Экземпляры над одним ``root`` эквивалентны: состояние — только файлы на диске.
    """

    def __init__(self, root: str | Path, *, max_bytes: int | None = None) -> None:
        if max_bytes is not None:
            if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
                raise TypeError("max_bytes должен быть int или None")
            if max_bytes < 0:
                raise ValueError("max_bytes не может быть отрицательным")
        self._root = Path(root)
        self._max_bytes = max_bytes
        # Наблюдаемость (не влияет на решения get/put).
        self._hits = 0
        self._misses = 0
        self._writes = 0
        self._corrupt = 0
        self._evicted = 0
        self._invalid_key = 0

    # ------------------------------------------------------------------ #
    # Свойства (нужны composition root'у, чтобы передать конфиг в воркер)
    # ------------------------------------------------------------------ #

    @property
    def root(self) -> Path:
        """Корень кэша (только чтение; каталог может ещё не существовать)."""
        return self._root

    @property
    def max_bytes(self) -> int | None:
        """Бюджет размера (``None`` — без вытеснения)."""
        return self._max_bytes

    # ------------------------------------------------------------------ #
    # Чтение
    # ------------------------------------------------------------------ #

    def get(
        self, content_hash: str, extractor_version: str, level: str
    ) -> ExtractedDoc | None:
        """Возвращает закэшированный :class:`ExtractedDoc` или ``None`` (промах).

        Промах — это отсутствие файла, битый/нечитаемый JSON, несовпадение версии формата,
        а также несовпадение любого элемента ключа (``content_hash``/
        ``extractor_version``/``level``). Ни один из случаев не бросает исключение:
        устаревший или повреждённый конверт не должен ломать прогон и не должен
        обслуживаться. Невалидный ключ (не hex / содержит путь) — тоже промах.

        Функция детерминирована: результат зависит только от аргументов и файлов на диске.
        """
        if not _valid_key(content_hash):
            self._invalid_key += 1
            self._misses += 1
            return None
        expected_version = _as_text(extractor_version)
        expected_level = _as_text(level)
        path = self._entry_path(content_hash)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            self._misses += 1
            return None
        except (OSError, UnicodeDecodeError):
            # Нечитаемый файл — не ошибка вызывающего, а испорченная запись.
            self._corrupt += 1
            self._misses += 1
            return None

        try:
            envelope = json.loads(raw)
        except ValueError:
            self._corrupt += 1
            self._misses += 1
            return None
        if not isinstance(envelope, dict):
            self._corrupt += 1
            self._misses += 1
            return None
        if envelope.get("format") != ENVELOPE_FORMAT:
            self._corrupt += 1
            self._misses += 1
            return None

        # Ключ конверта обязан совпасть с запросом; иначе — промах, не ошибка.
        if (
            envelope.get("content_hash") != content_hash
            or envelope.get("extractor_version") != expected_version
            or envelope.get("level") != expected_level
        ):
            self._misses += 1
            return None

        text = envelope.get("text")
        doc_hash = envelope.get("doc_content_hash")
        page_count = envelope.get("page_count")
        if not isinstance(text, str) or not text.strip():
            self._corrupt += 1
            self._misses += 1
            return None
        if not isinstance(doc_hash, str) or not doc_hash:
            self._corrupt += 1
            self._misses += 1
            return None
        if page_count is not None and (
            isinstance(page_count, bool)
            or not isinstance(page_count, int)
            or page_count < 0
        ):
            self._corrupt += 1
            self._misses += 1
            return None

        self._hits += 1
        return ExtractedDoc(
            text=text,
            content_hash=doc_hash,
            extractor_version=expected_version,
            page_count=page_count,
            error=None,
        )

    # ------------------------------------------------------------------ #
    # Запись
    # ------------------------------------------------------------------ #

    def put(
        self,
        doc: ExtractedDoc,
        *,
        level: str,
        content_hash: str | None = None,
    ) -> None:
        """Сохраняет успешное извлечение (атомарно, без блокировок).

        :param doc: результат экстрактора. Документы с ``error`` или пустым ``text``
            **не** сохраняются (см. модульный docstring: сбой парсера транзиентен, его
            заморозка отравила бы все будущие циклы).
        :param level: ось извлечения документа (``DocumentLevel`` или его строка).
        :param content_hash: ключ — SHA-256 **ретенированных байтов**, а не
            ``doc.content_hash`` (последний — хеш извлечённого текста и хранится в конверте
            как ``doc_content_hash``). Вызывающий обязан передать ключ сырья; по умолчанию
            берётся ``doc.content_hash`` — это осознанный, но почти всегда неверный для
            pipeline режим (он не даёт попаданий), оставленный только для совместимости
            сигнатуры и тестов.

        Невалидный ключ и сбой записи (нет прав, полный диск) — тихий no-op со счётчиком:
        кэш — оптимизация и не должен ронять прогон.
        """
        key = doc.content_hash if content_hash is None else content_hash
        if not _valid_key(key):
            self._invalid_key += 1
            return
        if getattr(doc, "error", None) is not None:
            return
        text = getattr(doc, "text", "")
        if not isinstance(text, str) or not text.strip():
            return
        doc_hash = getattr(doc, "content_hash", "")
        if not isinstance(doc_hash, str) or not doc_hash:
            return
        page_count = getattr(doc, "page_count", None)
        if page_count is not None and (
            isinstance(page_count, bool)
            or not isinstance(page_count, int)
            or page_count < 0
        ):
            page_count = None

        envelope = {
            "format": ENVELOPE_FORMAT,
            "content_hash": key,
            "extractor_version": _as_text(getattr(doc, "extractor_version", "") or ""),
            "level": _as_text(level),
            "text": text,
            "page_count": page_count,
            "doc_content_hash": doc_hash,
        }
        # Детерминированные байты: одинаковый вход → одинаковый файл (полезно для diff/аудита).
        payload = json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        path = self._entry_path(key)
        if self._write_atomic(path, payload, key) is None:
            return
        self._writes += 1
        self._evict_if_needed()

    def _write_atomic(self, path: Path, payload: str, key: str) -> Path | None:
        """Пишет ``payload`` во временный файл рядом и подменяет ``path`` атомарно.

        Временный файл — в **том же** каталоге (``os.replace`` атомарен только в пределах
        ФС), с суффиксом ``.tmp`` и скрытым префиксом: он никогда не имеет вид записи
        ``*.json`` и не будет обслужен читателем. Возвращает путь записи либо ``None``
        при сбое (temp убран).
        """
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, tmp_name = tempfile.mkstemp(
                dir=path.parent, prefix=f".{key}.", suffix=".tmp"
            )
        except OSError:
            return None
        tmp = Path(tmp_name)
        try:
            try:
                handle = os.fdopen(descriptor, "w", encoding="utf-8")
            except OSError:
                os.close(descriptor)
                raise
            with handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except OSError:
            try:
                tmp.unlink()
            except OSError:
                pass
            return None
        return path

    # ------------------------------------------------------------------ #
    # Вытеснение
    # ------------------------------------------------------------------ #

    def _evict_if_needed(self) -> None:
        """Удаляет самые старые по ``mtime`` записи, пока сумма не влезет в бюджет.

        Просто и детерминированно: полный обход ``*.json`` (индекса нет), сортировка по
        ``(mtime_ns, путь)`` — тай-брейк по пути делает результат воспроизводимым при
        одинаковых ``mtime``. Временные ``*.tmp`` не считаются и не удаляются (их уберёт
        писатель; читателю они безвредны).
        """
        budget = self._max_bytes
        if budget is None:
            return
        entries: list[tuple[int, str, int, Path]] = []
        total = 0
        try:
            for path in self._root.rglob("*.json"):
                try:
                    stat = path.stat()
                except OSError:
                    continue
                entries.append((stat.st_mtime_ns, str(path), stat.st_size, path))
                total += stat.st_size
        except OSError:
            return
        if total <= budget:
            return
        entries.sort(key=lambda row: (row[0], row[1]))
        for _mtime, _name, size, path in entries:
            if total <= budget:
                break
            try:
                path.unlink()
            except OSError:
                continue
            total -= size
            self._evicted += 1

    # ------------------------------------------------------------------ #
    # Наблюдаемость
    # ------------------------------------------------------------------ #

    def stats(self) -> dict[str, int]:
        """Счётчики экземпляра (детерминированный снимок, не влияет на решения).

        ``hits`` — обслуженные ``get``; ``misses`` — все промахи (включая битые файлы);
        ``writes`` — успешные ``put``; ``corrupt`` — битые/несовместимые конверты;
        ``evicted`` — вытесненные записи; ``invalid_key`` — отказы по невалидному ключу.
        """
        return {
            "hits": self._hits,
            "misses": self._misses,
            "writes": self._writes,
            "corrupt": self._corrupt,
            "evicted": self._evicted,
            "invalid_key": self._invalid_key,
        }

    # ------------------------------------------------------------------ #
    # Пути
    # ------------------------------------------------------------------ #

    def _entry_path(self, content_hash: str) -> Path:
        """Путь записи: ``{root}/{hash[:2]}/{hash}.json`` (шардирование по префиксу)."""
        return self._root / content_hash[:2] / f"{content_hash}.json"
