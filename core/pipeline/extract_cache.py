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
ретенированных байтов), ``extractor_version``, ``level``, ``text``, ``page_count``,
``family`` (семейство формата — вход выбора чанкера; поле аддитивное, в старых конвертах
отсутствует и читается как ``None`` = markdown-совместимое семейство),
``doc_content_hash`` — хеш **извлечённого текста** (поле ``ExtractedDoc.content_hash``) — и
аддитивные поля частичности ``partial``/``partial_reason``/``pages_converted`` (v5), а
также ``ocr_probe`` — исход проверки OCR-движка (поле аддитивное: конверт без него
читается как ``unknown`` — «не проверено», а не ``ok``; ``null`` — проверка не применима) —
и ``page_map`` (карта страниц: список троек ``[page_no, char_start, char_end]``; поле
аддитивное: конверт без него читается как ``None`` — «читатель карты не дал», а не
«страниц нет»; без него хит молча терял бы провенанс страниц у самых часто извлекаемых
документов), а также ``pages_ocr``/``pages_failed`` — исходы OCR по страницам (O38a;
поля аддитивные: конверт без них читается как ``None`` — «не измерено/не применимо», а
не ноль: ноль означал бы «распознавание применили и ни одна страница не дала текста»).
Два хеша различны по смыслу и оба обязательны: ключ адресует сырьё, ``doc_content_hash``
восстанавливает ровно тот ``ExtractedDoc``, который вернул экстрактор (по нему pipeline
делает дедуп и строит ``doc_id``). Несовпадение ``content_hash``/``extractor_version``/
``level`` с запросом — **промах**, а не ошибка: устаревший конверт не должен никогда
обслуживаться. Несовпадение версии формата и битый/нечитаемый JSON — тоже промах
(считается в :meth:`ExtractionCache.stats`), но не исключение.

**Ключ не может соврать о движке (Д-8).** ``extractor_version`` — не константа формата:
у документного извлечения это ``docling-doc-4:<level>/<device>/docling-<build>/<policy>``, где ``<build>`` —
версия установленной библиотеки (``core/adapters/extract_document.py``,
:func:`~core.adapters.extract_document.docling_extractor_version`). Поэтому апгрейд парсера
меняет ключ: конверт прежней сборки не находится, документ переизвлекается. Записи с
ключом старого формата (без сборки) намеренно не обслуживаются — обратная совместимость
не поддерживается.

**Кэшируются только полные успешные извлечения** (``error is None``, непустой ``text`` и
``partial is False``). Сбой парсера часто транзиентен или зависит от окружения (дефект OCR,
нехватка памяти, отсутствие системной библиотеки); закэшированный сбой заморозил бы плохой
результат на все будущие циклы, тогда как повторная попытка в другом окружении может дать
текст. Поэтому ошибки не сохраняются вовсе.

Частичный результат (``partial=True``: docling вернул ``PARTIAL_SUCCESS``, часть страниц не
сконвертирована) не кэшируется по той же причине, но она здесь острее: ключ —
``(content_hash байтов, extractor_version, level)`` — от объёма проделанной работы не
зависит, поэтому заморозка превратила бы «7 страниц из 2763» в документ навсегда, без
всякой возможности пере-экстракции.

**Частичность — свойство записи, а не вывода из текста (v5).** Конверт, помеченный
``partial: true``, обязан отдаваться частичным (``partial=True`` с причиной) и никогда —
полным: молчаливое повышение частичного наблюдения до полного и есть та подмена, ради
которой поле существует. Отсутствие поля читается как ``partial=False``, и это честно по
построению: **писатель этого модуля** частичный текст не сохраняет (предыдущий абзац),
поэтому запись без поля полная. Поле закрывает чужой/будущий случай — запись, сделанную
кодом, который частичность сохраняет.

**Где признак живёт durable.** Текст частичного извлечения здесь не хранится, поэтому
переживает прогоны сам признак — в per-product состоянии:
``document_version.extract_partial``/``extract_partial_reason``/
``extract_pages_converted``/``extract_page_count`` (``core/store/state_sqlite.py``, схема 5).
Переиспользование версии (``304``) — единственный путь, где признак нельзя пере-наблюдать,
и до v5 он там молча становился «полным»: усечённый текст объявлялся прочитанным целиком,
а ``CorpusCoverage`` — полным на нём (I2/I2c).

**I19.** Модуль сам не читает ``corpus_raw`` и не может проверить, что ключ соответствует
фактическим байтам: верификация — обязанность вызывающего (в pipeline ключ считается из
байтов, только что прочитанных из retained-файла, и хит принимается лишь при совпадении).
Кэш лишь гарантирует, что обслуживает конверт ровно под запрошенный ключ.

**Детерминизм.** :meth:`get` — чистая функция аргументов и состояния диска: ни сети, ни
LLM, ни часов. Счётчики :meth:`stats` — наблюдаемость экземпляра, на решение не влияют.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from core.ports.types import OCR_PROBE_UNKNOWN, OCR_PROBE_VALUES, ExtractedDoc

__all__ = ["ENVELOPE_FORMAT", "EVICT_CHECK_BYTES_DIVISOR", "ExtractionCache"]

#: Версия формата конверта. Входит в конверт: будущая ревизия схемы не будет обслужена
#: старым читателем (для него это промах), поэтому несовместимая запись безопасна.
ENVELOPE_FORMAT = 1

#: Допустимые символы ключа: полный hex (SHA-256). Заодно исключает ``/``, ``..`` и
#: прочие пути-ловушки: ключ — имя файла, а не произвольная строка.
_HASH_CHARS = frozenset("0123456789abcdefABCDEF")

#: Минимальная длина ключа: два символа нужны для шарда ``content_hash[:2]``.
_MIN_KEY_LEN = 2

#: Делитель бюджета для порога проверки вытеснения (1/16 бюджета записанных байтов).
#: Число — компромисс двух замеренных величин (15.11.2026, ``scripts/measure_extract_cache.py``):
#: полный обход 44 796 файлов стоит **0.293 с**, а одна запись без обхода — **0.002 с**.
#: При проверке на каждой записи прогон на 8 115 документов платит ≈39.6 мин за обходы;
#: при пороге 1/16 — ≈2.8 обхода на тот же прогон (≈0.8 с), ценой перебора бюджета не
#: более 1/16 (плюс одна запись) между проверками. Индекс вместо этого не берётся
#: намеренно: он потребовал бы согласованности между процессами-воркерами над одним
#: корнем, а скан остаётся единственным источником истины о занятом месте.
EVICT_CHECK_BYTES_DIVISOR = 16


def _as_text(value: Any) -> str:
    """Приводит ``StrEnum`` (``DocumentLevel``) к строке; прочее — ``str``."""
    return str(getattr(value, "value", value))


def _valid_key(content_hash: Any) -> bool:
    """Ключ пригоден как имя файла: непустой hex длиной ≥ 2 (без ``/``/``..``)."""
    if not isinstance(content_hash, str) or len(content_hash) < _MIN_KEY_LEN:
        return False
    return all(char in _HASH_CHARS for char in content_hash)


def _page_map_from_envelope(
    raw: object, text_length: int
) -> tuple[bool, tuple[tuple[int, int, int], ...] | None]:
    """Карта страниц из конверта: ``(форма_годна, карта)``.

    ``None`` (или отсутствие ключа) — законное «читатель карты не дал»: ``(True, None)``,
    и это **не** «страниц нет». Всё прочее обязано быть списком троек целых, идущих по
    возрастанию номера страницы, не пересекающихся и лежащих внутри текста; иначе
    ``(False, None)`` и запись считается порчей: номер страницы по такой карте был бы
    догадкой, а не провенансом.
    """
    if raw is None:
        return True, None
    if not isinstance(raw, list):
        return False, None
    entries: list[tuple[int, int, int]] = []
    previous_page = 0
    previous_end = 0
    for item in raw:
        if not isinstance(item, list) or len(item) != 3:
            return False, None
        if any(isinstance(value, bool) or not isinstance(value, int) for value in item):
            return False, None
        page_no, start, end = item
        if page_no < 1 or start < 0 or end < start or end > text_length:
            return False, None
        if page_no <= previous_page or start < previous_end:
            return False, None
        previous_page, previous_end = page_no, end
        entries.append((page_no, start, end))
    return True, tuple(entries)


class ExtractionCache:
    """Дисковый кэш результатов экстракции (см. модульный docstring).

    :param root: корень кэша; конструктор **не** создаёт каталог и не делает I/O.
    :param max_bytes: бюджет размера (сумма ``*.json`` под ``root``); ``None`` — без
        вытеснения. Самые старые по ``mtime`` записи удаляются, пока сумма не вернётся в
        бюджет. Индекса нет: каталог сканируется только в момент проверки, а проверка
        **амортизирована по записанным байтам** — порог :data:`EVICT_CHECK_BYTES_DIVISOR`
        (1/16 бюджета). Причина названа числом: полный обход 44 796 файлов стоит 0.293 с
        (замер 15.11.2026, ``scripts/measure_extract_cache.py``), то есть при проверке на
        каждой записи один прогон на 8 115 документов платит 39.6 мин только за обход.
        Цена амортизации тоже названа: между проверками сумма может превышать бюджет на
        порог (плюс одну запись), а при ``N`` процессах над одним ``root`` — до
        ``N × порог``, потому что счётчик записанных байтов живёт в процессе. Запись,
        которая одна больше бюджета, проверяется немедленно (кэш остаётся в бюджете,
        пусть и пустым).

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
        #: Обслуженные записи, несущие частичность (v5): хит — не «полное извлечение».
        self._partial_hits = 0
        #: Полные обходы каталога ради вытеснения (амортизированы, см. модуль).
        self._scans = 0
        #: Записано байтов с последней проверки бюджета (порог — 1/16 бюджета).
        self._pending_bytes = 0

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

        Запись, несущая частичность, возвращается **частичной** (``partial=True`` с
        причиной и числом прочитанных страниц); отдать её полной нельзя — это и было бы
        тихой подменой наблюдения. Противоречивая запись (причина без признака, признак
        без причины, прочитано больше, чем заявлено) — порча, то есть промах.

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
        # ``family`` — аддитивное поле: конверты до появления текстового чанкера его не
        # несут (и это корректно — тогда текстовые форматы вообще не кэшировались, они
        # падали в ``parser_failed``), поэтому ``None`` = markdown-совместимое семейство.
        family = envelope.get("family")
        if family is not None and not isinstance(family, str):
            self._corrupt += 1
            self._misses += 1
            return None
        # Частичность — свойство записи (v5): отсутствие поля = полное (писатель этого
        # модуля частичный текст не сохраняет), наличие ``true`` = запись обязана
        # отдаваться частичной. Обратное («есть смысл в partial_reason, но нет признака»)
        # — противоречивая запись, то есть порча, а не полное извлечение.
        partial_raw = envelope.get("partial")
        if partial_raw is not None and not isinstance(partial_raw, bool):
            self._corrupt += 1
            self._misses += 1
            return None
        partial = bool(partial_raw)
        partial_reason = envelope.get("partial_reason")
        pages_converted = envelope.get("pages_converted")
        if partial_reason is not None and not isinstance(partial_reason, str):
            self._corrupt += 1
            self._misses += 1
            return None
        if pages_converted is not None and (
            isinstance(pages_converted, bool)
            or not isinstance(pages_converted, int)
            or pages_converted < 0
        ):
            self._corrupt += 1
            self._misses += 1
            return None
        if not partial and (
            partial_reason
            or pages_converted is not None
            # Причина частичности без самой частичности — ложная провенанс-запись.
        ):
            self._corrupt += 1
            self._misses += 1
            return None
        if partial and not (partial_reason or "").strip():
            # «Частичный» без причины не обслуживается: молчание о том, что именно
            # прочитано, нельзя выдать ни за полное, ни за объяснимое частичное.
            self._corrupt += 1
            self._misses += 1
            return None
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
        if (
            partial
            and pages_converted is not None
            and page_count is not None
            and pages_converted > page_count
        ):
            self._corrupt += 1
            self._misses += 1
            return None

        # Провенанс OCR-пробы — аддитивное поле. Конверт без него писался до появления
        # проверки, поэтому читается как ``unknown`` («не проверено»), а **не** как ``ok``:
        # отсутствие факта не является фактом «проверено и работает». Явный ``null`` в
        # конверте означает «не применимо» и остаётся ``None``.
        ocr_probe = envelope.get("ocr_probe", OCR_PROBE_UNKNOWN)
        if ocr_probe is not None and (
            not isinstance(ocr_probe, str) or ocr_probe not in OCR_PROBE_VALUES
        ):
            self._corrupt += 1
            self._misses += 1
            return None

        # Карта страниц — аддитивное поле: конверт без него читается как ``None``
        # («читатель карты не дал»), и это **не** «страниц нет». Порченая форма (не
        # список троек, номер < 1, вывернутый интервал, выход за текст, пересечение)
        # — порча записи: по такой карте номер страницы был бы догадкой.
        page_map_ok, page_map = _page_map_from_envelope(
            envelope.get("page_map"), len(text)
        )
        if not page_map_ok:
            self._corrupt += 1
            self._misses += 1
            return None

        # Исходы OCR по страницам (O38a) — аддитивные поля: конверт без них читается как
        # ``None`` («не измерено/не применимо»), а **не** как ноль: ноль означал бы
        # «OCR применили и ни одна страница не дала текста» — другое наблюдение.
        pages_ocr = envelope.get("pages_ocr")
        pages_failed = envelope.get("pages_failed")
        for value in (pages_ocr, pages_failed):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                self._corrupt += 1
                self._misses += 1
                return None
        if page_count is not None:
            for value in (pages_ocr, pages_failed):
                if value is not None and value > page_count:
                    self._corrupt += 1
                    self._misses += 1
                    return None

        self._hits += 1
        if partial:
            self._partial_hits += 1
        return ExtractedDoc(
            text=text,
            content_hash=doc_hash,
            extractor_version=expected_version,
            page_count=page_count,
            error=None,
            family=family,
            partial=partial,
            partial_reason=partial_reason if partial else None,
            pages_converted=pages_converted if partial else None,
            ocr_probe=ocr_probe,
            page_map=page_map,
            pages_ocr=pages_ocr,
            pages_failed=pages_failed,
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
        # P8: частичный результат **не** кэшируется. Причина не косметическая: ключ —
        # ``(content_hash байтов, extractor_version, level)``, то есть от объёма реально
        # проделанной работы он не зависит. Живой случай: PDF на 2763 страницы дал
        # PARTIAL_SUCCESS с 7 прочитанными — заморозить это значило бы навсегда
        # объявить документом «7 страниц из 2763», и пере-экстракция не случилась бы
        # никогда. Частичность транзиентна (OCR, таймаут, память), поэтому кэшируется
        # только полный результат — по той же причине, что и сбои.
        #
        # Признак при этом **не теряется** (v5): он пишется в durable-запись версии
        # документа (``document_version.extract_partial``), откуда его восстанавливает
        # переиспользование (``304``) — единственный путь, где признак нельзя
        # пере-наблюдать. Конверт тоже умеет его нести (``get``), чтобы запись, сделанная
        # писателем, который частичность сохраняет, не была выдана за полную.
        if getattr(doc, "partial", False):
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
            "family": getattr(doc, "family", None),
            "doc_content_hash": doc_hash,
            # Аддитивное поле: конверты без него читаются как ``unknown``, поэтому бамп
            # ``ENVELOPE_FORMAT`` не нужен — старый конверт честно говорит «не проверено».
            "ocr_probe": getattr(doc, "ocr_probe", None),
            # Карта страниц — тоже аддитивное поле (JSON-формы: список троек). Конверт без
            # неё читается как ``None`` («читатель карту не дал»), а не как «страниц нет».
            # Без этого поля **хит** отдавал бы документ без провенанса страниц — и именно
            # тем документам, которые извлекались чаще всех.
            "page_map": getattr(doc, "page_map", None),
            # Исходы OCR по страницам (O38a) — аддитивные поля, бамп ``ENVELOPE_FORMAT`` не
            # нужен: конверт без них читается как ``None`` («не измерено»), а не как ноль.
            # Без них хит терял бы единственный постраничный признак того, что распознавание
            # вообще применялось, — у самых часто извлекаемых документов.
            "pages_ocr": getattr(doc, "pages_ocr", None),
            "pages_failed": getattr(doc, "pages_failed", None),
        }
        # Детерминированные байты: одинаковый вход → одинаковый файл (полезно для diff/аудита).
        payload = json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        path = self._entry_path(key)
        written = self._write_atomic(path, payload, key)
        if written is None:
            return
        self._writes += 1
        try:
            size = written.stat().st_size
        except OSError:
            size = 0
        self._evict_if_needed(size)

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
            # Уборка временного файла — best-effort и молча: исходная ошибка записи уже
            # видна вызывающему (``None`` = «не закэшировано»), второй отказ (файл снят
            # другим процессом / нет прав) этого факта не меняет и не должен подменять.
            with contextlib.suppress(OSError):
                tmp.unlink()
            return None
        return path

    # ------------------------------------------------------------------ #
    # Вытеснение
    # ------------------------------------------------------------------ #

    def _evict_if_needed(self, entry_bytes: int = 0) -> None:
        """Удаляет самые старые по ``mtime`` записи, пока сумма не влезет в бюджет.

        Просто и детерминированно: полный обход ``*.json`` (индекса нет), сортировка по
        ``(mtime_ns, путь)`` — тай-брейк по пути делает результат воспроизводимым при
        одинаковых ``mtime``. Временные ``*.tmp`` не считаются и не удаляются (их уберёт
        писатель; читателю они безвредны).

        **Проверка амортизирована по записанным байтам** (:data:`EVICT_CHECK_BYTES_DIVISOR`):
        обход — самая дорогая часть (0.293 с при 44 796 файлах), и делать его на каждой
        записи значило бы платить временем прогона за точность, которая бюджету не нужна.
        Немедленная проверка — только когда сама запись больше бюджета: иначе «одна
        запись больше бюджета» осталась бы в кэше до следующего порога.
        """
        budget = self._max_bytes
        if budget is None:
            return
        self._pending_bytes += max(0, entry_bytes)
        threshold = max(1, budget // EVICT_CHECK_BYTES_DIVISOR)
        if self._pending_bytes < threshold and entry_bytes <= budget:
            return
        self._pending_bytes = 0
        self._scans += 1
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
        ``evicted`` — вытесненные записи; ``invalid_key`` — отказы по невалидному ключу;
        ``partial_hits`` — хиты, отдавшие **частичное** наблюдение (подмножество ``hits``),
        то есть «переиспользовано, но не как полное»; ``scans`` — полные обходы каталога
        ради вытеснения (амортизированы по :data:`EVICT_CHECK_BYTES_DIVISOR`, поэтому
        ``scans`` много меньше ``writes`` — и это измеряемая цена бюджета).
        """
        return {
            "hits": self._hits,
            "misses": self._misses,
            "writes": self._writes,
            "corrupt": self._corrupt,
            "evicted": self._evicted,
            "invalid_key": self._invalid_key,
            "partial_hits": self._partial_hits,
            "scans": self._scans,
        }

    # ------------------------------------------------------------------ #
    # Пути
    # ------------------------------------------------------------------ #

    def _entry_path(self, content_hash: str) -> Path:
        """Путь записи: ``{root}/{hash[:2]}/{hash}.json`` (шардирование по префиксу)."""
        return self._root / content_hash[:2] / f"{content_hash}.json"
