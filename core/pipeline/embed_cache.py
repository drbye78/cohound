"""Контент-адресуемый кэш векторов эмбеддинга, переживающий прогоны (Ш5; A3).

**Проблема, которую модуль закрывает.** Переиспользование индекса сегодня возможно только
**целой продукт-партицией**: ``partition_build_id`` (``pb``) хеширует ``chunker_version``,
``embed_model_id``, ``extractor_version`` и отпечаток содержимого продукта
(``core/pipeline/corpus_index.py``,
:func:`~core.pipeline.corpus_index._derive_partition_build_id`).
Поэтому смена экстрактора (например, другого механизма извлечения PDF), добавление одного
документа или смена версии чанкера дают новый ``pb``, гейт переиспользования не проходит — и
эмбеддинг пересчитывается **для всего продукта**. Замеренная цена эмбеддинга — десятки
чанков в минуту, то есть на корпусе в десятки тысяч чанков это часы работы, потраченные на
повторный расчёт уже посчитанных векторов. Этот модуль экономит собственно эмбеддинг:
вектор кладётся на диск под ключом :func:`embed_cache_key` и переиспользуется следующим
прогоном **почанково**.

**Ключ — ``(content_hash текста чанка, embed_model_id, embed_device)`` и только он.** В ключ
намеренно **не** входят ``extractor_version``/``chunker_version``/``chunk_id``/``product_id``:
вектор — функция текста, модели и места счёта, а не истории происхождения этого текста. Если
текст чанка совпал байт-в-байт (тот же ``content_hash``), то и вектор совпадает, каким бы
экстрактором текст ни был получен и в каком бы продукте/документе он ни лежал. Именно это
свойство делает смену механизма извлечения дешёвой: текст, который старый и новый
экстракторы дали одинаково, обслуживается из кэша; пересчитывается только реально
изменившийся текст. Обратное включение версий производства в ключ вернуло бы ровно ту
проблему, ради которой модуль написан (новый ``pb`` ⇒ всё заново), и было бы ложью о
зависимости: вектор от версии чанкера не зависит. ``embed_model_id`` в ключе — наоборот
обязателен: векторы разных моделей несравнимы и не взаимозаменяемы (смена модели
по-прежнему даёт полный пересчёт — это измерение, а не издержка).

**``embed_device`` в ключе — потому что это не провенанс, а значение.** Векторы CPU и CUDA
одной модели **численно разные**: это разные числа, а не разные сведения о том, как получены
одни и те же числа. Без устройства в ключе прогон, объявивший ``cuda``, молча получил бы
``cpu``-векторы из кэша (и наоборот), то есть ось ``embed_device`` перестала бы действовать
ровно там, где она дороже всего. Поэтому устройство — обязательный аргумент, а не догадка
читателя: старые записи кэша созданы без объявленного устройства, приписать им ``cuda``
значило бы выдать догадку за измерение, поэтому они — **промах** (первый прогон после
появления оси пере-эмбеддит корпус; цена названа в 16 §5.3).

**Раскладка.** ``{root}/{model_dir}/{embed_device}/{content_hash[:2]}/{content_hash}.vec`` —
без центрального индекса и без блокировок: путь выводится из ключа, поэтому разные процессы
пишут в разные файлы, а совпадающие ключи дают идентичное содержимое. Каталог модели —
:func:`model_dir_name` (безопасный префикс + короткий хеш полного ``embed_model_id``):
идентификатор модели может содержать ``/`` и другие символы пути, а хеш исключает коллизию
после санитизации. Сегмент устройства разводит физически то, что различается в ключе: общий
каталог заставлял бы ``cpu``- и ``cuda``-прогоны перезаписывать записи друг друга, то есть
каждый второй прогон платил бы полный эмбеддинг — цена, которую кэш и должен снимать.
Запись атомарна (временный файл в том же каталоге → :func:`os.replace`), поэтому убитый
процесс оставляет либо старый, либо новый файл, но не половину.

**Формат — fp16, а не JSON-числа.** Вектор в индексе и так живёт как ``float16``
(``vectors.fp16.npy``, :mod:`core.adapters.index_npy`), поэтому хранить его в JSON значило бы
платить ~10–20 байт на компоненту вместо двух и терять биты при обратном разборе.
Конверт — одна строка JSON (заголовок) + сырой payload:

```
{"byteorder":"little","content_hash":"…","dim":1024,"dtype":"float16",
 "embed_device":"cpu","embed_model_id":"…","format":1}\n<payload: dim × 2 байта, little-endian half>
```

Заголовок человекочитаем (аудит/диагностика), payload — 2 байта на ось (1024 оси = 2 KB на
чанк). ``byteorder`` фиксирован ``little``: файл переносим между архитектурами, порядок байт
не зависит от хоста. **Квантизация не меняет сборку индекса:** сборщик сам переводит вектор
в ``float16``, а half → float64 → half — тождественное преобразование, поэтому матрица
индекса побитово та же, что при живом эмбеддинге. Это и есть причина хранить fp16, а не
float32/float64: кэш не имеет права менять измеряемое содержимое партиции.

**Промах — не ошибка.** Отсутствие файла, битый/нечитаемый заголовок, несовпадение версии
формата, ``content_hash``/``embed_model_id``/``embed_device``/``dim`` с запросом, неверная
длина payload, нечисловые (``NaN``/``inf``) компоненты — всё это **промах** (считается в
:meth:`EmbedCache.stats`), а не исключение: устаревшая или повреждённая запись не должна
ломать прогон, но и не должна обслуживаться. Проверка ``dim`` защищает от подмены файла
вектором другой размерности (такой хит дал бы матрицу неверной формы и упал бы уже в
сборщике), проверка ``embed_device`` — от подмены/переноса записи между устройствами.

**Бюджет диска.** ``max_bytes`` — суммарный размер ``*.vec`` под ``root``; когда объём
превышает бюджет, самые старые по ``mtime`` записи удаляются, пока сумма не вернётся в
бюджет. Это управление **диском**, а не потолок работы: число чанков/документов не
ограничивается ничем (измеряй, а не запрещай). Бюджет видим (:attr:`EmbedCache.max_bytes`),
вытеснения и число обходов каталога считаются (:meth:`EmbedCache.stats`). Обход каталога
оплачивается **не** на каждую запись, а по счётчику объёма процесса: на кэше в десятки тысяч
чанков наивный ``rglob`` после каждого ``put`` стоил бы O(n²) ``stat`` — дороже эмбеддинга,
ради экономии которого кэш существует.

**Детерминизм (A3).** :meth:`EmbedCache.get` — чистая функция аргументов и состояния диска:
ни сети, ни LLM, ни часов. Кэш не решает, *что* эмбеддить, и не меняет порядок векторов:
вызывающий (``_embed_batched``) подставляет хиты в те же позиции, в которых они стояли в
батче, поэтому порядок входа = порядок выхода и при смешанном батче.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import re
import struct
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.util.hashing import short_hash

__all__ = [
    "ENVELOPE_FORMAT",
    "KEY_SEP",
    "SUFFIX",
    "EmbedCache",
    "embed_cache_key",
    "model_dir_name",
]

#: Версия формата конверта. Входит в заголовок: будущая ревизия схемы не будет обслужена
#: старым читателем (для него это промах), поэтому несовместимая запись безопасна.
ENVELOPE_FORMAT = 1

#: Расширение файла записи. Не ``.json``: payload бинарный (fp16), и расширение честно
#: говорит, что читать это как JSON нельзя.
SUFFIX = ".vec"

#: Единственный поддерживаемый тип компонента. Поле в заголовке — не декорация: оно
#: позволяет отвергнуть запись, если формат компонента когда-нибудь изменится.
_DTYPE = "float16"

#: Порядок байт payload в файле (независимо от хоста).
_BYTEORDER = "little"

#: Разделитель полей ключа (unit separator) — как в ``core/util/hashing``: однозначная
#: склейка, не встречающаяся в hex-хешах и идентификаторах моделей.
KEY_SEP = "\x1f"

#: Допустимые символы ``content_hash``: полный hex (SHA-256). Заодно исключает ``/``,
#: ``..`` и прочие пути-ловушки: ключ — имя файла, а не произвольная строка.
_HASH_CHARS = frozenset("0123456789abcdefABCDEF")

#: Устройство счёта — короткая метка, годная как сегмент пути (``cpu``/``cuda``).
#: Шаблон, а не перечень значений: перечень — доменное знание (``EmbedDevice``), а модуль
#: обязан оставаться категорийно-нейтральным и не знать о нём.
_DEVICE_RE = re.compile(r"[A-Za-z0-9._:-]{1,32}")

#: Минимальная длина ключа: два символа нужны для шарда ``content_hash[:2]``.
_MIN_KEY_LEN = 2

#: Предел безопасного префикса каталога модели (длинные id моделей не должны давать
#: путь в сотни символов); уникальность держит хеш, а не префикс.
_MODEL_DIR_MAX = 48


def embed_cache_key(content_hash: str, embed_model_id: str, embed_device: str) -> str:
    """Ключ записи кэша: ``content_hash`` текста чанка + модель + устройство счёта.

    Все три компоненты обязательны. Возвращается склейка через :data:`KEY_SEP` — она же
    используется как материал для диагностики. В ключ **не** входят версии
    экстрактора/чанкера: вектор зависит от текста, модели и места счёта, а не от того,
    каким движком текст получен (см. модульный docstring). Устройство входит потому, что
    CPU- и CUDA-векторы одной модели — разные числа, а не разные сведения о них.
    """
    return f"{content_hash}{KEY_SEP}{embed_model_id}{KEY_SEP}{embed_device}"


def model_dir_name(embed_model_id: str) -> str:
    """Имя каталога модели под ``root``: безопасный префикс + хеш полного ``embed_model_id``.

    Идентификатор модели — внешняя строка (может содержать ``/``, ``:``, пробелы), поэтому
    он санитизируется до ``[A-Za-z0-9._-]`` и обрезается; хеш полного значения снимает
    риск коллизии двух разных id, дающих одинаковый префикс. Имя детерминировано: один
    ``embed_model_id`` — один каталог, две модели не смешиваются физически.
    """
    text = str(embed_model_id or "")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")[:_MODEL_DIR_MAX]
    return f"{safe or 'model'}-{short_hash(text, 12)}"


def _valid_key(content_hash: Any) -> bool:
    """Ключ пригоден как имя файла: непустой hex длиной ≥ 2 (без ``/``/``..``)."""
    if not isinstance(content_hash, str) or len(content_hash) < _MIN_KEY_LEN:
        return False
    return all(char in _HASH_CHARS for char in content_hash)


def _valid_device(embed_device: Any) -> bool:
    """Устройство пригодно как компонент ключа: короткая метка, а не символы пути."""
    if not isinstance(embed_device, str) or _DEVICE_RE.fullmatch(embed_device) is None:
        return False
    return embed_device not in {".", ".."}


def _half(value: Any) -> float:
    """Приводит компонент к ``float``; нечисловое значение → ``ValueError``."""
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("нечисловой компонент вектора")
    return number


def _decode(raw: bytes) -> tuple[dict[str, Any], list[float]] | None:
    """Разбирает конверт; ``None`` — запись непригодна (промах, не исключение).

    Проверяется всё, на чём может «соврать» подменённый/испорченный файл: наличие
    разделителя заголовка, версия формата, тип/порядок байт компонента, положительная
    ``dim``, строковые поля ключа, точная длина payload и конечность компонентов.
    ``embed_device`` здесь **не** обязателен: запись без него — не порча, а старая
    запись без объявленного устройства, и она обязана быть промахом в :meth:`EmbedCache.get`
    (догадка о её устройстве запрещена), а не ошибкой чтения.
    """
    head, separator, payload = raw.partition(b"\n")
    if not separator:
        return None
    try:
        envelope = json.loads(head.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(envelope, dict):
        return None
    if envelope.get("format") != ENVELOPE_FORMAT:
        return None
    if envelope.get("dtype") != _DTYPE or envelope.get("byteorder") != _BYTEORDER:
        return None
    dim = envelope.get("dim")
    if isinstance(dim, bool) or not isinstance(dim, int) or dim < 1:
        return None
    if not isinstance(envelope.get("content_hash"), str):
        return None
    if not isinstance(envelope.get("embed_model_id"), str):
        return None
    if len(payload) != dim * 2:
        return None
    try:
        # Полуточность: ``struct`` поддерживает формат ``e`` (IEEE 754 binary16),
        # little-endian — независимо от хоста.
        vector = list(struct.unpack(f"<{dim}e", payload))
    except struct.error:
        return None
    if not all(math.isfinite(component) for component in vector):
        return None
    return envelope, vector


class EmbedCache:
    """Дисковый кэш векторов эмбеддинга (см. модульный docstring).

    :param root: корень кэша; конструктор **не** создаёт каталог и не делает I/O.
    :param max_bytes: бюджет размера (сумма ``*.vec`` под ``root``); ``None`` — без
        вытеснения. Бюджет — управление диском, а не потолок работы: число записей
        ограничено только объёмом.

    Экземпляры над одним ``root`` эквивалентны: состояние — только файлы на диске.
    Экземпляр не потокобезопасен по счётчикам (они — наблюдаемость), но файловые
    операции безопасны: запись атомарна, чтение не блокирует писателя.
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
        self._scans = 0
        # Учёт занятого объёма (только при бюджете): ``None`` — ещё не измерен.
        # Счётчик процесса, а не ФС: обход стоит O(n), поэтому он выполняется один раз
        # на старте и далее только при превышении бюджета (см. ``_evict_if_needed``).
        self._known_total: int | None = None

    # ------------------------------------------------------------------ #
    # Свойства
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
        self,
        content_hash: str,
        embed_model_id: str,
        *,
        embed_device: str,
        dim: int | None = None,
    ) -> list[float] | None:
        """Вектор из кэша или ``None`` (промах).

        Промах — отсутствие файла, битая/несовместимая запись, несовпадение любого
        элемента ключа (``content_hash``/``embed_model_id``/``embed_device``) и
        несовпадение ``dim`` (вектор другой размерности несовместим с текущей моделью).
        Ни один случай не бросает исключение.

        :param content_hash: ``content_hash`` **текста** чанка (``CorpusChunk.content_hash``).
        :param embed_model_id: идентификатор модели, посчитавшей вектор (``EmbedderPort.model_id``).
        :param embed_device: устройство, на котором посчитан вектор (объявленная ось прогона).
            Обязателен и только по имени: устройство — часть значения, а не удобный дефолт.
            Запись, созданная без объявленного устройства (старый кэш), — **промах**:
            приписать ей ``cuda`` было бы догадкой.
        :param dim: ожидаемая размерность; ``None`` — не проверять.

        :returns: список ``dim`` чисел (значения — ровно те, что лежат в fp16-записи) либо
            ``None``. Квантизация half → float64 точна, поэтому сборка индекса из хита
            побитово равна сборке из живого вектора.
        """
        model_id = str(embed_model_id or "")
        if (
            not _valid_key(content_hash)
            or not model_id
            or not _valid_device(embed_device)
        ):
            self._invalid_key += 1
            self._misses += 1
            return None
        path = self._entry_path(content_hash, model_id, embed_device)
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            self._misses += 1
            return None
        except OSError:
            # Нечитаемый файл — не ошибка вызывающего, а испорченная запись.
            self._corrupt += 1
            self._misses += 1
            return None

        decoded = _decode(raw)
        if decoded is None:
            self._corrupt += 1
            self._misses += 1
            return None
        envelope, vector = decoded
        # Ключ конверта обязан совпасть с запросом; иначе — промах, не ошибка.
        if (
            envelope.get("content_hash") != content_hash
            or envelope.get("embed_model_id") != model_id
            or envelope.get("embed_device") != embed_device
        ):
            self._misses += 1
            return None
        if dim is not None and envelope.get("dim") != dim:
            self._corrupt += 1
            self._misses += 1
            return None

        self._hits += 1
        return vector

    # ------------------------------------------------------------------ #
    # Запись
    # ------------------------------------------------------------------ #

    def put(
        self,
        content_hash: str,
        embed_model_id: str,
        vector: Sequence[float],
        *,
        embed_device: str,
        dim: int | None = None,
    ) -> None:
        """Сохраняет вектор (атомарно, без блокировок).

        Невалидный ключ (в т.ч. необъявленное устройство), нечисловой/неконечный компонент,
        несовпадение ``dim`` и сбой записи (нет прав, полный диск, переполнение half) —
        тихий no-op со счётчиком: кэш — оптимизация и не должен ронять прогон, а
        «не закэшировали» дешевле, чем «сохранили ложь».

        :param embed_device: устройство, на котором вектор посчитан; пишется в заголовок
            конверта, поэтому читатель другой оси получит промах, а не чужой вектор.
        :param dim: ожидаемая размерность; ``None`` → длина ``vector``.
        """
        model_id = str(embed_model_id or "")
        if (
            not _valid_key(content_hash)
            or not model_id
            or not _valid_device(embed_device)
        ):
            self._invalid_key += 1
            return
        try:
            components = [_half(component) for component in vector]
        except (TypeError, ValueError):
            return
        expected_dim = len(components) if dim is None else dim
        if isinstance(expected_dim, bool) or not isinstance(expected_dim, int):
            return
        if expected_dim < 1 or len(components) != expected_dim:
            return
        try:
            body = struct.pack(f"<{expected_dim}e", *components)
        except (struct.error, OverflowError, ValueError):
            # Компонент вне диапазона half: запись отравила бы чтение (inf), поэтому
            # такой вектор не кэшируется вовсе.
            return

        envelope = {
            "format": ENVELOPE_FORMAT,
            "dtype": _DTYPE,
            "byteorder": _BYTEORDER,
            "dim": expected_dim,
            "content_hash": content_hash,
            "embed_model_id": model_id,
            "embed_device": embed_device,
        }
        # Детерминированные байты: одинаковый вход → одинаковый файл (diff/аудит).
        header = json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        path = self._entry_path(content_hash, model_id, embed_device)
        payload = header + b"\n" + body
        if self._write_atomic(path, payload, content_hash) is None:
            return
        self._writes += 1
        if self._known_total is not None:
            # Учёт этого процесса: перезапись того же ключа считается дважды (размер
            # записи не зависит от значения), поэтому оценка — сверху, и первый же
            # обход её исправляет.
            self._known_total += len(payload)
        self._evict_if_needed()

    def _write_atomic(self, path: Path, payload: bytes, key: str) -> Path | None:
        """Пишет ``payload`` во временный файл рядом и подменяет ``path`` атомарно.

        Временный файл — в **том же** каталоге (``os.replace`` атомарен только в пределах
        ФС), с суффиксом ``.tmp`` и скрытым префиксом: он никогда не имеет вид записи
        (``.vec``) и не будет обслужен читателем. Возвращает путь записи либо ``None``
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
                handle = os.fdopen(descriptor, "wb")
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

    def _evict_if_needed(self) -> None:
        """Удаляет самые старые по ``mtime`` записи, пока сумма не влезет в бюджет.

        **Обход — не на каждую запись.** Наивная версия («после каждого ``put`` обойти
        каталог») на кэше в десятки тысяч чанков стоила бы O(n²) ``stat`` — дороже самого
        эмбеддинга. Поэтому объём ведётся счётчиком процесса (:attr:`_known_total`): он
        растёт на размер каждой записи, а полный обход выполняется только когда счётчик
        превысил бюджет (и один раз — при первой записи, чтобы узнать базу). Так бюджет
        остаётся бюджетом, а цена его соблюдения — один обход на «бюджет записей».

        Учёт — в пределах экземпляра: записи чужого процесса счётчик не видит, поэтому
        бюджет может быть превышен до следующего обхода. Это мягкая граница управления
        диском, а не инвариант корректности: превышение видно в :meth:`stats`
        (``scans``/``evicted``), а испорченная или лишняя запись обслуживанию не мешает.

        Сортировка по ``(mtime_ns, путь)``: тай-брейк по пути делает результат
        воспроизводимым при одинаковых ``mtime``. Временные ``*.tmp`` не считаются и не
        удаляются.
        """
        budget = self._max_bytes
        if budget is None:
            return
        if self._known_total is not None and self._known_total <= budget:
            return
        scanned = self._scan()
        if scanned is None:
            return
        total, entries = scanned
        if total <= budget:
            self._known_total = total
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
        self._known_total = total

    def _scan(self) -> tuple[int, list[tuple[int, str, int, Path]]] | None:
        """Обходит ``*.vec`` под ``root``: сумма размеров + записи для вытеснения.

        ``None`` — каталог нечитаем (кэш не должен ронять прогон из-за прав/сбоя ФС).
        """
        self._scans += 1
        entries: list[tuple[int, str, int, Path]] = []
        total = 0
        try:
            for path in self._root.rglob(f"*{SUFFIX}"):
                try:
                    stat = path.stat()
                except OSError:
                    continue
                entries.append((stat.st_mtime_ns, str(path), stat.st_size, path))
                total += stat.st_size
        except OSError:
            return None
        return total, entries

    # ------------------------------------------------------------------ #
    # Наблюдаемость
    # ------------------------------------------------------------------ #

    def stats(self) -> dict[str, int]:
        """Счётчики экземпляра (детерминированный снимок, не влияет на решения).

        ``hits`` — обслуженные ``get``; ``misses`` — все промахи (включая битые файлы);
        ``writes`` — успешные ``put``; ``corrupt`` — битые/несовместимые записи;
        ``evicted`` — вытесненные записи; ``invalid_key`` — отказы по невалидному ключу;
        ``scans`` — сколько раз каталог обходился ради бюджета (цена соблюдения бюджета).
        """
        return {
            "hits": self._hits,
            "misses": self._misses,
            "writes": self._writes,
            "corrupt": self._corrupt,
            "evicted": self._evicted,
            "invalid_key": self._invalid_key,
            "scans": self._scans,
        }

    # ------------------------------------------------------------------ #
    # Пути
    # ------------------------------------------------------------------ #

    def _entry_path(
        self, content_hash: str, embed_model_id: str, embed_device: str
    ) -> Path:
        """Путь записи: ``{root}/{каталог модели}/{устройство}/{hash[:2]}/{hash}.vec``.

        Каталог модели разделяет физически: векторы разных моделей несравнимы, и общий
        каталог заставлял бы читателя проверять модель, чтобы не обслужить чужой вектор.
        Сегмент устройства делает то же для оси ``embed_device``: векторы CPU и CUDA —
        разные числа под одним ключом, и общий путь означал бы, что записи двух осей
        перезаписывают друг друга (каждый второй прогон платил бы полный эмбеддинг).
        """
        return (
            self._root
            / model_dir_name(embed_model_id)
            / embed_device
            / content_hash[:2]
            / f"{content_hash}{SUFFIX}"
        )
