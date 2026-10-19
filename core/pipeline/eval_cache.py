"""Межпрогонный кэш вердиктов оценщика Ш6 (I18/I19; производительность цикла).

Недельный цикл заново спрашивает оценщика о каждой ячейке ``(product × param)``, даже
если доказательная база ячейки не изменилась. При целевом масштабе это десятки тысяч
LLM-вызовов на цикл. Этот модуль переиспользует **сырой** ``LLMCellOutput``, если
отрендеренный промпт и конфигурация оценщика совпали байт-в-байт.

**Кэшируется ``LLMCellOutput``, а не ``Cell``.** ``Cell`` — результат цепочки
«выход LLM → :func:`validate_output` → синтез absence-evidence → гейт покрытия корпуса
(Ф0) → наложение прецедента → персистенция». Пост-шаги зависят от состояния, которого
нет в промпте (покрытие корпуса, защищённая ячейка высокого источника, ``run_id`` в
``derived_evidence_id``). Поэтому кэш отдаёт ровно то, что вернул бы оценщик, а все
существующие пост-шаги выполняются заново на каждом прогоне — хит неотличим от живого
вызова, кроме отсутствия сетевого вызова.

**Ключ.** ``prompt_hash`` (§5) для этого **недостаточен**: он не включает ``product_id``,
заголовок параметра, сателлиты, ``extra_instructions``, текст чанков и содержимое
шаблона. Ключ строится вызывающим из **отрендеренного** текста промпта и конфигурации
оценщика (:func:`build_eval_key`); конверт хранит все поля ключа (``echo``) и любой
рассинхрон отвергается как промах — та же дисциплина, что у ``ExtractionCache``.

**Кэшируются только успешные вердикты** (``✅``/``⚠️``/``❌``/``❓``). ``FAILED`` и
``PRELIMINARY`` — исходы **несостоявшейся** попытки, а не вердикты: их заморозка
навсегда зафиксировала бы транзиентный сбой адаптера. ``N/A`` на выходе LLM запрещён
(I7) и тоже не кэшируется. ``put`` вызывается только после успешного
``validate_output``; при повторной валидации на следующем прогоне конверт обязан
пройти её снова, иначе считается промахом.

**Раскладка и запись.** ``{root}/{key[:2]}/{key}.json`` — без индекса и блокировок.
Запись атомарна (временный файл в том же каталоге → ``fsync`` → :func:`os.replace`).
Битый JSON, несовместимый формат, рассинхрон ``echo`` или невалидный выход — **промах**,
а не исключение (считается в :meth:`EvalVerdictCache.stats`).

**I18/I19.** Модуль не делает сетевых вызовов и не читает корпус. Хит лишь повторяет
``source_id``/``quote``; продукт-скоупед валидация (I17/I19) выполняется заново.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from core.domain import LLMCellOutput, Status
from core.util.hashing import short_hash

__all__ = [
    "ENVELOPE_FORMAT",
    "EVAL_CACHE_FORMAT",
    "EvalVerdictCache",
    "build_eval_key",
]

#: Версия формата конверта. Несовпадение — промах: будущая ревизия схемы не будет
#: обслужена старым читателем, поэтому несовместимая запись безопасна.
ENVELOPE_FORMAT = 1

#: Версия пространства ключей (семантика полей ``echo``). Входит в сам ключ и в
#: конверт; повышается при изменении набора/смысла полей ключа.
EVAL_CACHE_FORMAT = 1

#: Допустимые символы ключа: полный hex (``short_hash``). Заодно исключает ``/``,
#: ``..`` и прочие пути-ловушки: ключ — имя файла, а не произвольная строка.
_HASH_CHARS = frozenset("0123456789abcdefABCDEF")

#: Минимальная длина ключа: два символа нужны для шарда ``key[:2]``.
_MIN_KEY_LEN = 2

#: Статусы, которые являются вердиктом и подлежат кэшированию. ``N/A`` сюда не входит
#: (I7: запрещён на выходе LLM); ``FAILED``/``PRELIMINARY`` — не ``Status`` вовсе.
_CACHEABLE_STATUSES: frozenset[Status] = frozenset(
    {Status.OK, Status.PARTIAL, Status.ABSENT, Status.UNKNOWN}
)


def build_eval_key(echo: Mapping[str, Any]) -> str:
    """Строит ключ кэша из полного набора полей ``echo`` (32 hex-символа).

    ``echo`` — плоский JSON-словарь с версией формата/схемы, версией шаблона, хешем
    **отрендеренного** промпта, ``prompt_hash`` (§5, для аудита) и конфигурацией
    оценщика. Сортировка ключей делает результат независимым от порядка вставки.
    """
    payload = json.dumps(dict(echo), ensure_ascii=False, sort_keys=True)
    return short_hash(payload, 32)


def _valid_key(key: Any) -> bool:
    """Ключ пригоден как имя файла: непустой hex длиной ≥ 2 (без ``/``/``..``)."""
    if not isinstance(key, str) or len(key) < _MIN_KEY_LEN:
        return False
    return all(char in _HASH_CHARS for char in key)


class EvalVerdictCache:
    """Дисковый кэш сырых вердиктов оценщика (см. модульный docstring).

    :param root: корень кэша; конструктор **не** создаёт каталог и не делает I/O.
    :param max_bytes: бюджет размера (сумма ``*.json`` под ``root``); ``None`` — без
        вытеснения. После ``put``, превысившего бюджет, самые старые по ``mtime``
        записи удаляются, пока сумма не вернётся в бюджет (tie-break — путь).

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
        self._echo_mismatch = 0
        self._rejected = 0

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

    def get(self, key: str, *, echo: Mapping[str, Any]) -> LLMCellOutput | None:
        """Возвращает закэшированный :class:`LLMCellOutput` или ``None`` (промах).

        Промах — отсутствие файла, битый/нечитаемый JSON, несовместимый формат,
        несовпадение ключа или **любого** поля ``echo``, а также невалидный выход.
        Ни один случай не бросает исключение: устаревший или повреждённый конверт не
        должен ломать прогон и не должен обслуживаться. Невалидный ключ (не hex /
        содержит путь) — тоже промах.

        Функция детерминирована: результат зависит только от аргументов и файлов на диске.
        """
        if not _valid_key(key):
            self._invalid_key += 1
            self._misses += 1
            return None
        path = self._entry_path(key)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            self._misses += 1
            return None
        except (OSError, UnicodeDecodeError):
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
        if envelope.get("key") != key:
            self._misses += 1
            return None

        stored_echo = envelope.get("echo")
        if not isinstance(stored_echo, dict) or stored_echo != dict(echo):
            # Рассинхрон конфигурации/промпта: старый вердикт не должен обслуживаться.
            self._echo_mismatch += 1
            self._misses += 1
            return None

        serialized = envelope.get("output")
        if not isinstance(serialized, str) or not serialized:
            self._corrupt += 1
            self._misses += 1
            return None
        try:
            output = LLMCellOutput.model_validate_json(serialized)
        except ValueError:
            # Выход, не проходящий текущую схему/валидаторы, — промах.
            self._corrupt += 1
            self._misses += 1
            return None

        status_value = getattr(output.status, "value", output.status)
        if envelope.get("verdict_status") != status_value:
            self._corrupt += 1
            self._misses += 1
            return None

        self._hits += 1
        return output

    # ------------------------------------------------------------------ #
    # Запись
    # ------------------------------------------------------------------ #

    def put(
        self, key: str, output: LLMCellOutput, *, echo: Mapping[str, Any]
    ) -> None:
        """Сохраняет успешный вердикт (атомарно, без блокировок).

        :param key: ключ из :func:`build_eval_key`.
        :param output: **уже провалидированный** выход оценщика. Статусы вне
            ``✅``/``⚠️``/``❌``/``❓`` отвергаются: ``N/A`` запрещён (I7), а
            ``FAILED``/``PRELIMINARY`` — состояния ячейки, не вердикты.
        :param echo: тот же словарь, что передан в :func:`build_eval_key`.

        Невалидный ключ и сбой записи (нет прав, полный диск) — тихий no-op со счётчиком:
        кэш — оптимизация и не должен ронять прогон.
        """
        if not _valid_key(key):
            self._invalid_key += 1
            return
        # Кэшируется только сырой выход оценщика: ``Cell`` (и любой другой носитель)
        # отвергается, даже если у него случайно совпало поле ``status``.
        if not isinstance(output, LLMCellOutput):
            self._rejected += 1
            return
        status = getattr(output, "status", None)
        if status not in _CACHEABLE_STATUSES:
            self._rejected += 1
            return
        try:
            serialized = output.model_dump_json()
        except (TypeError, ValueError):
            self._rejected += 1
            return

        envelope = {
            "format": ENVELOPE_FORMAT,
            "key": key,
            "echo": dict(echo),
            "verdict_status": getattr(status, "value", status),
            "output": serialized,
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

        ``hits`` — обслуженные ``get``; ``misses`` — все промахи (включая битые файлы и
        рассинхрон ``echo``); ``writes`` — успешные ``put``; ``corrupt`` — битые/
        несовместимые конверты/выходы; ``evicted`` — вытесненные записи;
        ``invalid_key`` — отказы по невалидному ключу; ``echo_mismatch`` — промахи из-за
        несовпадения конфигурации/промпта; ``rejected`` — отказы кэшировать не-вердикт.
        """
        return {
            "hits": self._hits,
            "misses": self._misses,
            "writes": self._writes,
            "corrupt": self._corrupt,
            "evicted": self._evicted,
            "invalid_key": self._invalid_key,
            "echo_mismatch": self._echo_mismatch,
            "rejected": self._rejected,
        }

    # ------------------------------------------------------------------ #
    # Пути
    # ------------------------------------------------------------------ #

    def _entry_path(self, key: str) -> Path:
        """Путь записи: ``{root}/{key[:2]}/{key}.json`` (шардирование по префиксу)."""
        return self._root / key[:2] / f"{key}.json"
