"""Глобальный content-addressed кэш внешних вызовов (R7, ADR 0016, §11.7).

Живые внешние вызовы (краул HTTP и SERP) кэшируются **глобально** — в корне
репозитория (``cache/http/{url_hash}.bin``, ``cache/serp/{query_hash}.json``), а не
внутри ``runs/{run_id}/``. Это делает R7 (двойной прогон) достижимым **между
разными ``run_id``**: второй прогон читает тот же кэш по хэшу ключа, без живого
ре-фетча (03 §A.2, 04 §A.2/§A.3, ADR 0016).

Модуль категорийно-нейтрален (§1.3), использует только stdlib и другие утилиты
ядра; внешних зависимостей нет.

Алгоритм ключа (детерминирован, версионирован префиксом схемы):

* ``http_key(url)`` — ключ = ``short_hash("http\\x1f" + url.strip(), 32)``.
  URL не приводится к нижнему регистру (путь и query-часть регистрозависимы),
  удаляются только ведущие/замыкающие пробелы. Префикс схемы разводит пространства
  ключей HTTP и SERP на случай совпадения материалов.
* ``serp_key(query, provider=None)`` — запрос нормализуется: ``strip`` +
  схлопывание любых пробельных последовательностей в один пробел + ``casefold``
  (регистронезависимое сравнение запросов). Провайдер нормализуется так же
  (``None`` → пустая строка). Ключ = ``short_hash("serp\\x1f" + provider + "\\x1f"
  + query, 32)``.

Длина ключа — 32 hex-символа (128-битный усечённый SHA-256); коллизий для объёма
кэша проекта достаточно. Хэш-функция — ``core.util.hashing.short_hash`` (SHA-256).
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from core.util.hashing import short_hash
from core.util.security import sanitize_filename

__all__ = ["ContentAddressedCache", "http_key", "serp_key"]

# Разделитель полей — unit separator: не встречается в URL/запросах/провайдерах и
# делает склейку однозначной (как в hashing.py).
_FIELD_SEP = "\x1f"

# Длина ключа в hex-символах (см. докстринг модуля).
_KEY_LEN = 32

# Суффиксы по назначению: сырые байты HTTP и JSON-ответы SERP (03 §A.1).
_BYTES_SUFFIX = ".bin"
_JSON_SUFFIX = ".json"

# Допустимый суффикс файла: точка + алфанумерика, без разделителей пути.
_SUFFIX_RE = re.compile(r"^\.[A-Za-z0-9]+$")

# Стандартные подкаталоги кэша в корне репозитория (03 §A.1).
_DEFAULT_SUBDIRS: tuple[str, ...] = ("serp", "http")


def http_key(url: str) -> str:
    """Детерминированный ключ HTTP-ресурса: ``strip`` (без lower) → truncated SHA-256.

    Регистр сохраняется, т.к. путь/query URL регистрозависимы; ведущие и замыкающие
    пробелы не влияют на ключ.
    """
    material = f"http{_FIELD_SEP}{url.strip()}"
    return short_hash(material, _KEY_LEN)


def _normalize_serp_part(value: str) -> str:
    """Нормализация компонента SERP: схлопывание пробелов + ``casefold``."""
    return " ".join(value.split()).casefold()


def serp_key(query: str, provider: str | None = None) -> str:
    """Детерминированный ключ SERP-запроса: нормализация query/provider → truncated SHA-256.

    Нормализация (см. докстринг модуля): ``strip`` + схлопывание пробелов +
    ``casefold``. Провайдер ``None`` трактуется как пустая строка и участвует в
    ключе (смена провайдера → новый ключ). Пустой (после нормализации) запрос
    отклоняется — кэшировать нечего.
    """
    normalized_query = _normalize_serp_part(query)
    if not normalized_query:
        raise ValueError("SERP-запрос пуст после нормализации")
    normalized_provider = _normalize_serp_part(provider) if provider is not None else ""
    material = f"serp{_FIELD_SEP}{normalized_provider}{_FIELD_SEP}{normalized_query}"
    return short_hash(material, _KEY_LEN)


class ContentAddressedCache:
    """Файловый content-addressed кэш внешних вызовов (ADR 0016).

    Раскладка: ``{root}/{kind}/{key}{suffix}``; по умолчанию ``root="cache"`` и
    подкаталоги ``("serp", "http")``. Каталоги создаются по требованию, запись
    атомарна (temp-файл + ``os.replace``), поэтому частичная запись не повреждает
    закэшированную запись. Ключи санитизируются и проверяются на выход за пределы
    ``root`` (path traversal).
    """

    # Ключи — чистая функция, доступны и как статические методы класса, и как
    # методы экземпляра: ``ContentAddressedCache.http_key(url)`` / ``cache.http_key(url)``.
    http_key = staticmethod(http_key)
    serp_key = staticmethod(serp_key)

    def __init__(
        self,
        root: str | Path = "cache",
        subdirs: tuple[str, ...] = _DEFAULT_SUBDIRS,
    ) -> None:
        if not subdirs:
            raise ValueError("subdirs не может быть пустым")
        for sub in subdirs:
            if not sub or sub in {".", ".."} or "/" in sub or "\\" in sub:
                raise ValueError(f"недопустимый подкаталог кэша: {sub!r}")
        self._root = Path(root)
        self._subdirs = tuple(subdirs)

    @property
    def root(self) -> Path:
        """Корень кэша (без создания каталогов)."""
        return self._root

    def path_for(self, kind: str, key: str, suffix: str) -> Path:
        """Безопасный путь ``{root}/{kind}/{key}{suffix}`` для известного ``kind``.

        Отклоняет неизвестный ``kind``, некорректный ``suffix`` и ключи, которые
        не совпадают с собственной санитизированной формой (разделители пути,
        ведущие точки, null-байты) — выход за пределы ``root`` невозможен.
        """
        if kind not in self._subdirs:
            raise ValueError(f"неизвестный вид кэша: {kind!r}")
        if not _SUFFIX_RE.match(suffix):
            raise ValueError(f"недопустимый суффикс кэша: {suffix!r}")
        safe = sanitize_filename(key)
        if safe != key:
            raise ValueError(f"недопустимый ключ кэша: {key!r}")
        target = (self._root / kind / f"{safe}{suffix}").resolve()
        if not target.is_relative_to(self._root.resolve()):
            raise ValueError(f"ключ выходит за пределы кэша: {key!r}")
        return target

    def get_bytes(self, kind: str, key: str) -> bytes | None:
        """Сырые байты записи (``.bin``) или ``None`` при промахе."""
        target = self.path_for(kind, key, _BYTES_SUFFIX)
        if not target.is_file():
            return None
        return target.read_bytes()

    def put_bytes(self, kind: str, key: str, data: bytes) -> Path:
        """Атомарно записывает сырые байты (``.bin``); возвращает путь записи."""
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data должен быть bytes")
        target = self.path_for(kind, key, _BYTES_SUFFIX)
        return self._write_atomic(target, bytes(data))

    def get_json(self, kind: str, key: str) -> Any | None:
        """Десериализованный JSON записи (``.json``) или ``None`` при промахе."""
        target = self.path_for(kind, key, _JSON_SUFFIX)
        if not target.is_file():
            return None
        return json.loads(target.read_text(encoding="utf-8"))

    def put_json(self, kind: str, key: str, obj: Any) -> Path:
        """Атомарно записывает JSON (``.json``) канонически; возвращает путь записи.

        Канонизация (``sort_keys``, компактные разделители, ``ensure_ascii=False``)
        делает байтовое представление детерминированным при одинаковом объекте.
        """
        payload = json.dumps(
            obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        target = self.path_for(kind, key, _JSON_SUFFIX)
        return self._write_atomic(target, payload)

    def get_or_fetch(
        self, kind: str, key: str, fetch: Callable[[], bytes]
    ) -> bytes:
        """Возвращает закэшированные байты или вызывает ``fetch`` и кэширует результат.

        ``fetch`` вызывается **ровно один раз** при промахе; при попадании не
        вызывается вовсе. Результат ``fetch`` должен быть ``bytes``.
        """
        cached = self.get_bytes(kind, key)
        if cached is not None:
            return cached
        data = fetch()
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("fetch должен вернуть bytes")
        self.put_bytes(kind, key, bytes(data))
        return bytes(data)

    def _write_atomic(self, target: Path, data: bytes) -> Path:
        """Атомарная запись: temp-файл в том же каталоге → ``os.fsync`` → ``os.replace``.

        Temp-файл удаляется при любой ошибке, поэтому частичная запись не оставляет
        мусора и не повреждает существующую запись.
        """
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
        )
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return target
