"""Сборщик индексной сборки ``.npy`` — реализация ``IndexBuilderPort`` (W8/T8.2).

Единственный писатель ``.npy``-шардов индекса: продукт-партиции (I17) в раскладке
``index_store/{package}/{build}/{product_id}/`` пишутся **через единый носитель формата**
``adapters/index_format.py`` (``vectors.fp16.npy``, ``chunk_ids.jsonl``, ``manifest.json``);
раскладка здесь не дублируется. ``pipeline`` файлов индекса напрямую не касается (K3):
за физическую сборку отвечает этот адаптер, за эмбеддинги — ``EmbedderPort``.

**Продукт-партиции (I17).** ``ports.types.Chunk`` — нейтральный value-объект без
``product_id``; поэтому ``build()`` вызывается **на одну продукт-партицию**: все переданные
чанки принадлежат ``meta.product_id``. Это основной путь Ш5 — ``pipeline/corpus.py`` зовёт
``build`` в цикле по продуктам с ``meta.product_id``. Мультипродуктовый вызов намеренно не
поддерживается: смешение продуктов в одной партиции нарушило бы I17. Для нескольких
продуктов ``build`` вызывается несколько раз (один ``build_id`` → N партиций).

**``build_id`` (§13, T8.2 DoD).** Детерминированный, без random/UUID. Приоритет:

1. явный ``meta.extra["partition_build_id"]`` — его передаёт Ш5 из
   ``corpus._derive_partition_build_id``, поэтому физическая сборка и ``index_manifest``/
   ``corpus_chunks.build_id`` несут один и тот же идентификатор партиции;
2. иначе — производный :func:`derive_build_id` от (``package`` + ``chunker_version`` +
   ``embed_model_id`` + упорядоченные ``chunk_id`` + ``content_hash``). ``snapshot_date``
   намеренно **не** входит (09 §8.1: снимок — провенанс, не содержимое). Это тот же стиль,
   что и ``corpus._derive_partition_build_id`` (разделитель ``\\x1f``, префикс ``pb_``,
   ``short_hash(..., 24)``), но дополнительно с содержимым чанков — автономная сборка
   чувствительна к контенту. Совпадения с corpus-id для тех же входов не требуется:
   corpus всегда передаёт явный id.

**Иммутабельность (§13/K2).** ``index_format.write_partition`` пишет атомарно (staging →
``os.replace``), а ``NpyIndexBuilder`` дополнительно отказывает в повторном ``build`` по
ключу ``(package, build_id, product_id)`` при **другом** содержимом
(:class:`ImmutableBuildError`). Полный отпечаток (``vector_digest``: id/хеши + байты
``float16``-матрицы) кладётся в манифест, поэтому защита действует и между процессами;
``content_digest`` — контентный отпечаток партиции из id-формулы (09 §8.1/§8.5).
``commit()`` фиксирует сборки (проверяет наличие артефактов) и ничего не возвращает.

**Зависимости.** ``numpy`` — необязательный (extra ``retrieval``): модуль импортируется без
него, а ``build()`` поднимает понятную ``RuntimeError`` при отсутствии. Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.adapters import index_format
from core.ports.index_builder import IndexMeta
from core.ports.types import Chunk
from core.util.hashing import content_hash, short_hash

__all__ = ["ImmutableBuildError", "NpyIndexBuilder", "derive_build_id", "numpy_available"]

#: Префикс ``build_id`` (стиль ``corpus._derive_partition_build_id``: партиция).
_BUILD_ID_PREFIX = "pb_"
#: Разделитель полей material-строки (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"
#: Разделитель групп (id чанков ↔ хеши) — исключает коллизии склейки.
_GROUP_SEP = "\x1e"


def numpy_available() -> bool:
    """Установлен ли ``numpy`` (extra ``retrieval``); без импорта модуля."""
    import importlib.util

    return importlib.util.find_spec("numpy") is not None


def _import_numpy() -> Any:
    """Ленивый импорт ``numpy``; понятная ошибка, если extra ``retrieval`` не установлен."""
    try:
        import numpy as np  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - зависит от окружения
        raise RuntimeError(
            "NpyIndexBuilder: требуется numpy (установите extra 'retrieval')"
        ) from exc
    return np


def derive_build_id(
    package: str,
    chunker_version: str,
    embed_model_id: str,
    chunk_ids: Sequence[str],
    content_hashes: Sequence[str],
) -> str:
    """Детерминированный ``build_id`` автономной сборки (без явного id от вызывающего).

    Склейка фиксированным разделителем; порядок ``chunk_ids``/``content_hashes`` значим,
    т.к. задаёт отображение «строка ``chunk_ids.jsonl`` ↔ строка матрицы векторов». Группы
    (id ↔ хеши) разделены отдельным символом — это исключает коллизии при склейке.
    ``snapshot_date`` не входит (09 §8.1: провенанс, не содержимое). Возвращает ``pb_`` +
    24 hex-символа SHA-256.
    """
    material = _FIELD_SEP.join(
        (
            package,
            chunker_version,
            embed_model_id,
            _FIELD_SEP.join(chunk_ids),
            _FIELD_SEP.join(content_hashes),
        )
    )
    return _BUILD_ID_PREFIX + short_hash(material, 24)


def _content_digest(
    np: Any,
    chunk_ids: Sequence[str],
    content_hashes: Sequence[str],
    vectors: Sequence[Sequence[float]],
) -> str:
    """Отпечаток содержимого партиции: id/хеши чанков + байты ``float16``-матрицы (I19)."""
    matrix = np.asarray(vectors, dtype=np.float16)
    meta_material = _GROUP_SEP.join(
        (_FIELD_SEP.join(chunk_ids), _FIELD_SEP.join(content_hashes))
    ).encode("utf-8")
    return content_hash(meta_material + _GROUP_SEP.encode("utf-8") + matrix.tobytes())


class ImmutableBuildError(RuntimeError):
    """Попытка изменить уже зафиксированную сборку другим содержимым (K2/§13)."""


class NpyIndexBuilder:
    """``IndexBuilderPort``: пишет ``.npy``-шарды и ``chunk_ids``/``manifest`` по партициям.

    ``vector_store`` — метка физического хранилища для ``index_manifest`` (``npy-fp16``).
    ``lexical_engine_version`` — версия лексического движка, которым будет обслужен
    ретривал этой сборки (``tantivy-<ver>``; пусто — лексического плеча нет). Оба
    атрибута — часть контракта сборки: они входят в ``partition_build_id`` (§13:676)
    и в манифест партиции, по которому идёт второй, физический контроль
    переиспользования (09 §8.5). ``pipeline`` читает их у сборщика, поэтому значение
    должно быть фактическим, а не дефолтным.
    """

    vector_store = "npy-fp16"

    def __init__(
        self,
        index_root: str | Path = "index_store",
        package: str | None = None,
        *,
        lexical_engine_version: str = "",
    ) -> None:
        """``index_root`` — корень ``index_store``; ``package`` — пакет по умолчанию.

        ``lexical_engine_version`` — фактический лексический движок прогона (его
        передаёт composition root; см. ``core.cli.wiring``). Пустая строка означает
        «лексического движка нет», а не «версия неизвестна» (§13:676).

        Пакет разрешается как ``meta.package`` (авторитетно для конкретного вызова) и лишь
        при его пустоте — конструкторный ``package``. Дефолт ``"index_store"`` совпадает с
        :class:`~core.config.paths.PathsConfig`; composition root передаёт resolved
        ``paths.index_store`` (``03`` §A.1).
        """
        self._index_root = Path(index_root)
        self._package = package
        self.lexical_engine_version = str(lexical_engine_version or "")
        #: ``(package, build_id, product_id) -> content_digest`` (защита иммутабельности).
        self._content: dict[tuple[str, str, str], str] = {}
        self._pending: set[tuple[str, str, str]] = set()
        self._committed: set[tuple[str, str, str]] = set()

    @property
    def index_root(self) -> Path:
        """Корень индексного хранилища (для диагностики/composition root)."""
        return self._index_root

    def build(
        self,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        meta: IndexMeta,
    ) -> str:
        """Пишет продукт-партицию ``meta.product_id`` и возвращает ``build_id``.

        Все ``chunks`` принадлежат одной продукт-партиции (I17). ``vectors[i]`` — вектор
        ``chunks[i]`` (порядок сохраняется как отображение строк). Запись атомарна
        (``index_format.write_partition``).

        :raises RuntimeError: если ``numpy`` не установлен.
        :raises ValueError: при пустой партиции, несовпадении длин или отсутствии
            ``meta.product_id``/пакета.
        :raises ImmutableBuildError: если по ключу ``(package, build_id, product_id)`` уже
            есть сборка с другим содержимым (§13).
        """
        np = _import_numpy()

        if len(chunks) != len(vectors):
            raise ValueError(
                f"NpyIndexBuilder: chunks ({len(chunks)}) и vectors ({len(vectors)}) "
                "должны совпадать"
            )
        if not chunks:
            raise ValueError("NpyIndexBuilder: пустая продукт-партиция не собирается")

        package = meta.package or self._package
        if not package:
            raise ValueError(
                "NpyIndexBuilder: package обязателен (meta.package или конструктор)"
            )
        product_id = meta.product_id
        if not product_id:
            raise ValueError("NpyIndexBuilder: meta.product_id обязателен (I17)")

        chunk_ids = [chunk.chunk_id for chunk in chunks]
        content_hashes = [chunk.content_hash for chunk in chunks]
        # Полный отпечаток (id/хеши + байты float16-матрицы) — защита иммутабельности.
        digest = _content_digest(np, chunk_ids, content_hashes, vectors)

        extra = dict(meta.extra or {})
        explicit = extra.get("partition_build_id")
        build_id = (
            str(explicit)
            if explicit
            else derive_build_id(
                package,
                meta.chunker_version,
                meta.embed_model_id,
                chunk_ids,
                content_hashes,
            )
        )

        key = (package, build_id, product_id)
        self._guard_immutable(key, digest)

        dim = (
            int(np.asarray(vectors[0], dtype=np.float16).reshape(-1).size)
            if vectors
            else 0
        )
        manifest: dict[str, Any] = {
            "build_id": build_id,
            "partition_build_id": build_id,
            "package": package,
            "product_id": product_id,
            "embed_model_id": meta.embed_model_id,
            "chunker_version": meta.chunker_version,
            # 09 §8.1: версии производства/формата — часть второго, физического
            # контроля переиспользования партиции (§8.5), поэтому лежат в манифесте.
            "extractor_version": str(extra.get("extractor_version") or ""),
            # Явный провенанс от вызывающего (Ш5 передаёт его из ``pb``-входов)
            # приоритетен; иначе — честная метка самого сборщика, чтобы манифест
            # не разошёлся с версией, объявленной при конструировании.
            "lexical_engine_version": str(
                extra.get("lexical_engine_version") or self.lexical_engine_version
            ),
            "vector_store": str(extra.get("vector_store") or ""),
            "snapshot_date": meta.snapshot_date,
            "dim": dim,
            "stats": {"chunks": len(chunk_ids), "dim": dim},
            "extra": extra,
            # ``content_digest`` — контентный отпечаток партиции из id-формулы
            # (09 §8.1/§8.5): по нему второй, физический контроль решает о
            # переиспользовании. Полный отпечаток (включая векторы) — ``vector_digest``.
            "content_digest": str(extra.get("content_digest") or digest),
            "vector_digest": digest,
        }
        index_format.write_partition(
            self._index_root, package, build_id, product_id, vectors, chunk_ids, manifest
        )
        self._content[key] = digest
        self._pending.add(key)
        return build_id

    def _guard_immutable(self, key: tuple[str, str, str], digest: str) -> None:
        """Отказывает в мутации зафиксированной сборки другим содержимым (K2/§13).

        Проверяет (1) сборки этого инстанса и (2) уже зафиксированную партицию на диске —
        последнее даёт защиту между процессами (``vector_digest``; legacy-партиции
        хранят полный отпечаток в ``content_digest``).
        """
        existing = self._content.get(key)
        if existing is not None and existing != digest:
            raise ImmutableBuildError(
                f"NpyIndexBuilder: сборка {key!r} уже существует с другим содержимым "
                "(§13: иммутабельная сборка не мутируется)"
            )
        if existing is not None:
            return
        package, build_id, product_id = key
        if not index_format.is_committed(
            self._index_root, package, build_id, product_id
        ):
            return
        manifest = index_format.read_manifest(
            self._index_root, package, build_id, product_id
        )
        # ``vector_digest`` — полный отпечаток (id/хеши + векторы); legacy-партиции
        # хранят его в ``content_digest``.
        old_digest = manifest.get("vector_digest") or manifest.get("content_digest")
        if old_digest != digest:
            raise ImmutableBuildError(
                f"NpyIndexBuilder: партиция {key!r} уже зафиксирована на диске с другим "
                "содержимым (§13)"
            )

    def commit(self) -> None:
        """Фиксирует сборки, записанные с прошлого ``commit`` (атомарность — в ``write_partition``).

        Проверяет, что все три артефакта на месте, и помечает ключи зафиксированными. Повторный
        вызов без новых сборок — no-op. Иммутабельность обеспечивает :meth:`_guard_immutable`
        (в т.ч. после ``commit``).

        :raises RuntimeError: если ожидаемая партиция не зафиксирована на диске.
        """
        for key in sorted(self._pending):
            if not index_format.is_committed(self._index_root, *key):
                raise RuntimeError(
                    f"NpyIndexBuilder: партиция {key!r} не зафиксирована на диске"
                )
        self._committed.update(self._pending)
        self._pending.clear()
