"""Лёгкий оффлайн-эмбеддер на ``model2vec`` — реализация ``EmbedderPort`` (P4a, Ш5).

Альтернатива :mod:`core.adapters.embed_local` (``bge-m3``): **статичные** эмбеддинги
без ``torch`` (чистый NumPy, CPU). Модель-кандидат по умолчанию —
``minishlab/potion-multilingual-128M`` (MIT, дистиллят нашего baseline ``BAAI/bge-m3``,
XLM-R токенизатор → RU+EN, ``dim = 256``; см. research ``embedder.md``). Модель
локальна и не покидает контур; ``model_id``/``dim`` пишутся в
``IndexManifestEntry``/``RunManifest`` (§13, поле ``embed_model_id``).

**Ленивый импорт тяжёлой зависимости.** ``model2vec``/``huggingface_hub``/``numpy``
импортируются только в реальном пути. Модуль импортируется и тестируется без них;
``available`` показывает готовность.

**Изолированный шов для тестов.** Конструктор принимает ``embed_fn`` — callable
``(list[str]) -> list[list[float]]``. Если он задан, реальный путь не задействуется,
и юнит-тесты не требуют модели/сети.

**Контракт отсутствия зависимости/модели.** Если ``model2vec`` не установлен,
снапшот модели не найден локально или ``embed_fn`` не внедрён, ``embed`` поднимает
``EmbedderUnavailable`` (подкласс ``RuntimeError``). Адаптер **никогда** не подменяет
недоступность нулевыми векторами — это исказило бы индекс и сделало бы карту
невоспроизводимой.

**Гард пустого входа.** ``model2vec`` для пустого/пробельного текста возвращает
**нулевой** вектор и upcast'ит весь батч в ``float64`` — это нарушило бы контракт
«никогда нулевых векторов». Поэтому пустые/пробельные тексты отвергаются
(``ValueError``), а выходной dtype фиксируется как ``float32`` (``.tolist()``).

**Воспроизводимость (§13).** ``StaticModel.from_pretrained`` в 0.9.x **не** принимает
``revision``. Ревизия резолвится вызывающим: ``revision=<sha>`` → адаптер грузит
**локальный путь** снапшота (``huggingface_hub.snapshot_download(..., local_files_only=True)``
либо явный ``model_path``) и никогда не ходит в сеть. Ревизия входит в ``model_id``
вида ``repo@<sha>``, поэтому попадает в ``index_build_id``/манифест как версионная
граница ``execution_versions`` (§13).

Ошибки размерности/числа векторов — как у sibling'а ``embed_local.py``: рассинхронизация
``dim`` и манифеста поднимает ``ValueError``.

Нейтрален (§1.3): идентификатор модели по умолчанию — данные документации адаптера,
переопределяется параметром/конфигом.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.adapters.embed_local import EmbedderUnavailable, EmbedFn

__all__ = [
    "EmbedderUnavailable",
    "EmbedFn",
    "LiteEmbedder",
    "DEFAULT_MODEL_ID",
    "DEFAULT_DIM",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_MAX_LENGTH",
    "model2vec_available",
]

#: Модель по умолчанию: статичный мультиязычный дистиллят ``BAAI/bge-m3`` (RU+EN, MIT).
DEFAULT_MODEL_ID = "minishlab/potion-multilingual-128M"
#: Размерность dense-вектора модели (не 1024 как у bge-m3).
DEFAULT_DIM = 256
#: Батч по умолчанию: компромисс памяти/скорости при CPU-инференсе.
DEFAULT_BATCH_SIZE = 1024
#: Длина контекста: ``None`` — без молчаливой усечки (она меняет tuple индекса, §13).
DEFAULT_MAX_LENGTH: int | None = None


def model2vec_available() -> bool:
    """Установлен ли ``model2vec`` (проверка без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("model2vec") is not None


class LiteEmbedder:
    """Статичный оффлайн-эмбеддер (``model2vec``). Реализация ``EmbedderPort``.

    :param embed_fn: шов для тестов/подмены; ``None`` → реальная модель (лениво).
    :param model_id: HF-repo-идентификатор модели (данные, не категория).
    :param dim: ожидаемая размерность вектора (для дефолтной модели — 256).
    :param batch_size: размер батча при нарезке входа (детерминирован).
    :param revision: пин ревизии (git-sha/тег) — входит в ``model_id`` как ``repo@<sha>``.
    :param model_path: явный локальный каталог снапшота; ``None`` → резолв из HF-кэша
        (только локально, без сети).
    :param max_length: cap длины контекста токенизатора; ``None`` — без усечки.
    """

    def __init__(
        self,
        embed_fn: EmbedFn | None = None,
        *,
        model_id: str = DEFAULT_MODEL_ID,
        dim: int = DEFAULT_DIM,
        batch_size: int = DEFAULT_BATCH_SIZE,
        revision: str | None = None,
        model_path: str | Path | None = None,
        max_length: int | None = DEFAULT_MAX_LENGTH,
    ) -> None:
        if not model_id:
            raise ValueError("model_id обязан быть непустым")
        if dim <= 0:
            raise ValueError(f"dim обязан быть положительным, получено {dim!r}")
        if batch_size <= 0:
            raise ValueError(f"batch_size обязан быть положительным, получено {batch_size!r}")
        if max_length is not None and max_length <= 0:
            raise ValueError(f"max_length обязан быть положительным, получено {max_length!r}")
        self._repo_id = model_id
        #: Идентификатор с пином ревизии: ``repo@<sha>`` (пишется в манифест, §13).
        self.model_id = f"{model_id}@{revision}" if revision else model_id
        self.dim = dim
        self.batch_size = batch_size
        self.revision = revision
        self.max_length = max_length
        self._model_path = Path(model_path) if model_path is not None else None
        self._embed_fn = embed_fn
        self._model: Any = None
        self._resolved_dir: Path | None = None
        self._resolved = False

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``embed_fn`` либо установлены стек и модель."""
        if self._embed_fn is not None:
            return True
        if not model2vec_available():
            return False
        return self._resolve_model_dir() is not None

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Кодирует тексты батчами; возвращает ``N × dim`` списков ``float``.

        :raises EmbedderUnavailable: нет зависимости/модели и не внедрён ``embed_fn``.
        :raises ValueError: пустой/пробельный текст (model2vec дал бы нулевой вектор)
            либо размерность/число векторов не совпали с ожиданием.
        """
        items = [str(t) for t in texts]
        if not items:
            return []
        for index, item in enumerate(items):
            if not item.strip():
                raise ValueError(
                    f"LiteEmbedder: пустой/пробельный текст на позиции {index}: "
                    "model2vec вернул бы нулевой вектор (нарушение контракта эмбеддера)"
                )
        encode = self._resolve_encode()
        vectors: list[list[float]] = []
        for start in range(0, len(items), self.batch_size):
            batch = items[start : start + self.batch_size]
            vectors.extend(self._validate(encode(batch), expected=len(batch)))
        return vectors

    # ------------------------------------------------------------------
    # Внутреннее
    # ------------------------------------------------------------------

    def _resolve_encode(self) -> EmbedFn:
        """Выбирает шов ``embed_fn`` либо реальный путь (ленивая загрузка модели)."""
        if self._embed_fn is not None:
            return self._embed_fn
        if not model2vec_available():
            raise EmbedderUnavailable(
                "model2vec не установлен: эмбеддинг недоступен. "
                "Установите extra 'embed-lite' или передайте embed_fn."
            )
        return self._load_model()

    def _resolve_model_dir(self) -> Path | None:
        """Локальный каталог снапшота (без сети) или ``None``; результат кэшируется."""
        if self._resolved:
            return self._resolved_dir
        self._resolved = True
        if self._model_path is not None:
            self._resolved_dir = self._model_path if self._model_path.is_dir() else None
            return self._resolved_dir
        try:
            from huggingface_hub import snapshot_download  # type: ignore[import-not-found]

            self._resolved_dir = Path(
                snapshot_download(
                    self._repo_id,
                    revision=self.revision,
                    local_files_only=True,
                )
            )
        except Exception:  # noqa: BLE001 — отсутствие снапшота → недоступность, не падение
            self._resolved_dir = None
        return self._resolved_dir

    def _load_model(self) -> EmbedFn:
        """Лениво загружает ``StaticModel`` из **локального** каталога и кэширует его."""
        if self._model is None:
            local_dir = self._resolve_model_dir()
            if local_dir is None:
                raise EmbedderUnavailable(
                    "LiteEmbedder: снапшот модели не найден локально "
                    f"({self.model_id!r}): оффлайн-инференс невозможен. "
                    "Задайте model_path/revision или передайте embed_fn."
                )
            from model2vec import StaticModel  # type: ignore[import-not-found]

            self._model = StaticModel.from_pretrained(str(local_dir))

        model = self._model

        def _encode(batch: list[str]) -> list[list[float]]:
            import numpy as np  # type: ignore[import-not-found]

            matrix = np.asarray(
                model.encode(
                    batch,
                    batch_size=len(batch),
                    max_length=self.max_length,
                    use_multiprocessing=False,
                )
            )
            # ``encode("x")`` на одиночной строке даёт 1-D массив ``(dim,)``.
            if matrix.ndim == 1:
                matrix = matrix.reshape(1, -1)
            return matrix.astype(np.float32, copy=False).tolist()

        return _encode

    def _validate(self, vectors: Any, *, expected: int) -> list[list[float]]:
        """Проверяет форму/размерность и приводит к ``list[list[float]]``."""
        rows = list(vectors)
        if len(rows) != expected:
            raise ValueError(
                f"эмбеддер вернул {len(rows)} векторов на {expected} текстов"
            )
        result: list[list[float]] = []
        for row in rows:
            values = [float(x) for x in row]
            if len(values) != self.dim:
                raise ValueError(
                    f"размерность вектора {len(values)} ≠ dim={self.dim} "
                    f"(model_id={self.model_id!r}); dim и манифест должны совпадать"
                )
            result.append(values)
        return result
