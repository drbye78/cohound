"""Локальный эмбеддер — реализация ``EmbedderPort`` (W8/T8.1, Ш5).

Основной кандидат — ``BAAI/bge-m3`` (05 §2.3, V3): плотные эмбеддинги, приоритет RU,
8192 токена контекста. Модель локальна и не покидает контур; ``model_id``/``dim``
пишутся в ``IndexManifestEntry``/``RunManifest`` (§13, поле ``embed_model_id``), поэтому
они — атрибуты адаптера, а не константы вызывающего кода.

**Батч.** ``embed`` принимает ``Sequence[str]`` и нарезает вход на батчи размера
``batch_size``, конкатенируя результаты. Порядок выхода совпадает с порядком входа
(детерминизм, A3).

**Ревизия и оффлайн.** ``revision`` (git-sha/тег) входит в ``model_id`` как
``repo@<sha>``, поэтому ``embed_model_id`` описывает фактические веса, а не «текущий
main»; ``local_files_only=True`` по умолчанию — как у ``LiteEmbedder``: веса берутся из
локального кэша, и отсутствие снапшота видно как ошибка загрузки, а не как тихий поход
в интернет из ядра.

**Ленивый импорт тяжёлой зависимости.** ``sentence-transformers`` (тянет ``torch``)
импортируется только в реальном пути. Модуль импортируется и тестируется без него;
``available`` показывает готовность.

**Изолированный шов для тестов.** Конструктор принимает ``embed_fn`` — callable
``(list[str]) -> list[list[float]]``. Если он задан, реальный путь не задействуется,
и юнит-тесты не требуют ``torch``/сети. Если ``embed_fn is None`` — ленивая загрузка
настоящей модели (кэшируется в экземпляре).

**Контракт отсутствия зависимости.** Если ``sentence-transformers`` не установлен и
``embed_fn`` не внедрён, ``embed`` поднимает ``EmbedderUnavailable`` (подкласс
``RuntimeError``). Адаптер **никогда** не подменяет недоступность нулевыми векторами —
это исказило бы индекс и сделало бы карту невоспроизводимой.

**Ошибки размерности.** Если вектор из ``embed_fn``/модели не совпадает по длине с
``dim`` (или число векторов не совпадает с числом текстов), поднимается ``ValueError``
с ясным сообщением: рассинхронизация ``dim`` и манифеста — версионная граница (§13).

**Будущее.** ``LiteEmbedder`` (``model2vec``, CPU без ``torch``, extra ``embed-lite``,
05 §2.3) реализован в sibling-модуле ``embed_model2vec.py`` (P4a, оффлайн-путь по умолчанию;
иной ``dim`` = 256 и иной API загрузки). ``LocalEmbedder`` остаётся качественным путём
(``bge-m3``) при наличии ``sentence-transformers``.

Происхождение идеи ленивого шва с ``embed_fn`` — локальная практика тестирования
адаптеров coHound (``extract_html``/``crawl_scrapy``); сторонний код не копировался.
Нейтрален (§1.3): ``BAAI/bge-m3`` — идентификатор модели (данные, не категория), и он
переопределяется через параметры.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable, Sequence
from typing import Any

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_DIM",
    "DEFAULT_MODEL_ID",
    "EmbedFn",
    "EmbedderUnavailable",
    "LocalEmbedder",
]

#: Модель-кандидат по умолчанию (05 §2.3, V3): плотные мультиязычные эмбеддинги bge-m3.
DEFAULT_MODEL_ID = "BAAI/bge-m3"
#: Размерность dense-вектора bge-m3.
DEFAULT_DIM = 1024
#: Батч по умолчанию: компромисс памяти/скорости при CPU-инференсе.
DEFAULT_BATCH_SIZE = 32

#: Внедряемая функция эмбеддинга: батч текстов → батч векторов (без torch).
EmbedFn = Callable[[list[str]], list[list[float]]]


class EmbedderUnavailable(RuntimeError):
    """Эмбеддер недоступен: нет тяжёлой зависимости и не внедрён ``embed_fn``."""


def _sentence_transformers_available() -> bool:
    """Установлен ли ``sentence-transformers`` (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("sentence_transformers") is not None


class LocalEmbedder:
    """Локальный батчевый эмбеддер (bge-m3). Реализация ``EmbedderPort``.

    :param embed_fn: шов для тестов/подмены; ``None`` → реальная модель (лениво).
    :param model_id: идентификатор модели (HF-repo); пишется в манифест как
        ``embed_model_id`` — с пином ревизии, если она объявлена (см. ``revision``).
    :param dim: ожидаемая размерность вектора (для bge-m3 — 1024).
    :param batch_size: размер батча при нарезке входа (детерминирован).
    :param revision: пин ревизии весов (git-sha/тег). При задании ``model_id``
        становится ``repo@<rev>``: иначе ``embed_model_id`` описывал бы «какую-то»
        версию весов, а не фактические байты, из которых построен индекс (тот же
        приём, что в :class:`core.adapters.embed_model2vec.LiteEmbedder`).
    :param local_files_only: не ходить в сеть за весами (дефолт ``True`` — как у
        ``LiteEmbedder``): снапшот модели обязан быть в локальном кэше HF, иначе
        загрузка **видимо** падает. Это осознанно: ядро не тянет веса из интернета
        в момент прогона, а ``available`` говорит только о наличии стека, не о кэше.
        Оператор, которому нужен первый скачок, передаёт ``local_files_only=False``
        явно — тогда это его объявленное действие, а не умолчание адаптера.
    """

    def __init__(
        self,
        embed_fn: EmbedFn | None = None,
        *,
        model_id: str = DEFAULT_MODEL_ID,
        dim: int = DEFAULT_DIM,
        batch_size: int = DEFAULT_BATCH_SIZE,
        revision: str | None = None,
        local_files_only: bool = True,
    ) -> None:
        if not model_id:
            raise ValueError("model_id обязан быть непустым")
        if dim <= 0:
            raise ValueError(f"dim обязан быть положительным, получено {dim!r}")
        if batch_size <= 0:
            raise ValueError(f"batch_size обязан быть положительным, получено {batch_size!r}")
        self._repo_id = model_id
        #: Идентификатор с пином ревизии: ``repo@<sha>`` (пишется в манифест, §13).
        self.model_id = f"{model_id}@{revision}" if revision else model_id
        self.revision = revision
        self.local_files_only = local_files_only
        self.dim = dim
        self.batch_size = batch_size
        self._embed_fn = embed_fn
        self._model: Any = None

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``embed_fn`` либо установлен стек модели."""
        if self._embed_fn is not None:
            return True
        return _sentence_transformers_available()

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Кодирует тексты батчами; возвращает ``N × dim`` списков ``float``.

        :raises EmbedderUnavailable: нет зависимости и не внедрён ``embed_fn``.
        :raises ValueError: размерность/число векторов не совпали с ожиданием.
        """
        items = [str(t) for t in texts]
        if not items:
            return []
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
        if not _sentence_transformers_available():
            raise EmbedderUnavailable(
                "sentence-transformers не установлен: эмбеддинг недоступен. "
                "Установите extra 'embed' (05 §2.3) или передайте embed_fn."
            )
        return self._load_model()

    def _load_model(self) -> EmbedFn:
        """Лениво загружает ``SentenceTransformer`` и кэширует его в экземпляре.

        Ревизия и оффлайн-режим передаются в загрузчик явно: без ``revision``
        загрузились бы «текущие» веса, и ``embed_model_id`` перестал бы описывать
        фактическую сборку (C11).
        """
        if self._model is None:
            from sentence_transformers import SentenceTransformer  # type: ignore[import-not-found]

            self._model = SentenceTransformer(
                self._repo_id,
                revision=self.revision,
                local_files_only=self.local_files_only,
            )

        model = self._model

        def _encode(batch: list[str]) -> list[list[float]]:
            matrix = model.encode(
                batch,
                batch_size=len(batch),
                normalize_embeddings=True,
                convert_to_numpy=True,
            )
            return matrix.tolist()

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
