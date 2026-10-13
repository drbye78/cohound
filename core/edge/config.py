"""coHound — runtime-конфигурация edge-узла (Z1/DMZ; Phase 3.0, 07 §4/§6).

Edge — **второй composition root**: собственные операционные параметры (каталог состояния,
адрес прослушивания, карта воркеров движков, back-pressure), а не harness-раскладка ядра
(``core/config/paths.py``). Здесь нет предметной терминологии (§1.3) и нет доступа к
store/domain: конфиг описывает только ресурсы веб-зоны.

Формат ``edge.yaml`` (все поля опциональны; отсутствие файла/поля → дефолт)::

    state_dir: ./edge_state
    blob_dir: ./edge_state/blobs
    host: 127.0.0.1
    port: 0
    workers: {http: 4, scrapy: 2, browser: 1}
    queue_limit: 256
    ttl_s: 86400
    schema_version: "1"

Env ``COHOUND_EDGE_STATE_DIR`` переопределяет ``state_dir`` (операторский шов раскладки
edge; симметрично ``COHOUND_*_DIR`` ядра). Схема строгая (``extra="forbid"``).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

__all__ = [
    "ENV_EDGE_STATE_DIR",
    "DEFAULT_EDGE_RUNTIME_FILE",
    "EdgeRuntimeConfig",
    "load_edge_runtime_config",
]

#: Env-переменная каталога состояния edge (переопределяет ``state_dir``).
ENV_EDGE_STATE_DIR = "COHOUND_EDGE_STATE_DIR"

#: Канонический путь runtime-конфига edge относительно CWD (если передан ``None`` не нужен).
DEFAULT_EDGE_RUNTIME_FILE: Path = Path("config/edge.yaml")

#: Дефолтная карта воркеров движков (кол-во параллельных слотов на движок).
_DEFAULT_WORKERS: dict[str, int] = {"http": 4, "scrapy": 2, "browser": 1}


class EdgeRuntimeConfig(BaseModel):
    """Runtime-конфигурация edge-узла: состояние, прослушивание, воркеры, TTL.

    Не путать с harness-раскладкой ядра: это **собственные** каталоги edge (DMZ), а не
    ``packages/``/``runs/``/``index_store/`` (07 §4: доверенные зоны разделены).

    ``blob_dir`` по умолчанию не задан; эффективный каталог блобов — ``{state_dir}/blobs``
    (:attr:`resolved_blob_dir`). ``workers`` — карта ``engine → число слотов``; ключи —
    имена движков краула (``http``/``scrapy``/``browser``), значение задаёт, какие движки
    собирает composition root edge.
    """

    model_config = ConfigDict(extra="forbid")

    #: Каталог состояния edge (edge-local БД/метаданные); дефолт относителен CWD.
    state_dir: str = "./edge_state"
    #: Каталог сырых блобов; ``None`` → ``{state_dir}/blobs`` (:attr:`resolved_blob_dir`).
    blob_dir: str | None = None
    #: Адрес прослушивания шва ``/v1/*``.
    host: str = "127.0.0.1"
    #: Порт прослушивания; ``0`` — эфемерный (ОС выбирает свободный).
    port: int = 0
    #: Карта ``engine → число слотов`` (какие движки собирает composition root edge).
    workers: dict[str, int] = Field(default_factory=lambda: dict(_DEFAULT_WORKERS))
    #: Потолок очереди (back-pressure): максимум одновременно принятых задач.
    queue_limit: int = 256
    #: TTL блобов, сек (``86400`` — сутки); истечение — ответственность edge-хранилища.
    ttl_s: int = 86400
    #: Версия конверта шва (совпадает с ``EDGE_SCHEMA_VERSION``, 07 §3/§7).
    schema_version: str = "1"

    @property
    def resolved_blob_dir(self) -> str:
        """Эффективный каталог блобов: явный ``blob_dir`` либо ``{state_dir}/blobs``."""
        return self.blob_dir or f"{self.state_dir.rstrip('/')}/blobs"


def _read_yaml(path: str | Path) -> Any:
    """Читает YAML-файл; ошибки I/O и синтаксиса приводит к ``ValueError``."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {p}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {p}: {exc}") from exc


def load_edge_runtime_config(path: str | Path | None = None) -> EdgeRuntimeConfig:
    """Загружает runtime-конфигурацию edge (:class:`EdgeRuntimeConfig`).

    Источники (по убыванию приоритета): env ``COHOUND_EDGE_STATE_DIR`` (переопределяет
    ``state_dir``) > YAML-файл ``path`` > дефолты модели. ``path=None`` или отсутствующий
    файл → только env/дефолты. Схема строгая (``extra="forbid"``): неизвестные поля
    отвергаются.

    :raises ValueError: нечитаемый/некорректный YAML; корень не словарь; схема не проходит
        валидацию (неизвестное поле, неверный тип значения и т.п.).
    """
    data: dict[str, Any] = {}
    if path is not None:
        p = Path(path)
        if p.exists():
            raw = _read_yaml(p)
            if raw is not None:
                if not isinstance(raw, dict):
                    raise ValueError(
                        f"{p}: ожидался YAML-словарь верхнего уровня, "
                        f"получено {type(raw).__name__}"
                    )
                data = dict(raw)

    env_state = os.environ.get(ENV_EDGE_STATE_DIR)
    if env_state is not None and env_state.strip():
        data["state_dir"] = env_state.strip()

    try:
        return EdgeRuntimeConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(
            f"edge.yaml: некорректная runtime-конфигурация edge: {exc}"
        ) from exc
