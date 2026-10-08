"""coHound — сборка зависимостей CLI (composition root Z0, ручной DI, A1; ``03`` §E.3).

Единственное место, где ядро соединяет ``pipeline`` с адаптерами: ``pipeline`` только
принимает порты как аргументы и **никогда** не инстанцирует адаптеры сам (``01`` §1, A2).
Никаких фреймворков — ручной DI (A1).

**Профили развёртывания (``07``).** ``local`` (дефолт) собирает реальные адаптеры
in-process. ``split`` подставляет remote-прокси за ``ContentGateway`` — реализация
появится в W13 (``edge/``), поэтому здесь оставлен только чистый шов: явный отказ
вместо молчаливой неверной сборки. ``edge/`` и любой DMZ-код **не импортируются**.

**Состояние W5.** Большинство адаптеров (краул/экстрактор/чанкер/индекс/LLM/SERP)
появятся в W7–W11. Сейчас собирается только существующее — ``SqliteStore`` и
``Runner`` (фазы-заглушки), а отсутствующие порты остаются ``None`` с явным TODO.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from pathlib import Path

from core.pipeline import Ports, Runner
from core.pipeline.runner import PHASES, Clock, PhaseFn
from core.store.sqlite_store import SqliteStore

__all__ = [
    "LOCAL_PROFILE",
    "SPLIT_PROFILE",
    "SUPPORTED_PROFILES",
    "PHASES",
    "WiringError",
    "run_db_path",
    "build_store",
    "build_adapters",
    "build_runner",
]

#: Профиль ``local`` — один процесс, реальные адаптеры in-process (дефолт, ``07``).
LOCAL_PROFILE = "local"
#: Профиль ``split`` — веб-адаптеры в DMZ, ядро ходит через ``ContentGateway`` (``07``).
SPLIT_PROFILE = "split"
#: Известные профили развёртывания, выбираемые CLI (``03`` §D.5/D.6).
SUPPORTED_PROFILES: tuple[str, ...] = (LOCAL_PROFILE, SPLIT_PROFILE)


class WiringError(ValueError):
    """Ошибка сборки зависимостей: неизвестный или ещё не реализованный профиль."""


def run_db_path(runs_dir: str | Path, run_id: str) -> Path:
    """Путь к операционной БД прогона: ``runs/{run_id}/cohound.db`` (``03`` §A.2)."""
    return Path(runs_dir) / run_id / "cohound.db"


def build_store(
    db_path: str | Path = ":memory:", *, profile: str = LOCAL_PROFILE
) -> SqliteStore:
    """Собирает операционное хранилище прогона.

    Для обоих профилей ``StorePort`` живёт в ядре (доверенная зона ``core``, ``01`` §3.0),
    поэтому выбор профиля на хранилище не влияет — разделяются лишь веб-адаптеры
    (:func:`build_adapters`). Каталог БД создаётся при необходимости.

    :param db_path: файл БД или ``":memory:"``.
    :param profile: профиль развёртывания (проверяется на известность).
    :raises WiringError: неизвестный профиль.
    """
    if profile not in SUPPORTED_PROFILES:
        raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")
    if str(db_path) != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return SqliteStore(db_path)


def build_adapters(*, profile: str = LOCAL_PROFILE) -> Ports:
    """Собирает порты внешних систем по профилю развёртывания (``03`` §E.3).

    Сейчас (W5) реальных адаптеров ещё нет: возвращается контейнер :class:`Ports` с
    ``None``-полями, которые заполнят W7–W11. Профиль ``split`` намеренно пока не
    поддержан — remote-прокси и ``ContentGateway`` относятся к W13 (``07``); здесь
    зафиксирован чистый шов, а не заглушка, маскирующая отсутствие DMZ.

    :raises WiringError: профиль ``split`` (ещё не реализован) или неизвестный профиль.
    """
    if profile == LOCAL_PROFILE:
        # TODO(W7–W11): crawler/extractor/chunker (W7), индексы/embedder (W8),
        # evaluator (W9), search (W11), registry (W6) — заполнять здесь по мере появления.
        return Ports()
    if profile == SPLIT_PROFILE:
        # Шов 07/W13: remote-прокси (RemoteCrawler/RemoteSearch/…), вызывающие DMZ-сервис
        # через ContentGateway. `edge/` не импортируется из ядра (01 §1) — это отдельный
        # composition root Z1. До W13 профиль `split` честно недоступен.
        raise WiringError(
            "профиль split ещё не поддержан: remote-прокси и ContentGateway появятся "
            "в W13 (07). Используйте профиль local (дефолт)."
        )
    raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")


def build_runner(
    store: SqliteStore,
    *,
    profile: str = LOCAL_PROFILE,
    ports: Ports | None = None,
    phases: dict[str, PhaseFn] | None = None,
    clock: Clock | None = None,
) -> Runner:
    """Собирает :class:`Runner` с внедрёнными портами (ручной DI, A1).

    ``phases`` переопределяет реестр фаз (тесты/будущие W7–W11); по умолчанию действует
    реестр ``Runner`` (echo-заглушки + реальные проверки гейтов). Порты, если не переданы,
    собираются :func:`build_adapters` по профилю.

    :raises WiringError: неизвестный/нереализованный профиль (см. :func:`build_adapters`).
    """
    resolved_ports = ports if ports is not None else build_adapters(profile=profile)
    return Runner(store, ports=resolved_ports, clock=clock, phases=phases)
