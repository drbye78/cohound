"""Зонд артефакта — классификация **до** экстракции (B3, шаг между краулом и извлечением).

Шаг отвечает на вопросы, которые решаются **без** разбора содержимого:

* что это за артефакт (класс/формат/размер) — по байтам, а не по URL-суффиксу;
* сколько страниц в PDF — по структуре файла (без рендеринга, см.
  :mod:`core.util.document_probe`);
* что внутри контейнера — по **индексу** (``ContainerPort.list_members``: central
  directory / заголовок архива). Тела членов не читаются: листинг — не извлечение
  (инвариант (a) порта).

Запись зонда — провенанс прогона: она объясняет, почему артефакт пошёл маршрутом спуска
или экстрактора, и **не** зависит от того, удалось ли извлечение. ``archive_descent=plan``
останавливается ровно здесь: листинг и прогноз есть, членов нет.

Слой ``pipeline``: читатель контейнеров инъектируется (``container_for``) — адаптеры здесь
не импортируются (A1/матрица 01 §1). Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.ports.container import ContainerPort
from core.util.document_probe import ArtifactProbe, classify_artifact

__all__ = ["ArtifactProbeRecord", "probe_artifact"]


@dataclass(frozen=True)
class ArtifactProbeRecord:
    """Наблюдение зонда: класс артефакта + (для контейнера) индекс его состава.

    ``container`` — сводка листинга: ``format``/``members_total``/``solid``/``encrypted``/
    ``truncated``/``unreadable``. Она **не** заменяет прогноз спуска
    (``archive_forecast``): зонд фиксирует «что лежит в индексе», спуск — «что из этого
    будет взято и почему». Для не-контейнера ``container is None``.
    """

    product_id: str
    url: str
    artifact: ArtifactProbe
    container: dict[str, Any] | None = None

    @property
    def is_container(self) -> bool:
        """Контейнер (``zip``/``7z``/``rar``) — маршрут спуска, а не экстрактора."""
        return self.artifact.is_container

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимая запись провенанса (стабильные ключи)."""
        payload: dict[str, Any] = {
            "product_id": self.product_id,
            "url": self.url,
            **self.artifact.as_dict(),
        }
        payload["container"] = dict(self.container) if self.container is not None else None
        return payload


def probe_artifact(
    path: str | Path,
    *,
    product_id: str,
    url: str,
    content_type: str | None = None,
    container_for: Callable[[str | Path], ContainerPort | None] | None = None,
) -> ArtifactProbeRecord:
    """Классифицирует артефакт по файлу; контейнер дополнительно листается по индексу.

    ``container_for`` — инъекция читателя контейнеров (composition root передаёт
    ``container_any.container_for``). ``None`` — читатель не подключён: запись честно
    несёт ``container={"unreadable": "container_reader_unavailable: ..."}``, а не пустой
    «контейнер без членов» (пустота и «не умею читать» — разные факты).
    """
    artifact = classify_artifact(path, url=url, content_type=content_type)
    container: dict[str, Any] | None = None
    if artifact.is_container:
        reader = None
        if container_for is not None:
            try:
                reader = container_for(path)
            except Exception:  # noqa: BLE001 — выбор читателя не должен ронять фазу
                reader = None
        if reader is None:
            container = {
                "format": artifact.format,
                "members_total": 0,
                "solid": None,
                "encrypted": False,
                "truncated": False,
                "unreadable": (
                    "container_reader_unavailable: ContainerPort не инжектирован"
                    if container_for is None
                    else "container_reader_unavailable: формат не распознан читателем"
                ),
            }
        else:
            listing = reader.list_members(str(path))
            container = {
                "format": listing.format,
                "members_total": len(listing.members),
                "solid": listing.solid,
                "encrypted": listing.encrypted,
                "truncated": listing.truncated,
                "unreadable": listing.unreadable,
            }
    return ArtifactProbeRecord(
        product_id=product_id, url=url, artifact=artifact, container=container
    )
