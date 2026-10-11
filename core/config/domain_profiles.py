"""coHound — загрузчик ``domain_profiles.yaml`` (D2, ``03`` §D.2).

Профиль домена описывает **две независимые оси** получения/извлечения контента:

* ``fetch``   — ``http`` | ``browser`` (HTTP-фетч или браузерный рендер JS);
* ``extract`` — ``auto`` | ``html`` | ``document`` (авто-детект по формату | HTML | документ);
* ``wait_for``— опциональное JS-условие готовности для браузерного рендера.

Формат::

    example.com:        {fetch: browser, extract: html}
    parent.test/path:   {fetch: http,    extract: document}

Ключ — ``host`` с опциональным ``/path``; сопоставление и приоритет профилей —
ответственность pipeline (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`), не адаптеров.
Схема строгая (``extra="forbid"``): неизвестные поля (``render``/``index``/``spa``) отвергаются.

Нейтрален к предметной категории (§1.3). Зависимости слоя ``config`` — ``domain``/``config``/
``util``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from core.domain.enums import ExtractionRoute, FetchMode

__all__ = ["DomainProfile", "load_domain_profiles"]


class DomainProfile(BaseModel):
    """Профиль домена: оси ``fetch``/``extract`` + опциональное JS-ожидание (D2)."""

    model_config = ConfigDict(extra="forbid")

    fetch: FetchMode = FetchMode.HTTP
    extract: ExtractionRoute = ExtractionRoute.AUTO
    wait_for: str | None = None


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


def load_domain_profiles(path: str | Path) -> dict[str, DomainProfile]:
    """Загружает ``domain_profiles.yaml`` (D.2) в ``dict[str, DomainProfile]``.

    Корень — YAML-словарь, каждое значение — профиль домена (строгая схема
    :class:`DomainProfile`). Неизвестные поля профиля (напр. старые ``render``/``index``)
    отвергаются (``extra="forbid"``).

    :raises ValueError: нечитаемый/некорректный YAML; корень не словарь; профиль не словарь
        либо не проходит валидацию.
    """
    data = _read_yaml(path)
    if not isinstance(data, dict):
        raise ValueError(
            f"domain_profiles.yaml: ожидался YAML-словарь верхнего уровня, "
            f"получено {type(data).__name__}"
        )
    profiles: dict[str, DomainProfile] = {}
    for key, profile in data.items():
        if not isinstance(profile, dict):
            raise ValueError(
                f"domain_profiles.yaml[{key}]: ожидался словарь профиля, "
                f"получено {type(profile).__name__}"
            )
        try:
            profiles[str(key)] = DomainProfile.model_validate(profile)
        except ValidationError as exc:
            raise ValueError(
                f"domain_profiles.yaml[{key}]: некорректный профиль домена: {exc}"
            ) from exc
    return profiles
