"""coHound — загрузчик паспортов продуктов (``passports/{product_id}/{YYYY-MM-DD}.yaml``, W5).

Закрывает пробел «passport loader»: конвейер получает паспорта для Ш4 (наследование от
``upstream``) и для будущего вывода корпусных URL из ``doc_domains``. Модуль только
**читает с диска и валидирует** YAML в доменную модель :class:`~core.domain.models.Passport`
(§4.3); семантика (G2, relationship evidence, глубина сателлита = 1) — не его задача.

Раскладка (03 §A.1 / §3.3): ``passports/{product_id}/{YYYY-MM-DD}.yaml``. Для продукта
выбирается **самый свежий** паспорт по дате в имени файла: ISO ``YYYY-MM-DD`` сортируется
лексикографически как хронологически, поэтому выбор детерминирован и не зависит от mtime.
При совпадении даты (теоретически невозможно при точном имени файла) побеждает
лексикографически наибольшее имя — стабильный тай-брейк.

**doc_domains → URL-кандидаты.** :func:`passport_urls` выводит только **кандидатные**
базовые URL из ``passport.doc_domains``. Они не фетчатся, не являются evidence и не
проходят I19 (внешний контент становится evidence лишь после corpus_raw → hash → index).
Функция не достраивает пути: схема-less домен получает только ``https://`` и завершающий
``/``; запись со схемой сохраняется как есть.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
Допустимые зависимости слоя ``config`` — ``domain``/``config``/``util`` (01 §1).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from core.domain.models import Passport

__all__ = [
    "PassportLoadError",
    "load_passport",
    "load_passports",
    "latest_passport_path",
    "passport_urls",
]

# Имя паспортного файла — строго ISO-дата: ``YYYY-MM-DD.yaml`` (§4.3). Прочие файлы
# в каталоге продукта игнорируются (не считаются паспортами).
_PASSPORT_FILENAME = re.compile(r"^\d{4}-\d{2}-\d{2}\.yaml$")

# Наличие схемы в записи doc_domains: ``scheme://``. Требуем именно ``://``, чтобы
# не спутать схему с портом (``host:8080``) — doc_domains может нести порт.
_SCHEME_PREFIX = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


class PassportLoadError(ValueError):
    """Ошибка загрузки паспорта: некорректный YAML или несоответствие схеме §4.3.

    Сообщение всегда называет файл, чтобы диагностика не теряла источник.
    """


# ---------------------------------------------------------------------------
# Низкоуровневые помощники
# ---------------------------------------------------------------------------


def _read_yaml(path: Path) -> Any:
    """Читает YAML-файл; ошибки I/O и синтаксиса приводит к :class:`PassportLoadError`."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise PassportLoadError(f"не удалось прочитать паспорт {path}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PassportLoadError(f"некорректный YAML паспорта {path}: {exc}") from exc


def _as_mapping(data: Any, path: Path) -> dict[str, Any]:
    """Проверяет, что документ паспорта — непустой YAML-словарь."""
    if data is None:
        raise PassportLoadError(f"паспорт {path}: пустой документ")
    if not isinstance(data, dict):
        raise PassportLoadError(
            f"паспорт {path}: ожидался YAML-словарь, получено {type(data).__name__}"
        )
    return data


def _passport_files(product_dir: Path) -> list[Path]:
    """Возвращает паспортные файлы каталога продукта, свежайшие — первыми.

    Учитываются только файлы с именем ``YYYY-MM-DD.yaml``. Сортировка по имени
    в обратном порядке: ISO-дата даёт хронологический порядок, а лексикографически
    наибольшее имя — детерминированный тай-брейк.
    """
    if not product_dir.is_dir():
        return []
    files = [
        entry
        for entry in product_dir.iterdir()
        if entry.is_file() and _PASSPORT_FILENAME.match(entry.name)
    ]
    return sorted(files, key=lambda p: p.name, reverse=True)


# ---------------------------------------------------------------------------
# Публичные загрузчики
# ---------------------------------------------------------------------------


def latest_passport_path(product_id: str, passports_dir: str | Path) -> Path | None:
    """Путь к самому свежему паспорту продукта или ``None``.

    Детерминирован: свежесть определяется датой в имени файла (ISO), тай-брейк —
    лексикографически наибольшее имя. ``None``, если каталога продукта нет или в нём
    нет файла вида ``YYYY-MM-DD.yaml``.
    """
    product_dir = Path(passports_dir) / product_id
    files = _passport_files(product_dir)
    return files[0] if files else None


def load_passport(product_id: str, passports_dir: str | Path) -> Passport | None:
    """Загружает самый свежий паспорт продукта (§4.3) или ``None``.

    Читает ``{passports_dir}/{product_id}/*.yaml``, выбирает последний по дате в имени
    файла (тай-брейк — лексикографически наибольшее имя) и валидирует в
    :class:`~core.domain.models.Passport`. ``None``, если каталога продукта нет или в
    нём нет паспортного файла.

    :raises PassportLoadError: некорректный YAML или несоответствие схеме Passport;
        сообщение называет файл.
    """
    path = latest_passport_path(product_id, passports_dir)
    if path is None:
        return None
    data = _as_mapping(_read_yaml(path), path)
    try:
        return Passport.model_validate(data)
    except ValidationError as exc:
        raise PassportLoadError(f"паспорт {path}: несоответствие схеме §4.3: {exc}") from exc


def load_passports(passports_dir: str | Path) -> dict[str, Passport]:
    """Загружает свежайший паспорт каждого продукта из каталога.

    Обходит **подкаталоги** ``passports_dir`` в детерминированном (отсортированном по
    имени) порядке; каждый подкаталог — продукт. Продукты без паспортного файла
    пропускаются. Ключ словаря — имя подкаталога (``product_id``).

    Отсутствующий ``passports_dir`` даёт ``{}`` (безопасный дефолт, как
    ``load_deploy_config``); не-каталожные файлы в корне игнорируются.
    """
    root = Path(passports_dir)
    if not root.is_dir():
        return {}
    result: dict[str, Passport] = {}
    for entry in sorted(root.iterdir(), key=lambda p: p.name):
        if not entry.is_dir():
            continue
        passport = load_passport(entry.name, root)
        if passport is not None:
            result[entry.name] = passport
    return result


# ---------------------------------------------------------------------------
# Вывод URL-кандидатов из doc_domains
# ---------------------------------------------------------------------------


def passport_urls(passport: Passport) -> list[str]:
    """Выводит **кандидатные** базовые URL из ``passport.doc_domains``.

    Это именно кандидаты для последующего обхода: они **не фетчатся**, **не являются
    evidence** и не проходят I19 (внешний контент становится evidence только после
    ``corpus_raw`` → hash → index). Пути не достраиваются: схема-less запись получает
    лишь ``https://`` и завершающий ``/``; запись со схемой сохраняется как есть.
    Порядок — как в ``doc_domains``; дубликаты схлопываются (первое вхождение).
    """
    urls: list[str] = []
    for domain in passport.doc_domains:
        raw = str(domain).strip()
        if not raw:
            continue
        if _SCHEME_PREFIX.match(raw):
            url = raw
        else:
            url = f"https://{raw.strip('/')}/"
        if url not in urls:
            urls.append(url)
    return urls
