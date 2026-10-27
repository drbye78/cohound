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

**Дайджест паспортов** (:func:`passport_content_digest`) — отпечаток **авторских байтов**
всех паспортных файлов продукта. Он входит в дайджест профиля пакета-якоря (G1/G2):
паспорт фиксируется на цикл, поэтому его правка обязана менять профиль.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
Допустимые зависимости слоя ``config`` — ``domain``/``config``/``util`` (01 §1).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from core.domain.models import Passport
from core.util.hashing import short_hash

__all__ = [
    "PassportLoadError",
    "latest_passport_path",
    "load_passport",
    "load_passports",
    "passport_content_digest",
    "passport_content_digests",
    "passport_urls",
]

#: Длина дайджеста паспорта (как у прочих дайджестов идентичности, §12.2).
_PASSPORT_DIGEST_LEN = 24

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
        url = raw if _SCHEME_PREFIX.match(raw) else f"https://{raw.strip('/')}/"
        if url not in urls:
            urls.append(url)
    return urls


# ---------------------------------------------------------------------------
# Дайджест авторских байтов паспортов (компонент профиля пакета-якоря, G1/G2)
# ---------------------------------------------------------------------------


def passport_content_digest(
    product_id: str, passports_dir: str | Path
) -> str | None:
    """Дайджест авторских байтов **всех** паспортных файлов продукта (24 hex) или ``None``.

    Учитываются файлы ``YYYY-MM-DD.yaml`` (тот же критерий, что у загрузчика): паспорт
    фиксируется на цикл, поэтому в отпечаток входит **весь** набор, а не только свежий
    файл — иначе правка старой даты прошла бы мимо аттестации. Материал — отсортированный
    по имени список ``(имя файла, sha256 байтов)``; mtime и пути в дайджест не входят
    (переезд каталога или ``touch`` не меняет профиль).

    ``None`` — у продукта нет паспортных файлов. Это **не** «нет компонента»: отсутствие
    паспорта — наблюдаемое состояние профиля, и его появление меняет дайджест.
    """
    product_dir = Path(passports_dir) / str(product_id)
    files = sorted(_passport_files(product_dir), key=lambda path: path.name)
    if not files:
        return None
    payload = [
        {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in files
    ]
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return short_hash(canonical, _PASSPORT_DIGEST_LEN)


def passport_content_digests(
    product_ids: list[str] | tuple[str, ...], passports_dir: str | Path
) -> dict[str, str | None]:
    """Дайджесты паспортов для набора продуктов: ``{product_id: digest | None}``.

    Ключи — в детерминированном (алфавитном) порядке; продукты без паспорта получают
    ``None`` (см. :func:`passport_content_digest`).
    """
    return {
        str(product_id): passport_content_digest(product_id, passports_dir)
        for product_id in sorted({str(item) for item in product_ids})
    }
