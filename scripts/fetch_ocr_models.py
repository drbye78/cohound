#!/usr/bin/env python3
"""Доставка rec-моделей OCR, которых нет в колесе ``rapidocr`` (нейтрален к §1.3).

Зачем прибор. Объявленный алфавит OCR ``document_level=full`` (``cyrillic``,
:data:`core.adapters.extract_document.DOCLING_OCR_LANG`) исполняется **файлом rec-модели**,
и этого файла в колесе ``rapidocr`` нет. Проверено по ``RECORD`` установленного
дистрибутива 3.9.2 (15.11.2026): в колесе три модели —
``PP-OCRv6_det_small.onnx``, ``PP-OCRv6_rec_small.onnx``, ``ch_ppocr_mobile_v2.0_cls_mobile.onnx``.
Кириллическая появляется в ``rapidocr/models/`` только после скачивания
(``rapidocr.utils.download_models``) — то есть на машине, где её когда-то скачали, всё
работает, а на чистой установке ``full`` отказывает ``OcrEngineUnavailableError``.

Цена этой разницы измерена 15.11.2026: при удалённом файле 15 тестов падают на чистом
окружении (``test_extract_document`` 6, ``test_extract_office_real`` 8,
``test_extraction_eval`` 1) — ровно те 15, которыми красный CI после ``65fc169``.
Локально прогон зелёный, потому что файл уже лежал в ``site-packages``.

Что делает прибор. Скачивает **объявленные** модели по адресу и сверяет SHA-256 из
таблицы :data:`MODELS` — пины взяты из ``rapidocr/default_models.yaml`` (v3.9.2), то есть
из источника, который библиотека объявляет сама, а не из памяти. Несовпадение хеша —
отказ (код 1), а не «скачали и поехали»: подменённые веса дали бы другой текст под тем же
``extractor_version``.

Куда кладёт. В каталог ``models`` установленного пакета ``rapidocr`` — туда же, куда пишет
собственный загрузчик библиотеки, и откуда путь резолвит адаптер
(:func:`core.adapters.extract_document._rapidocr_rec_model_path`). ``--dest`` позволяет
указать другой каталог (проверка без записи в ``.venv``).

Запуск::

    .venv/bin/python scripts/fetch_ocr_models.py --check     # только проверить
    .venv/bin/python scripts/fetch_ocr_models.py             # скачать недостающие
    .venv/bin/python scripts/fetch_ocr_models.py --dry-run   # что и куда, без сети

Код выхода: 0 — все объявленные модели на месте с верным хешем; 1 — чего-то нет или хеш
не тот; 2 — не найден пакет ``rapidocr`` (или каталог ``models`` в нём).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ModelSpec:
    """Объявленная модель: имя файла, адрес и ожидаемый SHA-256."""

    name: str
    url: str
    sha256: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "url": self.url, "sha256": self.sha256}


#: Модели, которые обязан уметь исполнить объявленный алфавит OCR.
#:
#: Пины — из ``rapidocr/default_models.yaml`` (v3.9.2), блок ``PP-OCRv5``: тот же файл и тот
#: же хеш, что библиотека скачала бы сама. Сверено на байтах 15.11.2026: SHA-256
#: установленного файла совпал с объявленным в библиотеке.
MODELS: tuple[ModelSpec, ...] = (
    ModelSpec(
        name="cyrillic_PP-OCRv5_rec_mobile.onnx",
        url=(
            "https://www.modelscope.cn/models/RapidAI/RapidOCR/resolve/v3.9.2/"
            "onnx/PP-OCRv5/rec/cyrillic_PP-OCRv5_rec_mobile.onnx"
        ),
        sha256="90f761b4bfcce0c8c561c0cb5c887b0971d3ec01c32164bdf7374a35b0982711",
    ),
)

_CHUNK = 1 << 20


def rapidocr_models_dir() -> Path:
    """Каталог ``models`` установленного ``rapidocr``.

    Резолвится от пакета (``importlib.resources``), а не от ``.venv``: раскладка
    виртуального окружения — свойство хоста, а не измерения.
    """
    import importlib.resources

    try:
        resource = importlib.resources.files("rapidocr") / "models"
    except (ImportError, ModuleNotFoundError) as error:
        raise SystemExit(
            "rapidocr не установлен: модели OCR некуда класть (extra `extract-pdf`)"
        ) from error
    path = Path(str(resource))
    if not path.is_dir():
        raise SystemExit(f"каталог моделей rapidocr не найден: {path}")
    return path


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(_CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def state_of(spec: ModelSpec, dest: Path) -> dict[str, Any]:
    """Состояние одной модели: ``present``, ``sha256`` (фактический), ``ok``."""
    path = dest / spec.name
    if not path.is_file():
        return {"name": spec.name, "path": str(path), "present": False, "ok": False}
    actual = sha256_of(path)
    return {
        "name": spec.name,
        "path": str(path),
        "present": True,
        "sha256": actual,
        "ok": actual == spec.sha256,
    }


def download(spec: ModelSpec, dest: Path) -> dict[str, Any]:
    """Скачивает модель во временный файл, сверяет хеш и кладёт на место атомарно.

    Временный файл в том же каталоге, что и цель: ``os.replace`` через границу файловых
    систем не атомарен, и половина модели на месте целой читалась бы как «модель есть».
    """
    target = dest / spec.name
    # Схема проверяется **до** открытия: `urlopen` принимает и `file:`, и свои схемы, а
    # адрес здесь — объявленная строка, а не ввод пользователя. Пины лежат в `MODELS`.
    if urllib.parse.urlsplit(spec.url).scheme != "https":
        return {
            "name": spec.name,
            "ok": False,
            "error": f"адрес модели обязан быть https: {spec.url}",
        }
    with tempfile.NamedTemporaryFile(dir=dest, prefix=".fetch-", suffix=".part") as handle:
        digest = hashlib.sha256()
        # noqa: S310 — схема проверена строкой выше.
        with urllib.request.urlopen(spec.url, timeout=60) as response:  # noqa: S310
            for block in iter(lambda: response.read(_CHUNK), b""):
                handle.write(block)
                digest.update(block)
        handle.flush()
        actual = digest.hexdigest()
        if actual != spec.sha256:
            return {
                "name": spec.name,
                "ok": False,
                "error": f"sha256 не совпал: получено {actual}, объявлено {spec.sha256}",
            }
        Path(handle.name).replace(target)
    # `NamedTemporaryFile` создаёт файл 0600; модель читает тот же пользователь, но 0644
    # повторяет права остальных файлов колеса и не делает сюрприза при общей установке.
    target.chmod(0o644)
    return {"name": spec.name, "path": str(target), "ok": True, "downloaded": True}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Скачать объявленные rec-модели OCR (их нет в колесе rapidocr)"
    )
    parser.add_argument("--check", action="store_true", help="только проверить, не качать")
    parser.add_argument("--dry-run", action="store_true", help="показать план, без сети")
    parser.add_argument(
        "--dest", default=None, help="каталог моделей (по умолчанию — пакет rapidocr)"
    )
    parser.add_argument("--json", action="store_true", help="машинный отчёт")
    args = parser.parse_args(argv)

    dest = Path(args.dest) if args.dest else rapidocr_models_dir()
    results: list[dict[str, Any]] = []
    for spec in MODELS:
        state = state_of(spec, dest)
        if state["ok"]:
            results.append({**state, "action": "ok"})
            continue
        if args.check or args.dry_run:
            results.append(
                {**state, "action": "would_fetch" if state["present"] else "missing"}
            )
            continue
        try:
            fetched = download(spec, dest)
        except Exception as error:  # noqa: BLE001 — сеть/HTTP: причина обязана быть видна
            fetched = {"name": spec.name, "ok": False, "error": str(error)}
        results.append({**fetched, "action": "fetched" if fetched["ok"] else "failed"})

    failed = [item for item in results if not item["ok"]]
    if args.json:
        payload = {"dest": str(dest), "models": results, "ok": not failed}
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(f"каталог моделей: {dest}")
        for item in results:
            name = item["name"]
            if item["ok"] and item["action"] == "ok":
                print(f"  ok      {name}")
            elif item["ok"]:
                print(f"  скачано {name}")
            elif item["action"] in {"missing", "would_fetch"}:
                print(f"  НЕТ     {name} (нужен: document_level=full)")
            else:
                print(f"  ОТКАЗ   {name}: {item.get('error', 'неизвестная причина')}")
        if failed:
            print(
                "объявленный алфавит OCR неисполним: document_level=full откажет "
                "OcrEngineUnavailableError",
                file=sys.stderr,
            )
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
