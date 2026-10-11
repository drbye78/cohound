"""coHound — общие хелперы веб-разведки (``passport-scout`` / ``package-scout``).

Категорийно-нейтральный (L1, §1.3) внутренний модуль: здесь живут помощники,
одинаковые для разведки паспорта продукта (:mod:`core.pipeline.passport_scout`) и
разведки пакета категории (:mod:`core.pipeline.package_scout`). Модуль не содержит
предметных терминов и **не** импортирует ``core.adapters``/``core.store`` (pipeline-слой).

Публичные имена — без ведущего подчёркивания (это разделяемый контракт); вызывающие
модули импортируют их напрямую, при необходимости сохраняя приватные алиасы для
минимизации диффа. Поведение и сигнатуры — 1:1 относительно прежних локальных копий.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.util.hashing import content_hash


def clean(value: Any) -> str | None:
    """Строка без внешних пробелов или ``None`` (пустая → ``None``)."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def render_pages(page_texts: dict[str, str]) -> str:
    """Рендер страниц: ``[source: url]`` + чистый текст (детерминированный порядок)."""
    if not page_texts:
        return "(нет фрагментов)"
    blocks = [
        f"[source: {url}]\n{page_texts[url]}" for url in sorted(page_texts)
    ]
    return "\n\n".join(blocks)


def join_pages(page_texts: dict[str, str], max_chars: int) -> str:
    """Объединяет страницы в текст ≤ ``max_chars`` (маркеры ``[source: url]``)."""
    if max_chars <= 0:
        return ""
    return render_pages(page_texts)[:max_chars]


def usage_dict(usage: Any, reasoning_tokens: int = 0) -> dict[str, int]:
    """JSON-совместимый ``usage`` (нули при отсутствии) + токены reasoning."""
    if usage is None:
        prompt_tokens = completion_tokens = 0
    else:
        prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
        completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "reasoning_tokens": int(reasoning_tokens or 0),
    }


def fetch_one(
    url: str,
    *,
    crawler: CrawlerPort,
    extractor: ExtractorPort,
    fetched: list[dict[str, Any]],
    page_texts: dict[str, str],
    errors: list[str],
) -> None:
    """Фетчит и извлекает одну страницу; ошибки изолируются (без исключений наружу)."""
    try:
        page = crawler.fetch(url)
    except Exception as exc:  # noqa: BLE001 — сбой фетча изолируется по документу
        errors.append(f"fetch {url}: {type(exc).__name__}: {exc}")
        fetched.append({"url": url, "error": f"{type(exc).__name__}: {exc}"})
        return
    try:
        doc = extractor.extract(page)
    except Exception as exc:  # noqa: BLE001 — сбой экстракции изолируется по документу
        errors.append(f"extract {url}: {type(exc).__name__}: {exc}")
        fetched.append(
            {
                "url": url,
                "content_hash": content_hash(page.content),
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return
    text = doc.text or ""
    page_texts[url] = text
    resolved_url = getattr(page, "url", None) or url
    if resolved_url != url:
        page_texts[resolved_url] = text
    fetched.append(
        {
            "url": url,
            "content_hash": doc.content_hash or content_hash(page.content),
            "extractor_version": doc.extractor_version,
            "error": doc.error,
        }
    )


def url_allowed(url: str, allowed: set[str]) -> bool:
    """Входит ли URL в собранные источники (точное совпадение либо без ``/`` на конце)."""
    if url in allowed:
        return True
    stripped = url.rstrip("/")
    return stripped in allowed or f"{stripped}/" in allowed


def dedupe(items: list[str]) -> list[str]:
    """Убирает повторы, сохраняя порядок (детерминизм sidecar)."""
    seen: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.append(item)
    return seen


def as_date(value: _dt.date | str | None) -> _dt.date:
    """Нормализует дату снимка (``date``/ISO-строка/``None`` → сегодня)."""
    if value is None:
        return _dt.date.today()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value))
