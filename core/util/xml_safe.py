"""coHound — ленивый доступ к ``defusedxml`` (единственное место, знающее про extra).

Разбор XML (фиды, sitemap, канальные документы) идёт через ``defusedxml``: XXE, внешние
сущности и billion-laughs отвергаются, DOCTYPE запрещён (§10). Пакет объявлен
**опциональным** (extra ``safety``), но ``core/ports/poller.py`` типизирует
:class:`~core.util.feed.FeedEntry`, поэтому ``core.util.feed`` попадает в цепочку импорта
CLI. Eager-импорт делал базовую установку нерабочей — ``./hound.py`` падал с
``ModuleNotFoundError: defusedxml``, хотя ``doctor`` печатает ``deps:safety`` как
необязательную зависимость.

Отсюда ленивый доступ: импорт происходит только при реальном разборе XML, а его отсутствие
даёт :class:`XmlSecurityUnavailable` с указанием extra. Тихая деградация (разбор без
защиты) не вариант: незащищённый XML — это не «чуть хуже разобрали», а другой класс входа.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

__all__ = ["XmlSecurityUnavailable", "defused_xml"]


class XmlSecurityUnavailable(RuntimeError):
    """Разбор XML запрошен, а ``defusedxml`` не установлен (extra ``safety``)."""


@lru_cache(maxsize=1)
def defused_xml() -> tuple[Any, type[BaseException]]:
    """Ленивый ``(defusedxml.ElementTree, DefusedXmlException)``.

    ``DefusedXmlException`` отдаётся отдельно: ``defusedxml.ElementTree`` его не
    реэкспортирует (проверено на 0.7), а импортировать его на уровне модуля — значит
    вернуть ту же поломку, от которой модуль и заведён.

    :raises XmlSecurityUnavailable: пакета нет в окружении. Это не ошибка разбора:
        вызывающему нечего разбирать, потому что нечем.
    """
    try:
        import defusedxml.ElementTree as det
        from defusedxml.common import DefusedXmlException
    except ImportError as exc:  # pragma: no cover - зависит от окружения
        raise XmlSecurityUnavailable(
            "разбор XML (фиды/sitemap/канальные документы) требует пакет defusedxml; "
            "установите extra: uv sync --extra safety"
        ) from exc
    return det, DefusedXmlException