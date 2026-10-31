"""Тесты ленивого доступа к ``defusedxml`` (``core/util/xml_safe.py``).

Что доказывается:

* отсутствие опционального пакета ``defusedxml`` (extra ``safety``) **не ломает**
  импорт цепочки CLI — регрессия на реальный дефект: ``core/ports/poller.py`` типизирует
  ``FeedEntry``, поэтому ``core.util.feed`` попадал в любой импорт CLI, а eager-импорт
  ``defusedxml`` ронял базовую установку на ``ModuleNotFoundError``;
* отсутствие пакета **не отключает** защиту тихо: запрос разбора падает
  :class:`XmlSecurityUnavailable`, а не возвращает пустой результат.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from core.util.feed import parse_feed
from core.util.sitemap import parse_sitemap
from core.util.xml_safe import XmlSecurityUnavailable, defused_xml


@pytest.fixture(autouse=True)
def _clear_loader_cache():
    """``defused_xml`` кэширован: тест, блокирующий пакет, обязан видеть чистый кэш."""
    defused_xml.cache_clear()
    yield
    defused_xml.cache_clear()


def _block_defusedxml(monkeypatch: pytest.MonkeyPatch) -> None:
    """Имитация «пакета нет»: ``None`` в ``sys.modules`` даёт ImportError на импорте."""
    monkeypatch.setitem(sys.modules, "defusedxml", None)


def test_loader_returns_element_tree_and_exception_class() -> None:
    """Пакет есть: возвращается сам модуль парсера и класс исключения defusedxml."""
    det, defused_error = defused_xml()
    assert det.fromstring is not None
    assert issubclass(defused_error, ValueError)


def test_loader_raises_actionable_error_when_package_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пакета нет: ошибка называет extra, а не просто «No module named»."""
    _block_defusedxml(monkeypatch)
    with pytest.raises(XmlSecurityUnavailable, match="safety"):
        defused_xml()


def test_parse_feed_reports_missing_package_not_parse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Фиды: отсутствие пакета — не ошибка разбора и не пустой список."""
    _block_defusedxml(monkeypatch)
    with pytest.raises(XmlSecurityUnavailable, match="defusedxml"):
        parse_feed(b"<rss version='2.0'><channel/></rss>")


def test_parse_sitemap_reports_missing_package_not_parse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sitemap: то же самое — отказ видимый и с указанием причины."""
    _block_defusedxml(monkeypatch)
    with pytest.raises(XmlSecurityUnavailable, match="defusedxml"):
        parse_sitemap(b"<urlset/>")


def test_import_chain_does_not_require_optional_package() -> None:
    """Регрессия дефекта: базовая установка импортирует CLI-цепочку без ``defusedxml``.

    Проверяется в отдельном процессе: подмена ``sys.modules`` в текущем процессе уже
    загруженные модули не переоткрывает, а дефект был именно в импорте.
    """
    code = (
        "import sys\n"
        "sys.modules['defusedxml'] = None\n"
        "sys.modules['defusedxml.common'] = None\n"
        "sys.modules['defusedxml.ElementTree'] = None\n"
        "import core.ports, core.util.feed, core.util.sitemap, core.util.xml_safe\n"
        "print('ok')\n"
    )
    done = subprocess.run(  # noqa: S603 - аргументы фиксированы, shell не используется
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "ok"