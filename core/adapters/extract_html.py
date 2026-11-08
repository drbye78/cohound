"""Экстрактор HTML → markdown/text (trafilatura) — реализация ``ExtractorPort`` (W7/T7.2).

Отдельный от документного контура (``extract_document.py``, docling) HTML-адаптер:
формат выводится из ``RawPage`` (url/content-type), реестр поддерживаемых форматов
остаётся единым в ``extract_document.SUPPORTED_FORMATS`` (архитектура 01 §3.5, дрейф
контракта). Санитайзинг — на границе экстракции (T7.5, 02 §C.2): **структура
сохраняется, удаляется только активное** — входной пре-клин
(:func:`core.util.sanitize.strip_active_html`) вырезает ``script``/``style``/``iframe``,
обработчики ``on*`` и опасные URL-схемы, но оставляет ``class``/``id``/``role``, по
которым ``trafilatura`` отличает статью от chrome (решение владельца D1, 02.11.2026).
Извлечённый текст освобождается от управляющих символов и каркаса промпт-инъекций.

До самой библиотеки правится **структура**, а не содержимое: страница без объявленного
основного блока получает ``<main>`` (D2), списки определений ``<dl>/<dt>/<dd>`` с
заголовком внутри разворачиваются в обычные блоки (D3) — иначе ``trafilatura`` печатает
``<item>`` одной строкой и заголовки из ``<dd>`` инлайнятся в абзац. Обе правки —
``execution_change``: текст корпуса меняется, поэтому меняется ``version`` читателя.

Грациозная деградация (как у ``DoclingDocumentExtractor``): отсутствие ``trafilatura``
или ошибка разбора возвращают ``ExtractedDoc`` с пустым ``text`` и заполненным ``error``,
а не исключение. ``trafilatura`` импортируется лениво и является необязательной
зависимостью; для тестов можно внедрить собственный ``extract_fn``. Нейтрален (§1.3).
"""

from __future__ import annotations

import codecs
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.util.hashing import content_hash
from core.util.sanitize import finalize_extracted_text, strip_active_html

_TRAFILATURA_AVAILABLE = False
# ``mypy`` не выражает «модуль либо ``None``» для ``import``: под ``TYPE_CHECKING``
# модуль объявлен типизированным, а отсутствие на рантайме видно по
# ``_TRAFILATURA_AVAILABLE`` и проверке ``trafilatura is None`` в месте использования.
if TYPE_CHECKING:  # pragma: no cover - статическая ветка
    import trafilatura
else:
    try:  # необязательная зависимость
        import trafilatura

        _TRAFILATURA_AVAILABLE = True
    except ImportError:  # pragma: no cover - зависит от окружения
        trafilatura = None

# ``extract_fn``: HTML (строка) → markdown/text или ``None``, если извлечение не удалось.
ExtractFn = Callable[[str], "str | None"]

_CHARSET_RE = re.compile(r"""charset\s*=\s*["']?([A-Za-z0-9._:-]+)""", re.IGNORECASE)

#: Мета-тег целиком: charset ищется **внутри** тега, иначе строка вида
#: ``<p>…charset=…</p>`` в прозе давала бы ложное объявление.
_META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)

#: Сниффинг мета-объявления ограничен началом файла: HTML5 требует объявлять
#: кодировку в первых 1024 байтах, здесь — с запасом 4096.
_META_SNIFF_BYTES = 4096

#: BOM → кодек. Порядок важен: UTF-8 проверяется первым (его BOM не является
#: префиксом UTF-16-ных), UTF-16 LE/BE — как требует §10 (кодировка объявлена самим файлом).
_BOM_CODECS: tuple[tuple[bytes, str], ...] = (
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
)


def _charset_from_content_type(content_type: str | None) -> str | None:
    """Извлекает ``charset`` из заголовка Content-Type (или ``None``)."""
    if not content_type:
        return None
    match = _CHARSET_RE.search(content_type)
    return match.group(1) if match else None


def _bom_charset(content: bytes) -> str | None:
    """Кодек по BOM файла (UTF-8 / UTF-16 LE / UTF-16 BE) или ``None``.

    BOM — самое сильное объявление: он часть самих байтов, а не утверждение
    сервера или разметки. Никакого угадывания: без BOM возвращается ``None``.
    """
    for bom, charset in _BOM_CODECS:
        if content.startswith(bom):
            return charset
    return None


def _charset_from_meta(content: bytes) -> str | None:
    """``charset`` из первых 4096 байт: ``<meta charset=…>`` или ``<meta http-equiv=…>``.

    Сниффинг идёт по **байтам**: начало файла разбирается как latin-1 (однобайтовое
    отображение, объявление кодировки читается и в однобайтовых, и в UTF-8/UTF-16
    документах), затем из найденных мета-тегов извлекается ``charset`` тем же
    :data:`_CHARSET_RE`. Оба объявления (HTML5 ``charset`` и legacy ``http-equiv``)
    содержат подстроку ``charset=``, поэтому разбор один. Если объявления нет —
    ``None`` (кодировка не угадывается).
    """
    head = content[:_META_SNIFF_BYTES].decode("latin-1")
    for tag in _META_TAG_RE.findall(head):
        match = _CHARSET_RE.search(tag)
        if match:
            return match.group(1)
    return None


def _decode_bytes(content: bytes, charset: str | None) -> str:
    """Декодирует байты объявленным кодеком; неизвестный/битый — UTF-8 replace.

    Неизвестное имя кодека (``LookupError``) и невозможность декодировать
    (``UnicodeError``, напр. ``utf-16`` без BOM) — **честный откат**: текст с
    ``errors="replace"``, где потеря видна как U+FFFD, а не исключение и не
    подмена кодировки. BOM, если он был в байтах, снимается с результата.
    """
    if charset:
        try:
            text = content.decode(charset, errors="replace")
        except (LookupError, UnicodeError):
            text = content.decode("utf-8", errors="replace")
    else:
        text = content.decode("utf-8", errors="replace")
    return text.removeprefix("\ufeff")


def _decode_page(page: RawPage) -> str:
    """Декодирует байты страницы: BOM → Content-Type → ``<meta>`` → UTF-8 replace.

    Порядок — от самого сильного объявления к самому слабому: BOM живёт в самих
    байтах, ``Content-Type`` — утверждение сервера, ``<meta charset>`` — утверждение
    самой разметки. Только при отсутствии **всех** объявлений включается UTF-8 с
    ``errors="replace"`` (замена видна в тексте): угадывания кодировки нет.

    Байты берутся из ``page.content``; если их там нет, а задан ``page.source_path``
    (страница несёт только путь к retained-файлу, I19) — файл дочитывается здесь.
    HTML-экстрактор работает по байтам, поэтому это единственное место, где
    retained-файл поднимается в RAM: документный путь читает docling сам
    (``DoclingDocumentExtractor.extract_path``), а HTML-страницы на порядки меньше.
    """
    content = page.content
    if not content and page.source_path:
        content = Path(page.source_path).read_bytes()
    charset = (
        _bom_charset(content)
        or _charset_from_content_type(page.content_type)
        or _charset_from_meta(content)
    )
    return _decode_bytes(content, charset)


def _default_extract(html: str) -> str | None:
    """Основной путь: trafilatura main-content → markdown."""
    if trafilatura is None:  # pragma: no cover - защита от рассинхрона флага
        raise RuntimeError("trafilatura не установлен")
    return trafilatura.extract(
        html,
        output_format="markdown",
        include_comments=False,
        include_tables=True,
    )


#: Семантические маркеры основного блока: страница **сама** объявляет, где её статья.
#: ``article``/``main``/``role``/``itemprop`` — HTML-семантика, ``id`` — распространённая
#: конвенция CMS. Список намеренно узкий: это не попытка воспроизвести ``BODY_XPATH``
#: ``trafilatura`` (её конвенция — её дело), а проверка «страница что-то объявила».
_MAIN_MARKER_XPATH = (
    "//article | //main | //*[@role='main'] | //*[@role='article'] | "
    "//*[@itemprop='articleBody'] | //*[@id='content'] | //*[@id='articleContent']"
)


def _declare_main_region(html: str) -> str:
    """Оборачивает тело в ``<main>``, если страница не объявила основной блок сама.

    Зачем. ``trafilatura`` выделяет статью по контейнеру (``article``/``main``/
    ``role=main``/``#content``…). Если контейнера нет — а так устроен DocBook
    (``div.container > div.doscList > div.refentry > div.refsect1``), — правило не
    срабатывает, извлечение уходит плоским путём: ``<h2>Описание</h2>`` остаётся текстом
    без ATX-маркера, а каркас попадает в текст. Замер 02.11.2026 (40 страниц на host):
    сохранность заголовков `database.diasoft.ru` 0.000 → 0.845, `edu.postgrespro.ru`
    0.024 → 0.939, `tantorlabs.ru` 0.000 → 0.782; хосты, где маркер есть, не изменились
    ни на один знак. Отчёт: `docs/reports/measurements/2026-11-02-html-headings-main-region.md`.

    Это не чистка и не догадка «где статья»: страница **молчит** о своём основном блоке,
    поэтому объявляется тело документа. Если маркер есть, строка возвращается **байт в
    байт** — иначе извлечение на этих страницах изменилось бы от нашей догадки. Ошибка
    разбора — тоже байт в байт: объявление не имеет права быть гейтом извлечения.
    """
    try:
        import lxml.html
    except ImportError:  # pragma: no cover - lxml приходит вместе с trafilatura (extra extract)
        return html
    try:
        tree = lxml.html.document_fromstring(html)
    except Exception:  # noqa: BLE001 - битый HTML: объявление не наша забота
        return html
    if tree.xpath(_MAIN_MARKER_XPATH):
        return html
    body = tree.find("body")
    if body is None or not len(body):
        return html
    main = lxml.html.Element("main")
    for child in list(body):
        main.append(child)
    body.append(main)
    return lxml.html.tostring(tree, encoding="unicode")


#: Списки определений → обычные блоки. Ключи — теги, значения — их замена.
#: ``<dt>`` становится абзацем, **не** заголовком: термин — не раздел документа.
_DEFINITION_TAGS: dict[str, str] = {"dl": "div", "dt": "p", "dd": "div"}

#: Дешёвая проверка «в документе вообще есть список определений» до разбора HTML.
_DEFINITION_LIST_RE = re.compile(r"<dl[\s>/]", re.IGNORECASE)

#: Признак дефекта: заголовок внутри списка определений. Список без заголовков не трогаем
#: (см. «почему только такие» в докстринге функции).
_DEFINITION_HEADING_XPATH = ".//h1|.//h2|.//h3|.//h4|.//h5|.//h6"


def _neutralize_definition_lists(html: str) -> str:
    """Убирает ``<dl>/<dt>/<dd>`` до ``trafilatura``: список определений → обычные блоки.

    Зачем. ``trafilatura`` 2.3.1 в ``htmlprocessing.convert_lists()`` переписывает
    ``dl``/``ol``/``ul`` в свои ``<list>``, а ``dt``/``dd``/``li`` — в ``<item>``
    (``rend`` = ``dt-1``/``dd-1``…), после чего markdown-сериализатор печатает ``<item>``
    **одной строкой**. Всё, что лежит внутри ``<dd>`` — включая ``<div class="note">``
    с ``<h3>Примечание</h3>`` и ``<p>``, — склеивается в строку вида
    ``- текст ### Примечание текст``: заголовок перестаёт быть заголовком, границы
    секций теряются, а ``heading_path`` чанкера несёт целый абзац. Замер 03.11.2026 на
    сохранённой странице postgrespro (``runtime-config-logging``): как есть — 16
    заголовков, самый длинный 966 символов, 5 строк с ``###`` внутри строки; после этой
    функции — 21 заголовок, самый длинный 63 символа, таких строк 0. Отчёт:
    ``docs/reports/measurements/2026-11-03-html-definition-lists.md``.

    Замена — ``dl``→``div``, ``dt``→``p``, ``dd``→``div``: это сохранение структуры
    абзацев, а не догадка об иерархии. Термин остаётся термином-абзацем; вариант
    ``dt``→``h4`` отвергнут — он превращал бы термин в раздел, которого в документе нет.

    **Почему только списки с заголовком** (``_DEFINITION_HEADING_XPATH``). Замер по тому
    же корпусу (2 379 страниц, ``scripts/measure_html_dl.py --policy all``): замена **всех**
    ``<dl>`` задевает 809 документов и у **7** из них роняет счётчик заголовков — суммарно
    **29** строк-заголовков, у всех семи исчезают **все** заголовки (``sql-rollback``
    10 → 0, ``sql-dropcast`` 9 → 0, пять страниц-указателей по 2 → 0). Сверх того она
    приносит **+258 787** символов вместо +406: навигационные ``<dl>`` без заголовков после
    снятия ``<a>`` перестают вырезаться отсевом по плотности ссылок. Эффект по строкам-
    дефектам при этом **тот же** (88 → 59), а замена списков **с** заголовком даёт его на
    46 документах и не теряет ни одного заголовка. Список определений без заголовков не
    несёт дефекта, который здесь чинится, поэтому возвращается **байт в байт**: менять
    документ без причины — это чужое измерение.

    **Почему внутри заменяемых списков снимаются ссылки** (``<a>`` → текст). Пока
    ``<dd>`` был ``<item>``, ``trafilatura`` защищала вложенный ``<p>`` от отсева по
    плотности ссылок (``delete_by_link_density``: ``parent.tag in ("item", "td", "th")``,
    GH #788). После замены защита исчезает, и абзац, целиком состоящий из одной длинной
    ссылки, вырезается как навигация: на странице сокращений postgrespro извлечение
    падало с 6 101 до 2 390 символов (−61 %), а в ``app-psql`` исчезали строки
    ``\\d[S+]``. Ссылки в этих списках и раньше не попадали в markdown (``trafilatura``
    вызывается с ``include_links=False``), поэтому снятие ``<a>`` возвращает текст к
    прежнему виду: замер по корпусу — дельта символов была −2 441 без снятия ссылок и
    +406 со снятием (потеря уходит, остаётся только снятие маркеров ``- ``).

    Контракт функции узкий: (1) документ без ``<dl>`` возвращается **байт в байт**
    (проверка дешёвая, разбор не запускается); (2) повторный вызов идемпотентен —
    после первого прохода ``<dl>`` в документе нет; (3) ошибка разбора не скрывается:
    вызывающий превращает её в ``error`` (грациозная деградация — его забота, здесь
    молчаливая подмена смысла запрещена). Сеть не используется.

    **Граница обещания.** ``<dl>`` **внутри** ``<li>`` этой правкой не чинится: родительский
    ``<li>`` всё равно станет ``<item>`` (его переписывает сам ``trafilatura``), и
    определённый список внутри пункта снова склеится в строку. В выборке 2 379 страниц
    такого случая **с заголовком** не встретилось, поэтому «класс закрыт» было бы
    неверным утверждением; понадобится — лечится отдельно и отдельным замером.

    Это ``execution_change``: текст корпуса меняется, версия читателя —
    ``trafilatura-html-6``.
    """
    if not _DEFINITION_LIST_RE.search(html):
        return html
    import lxml.html

    tree = lxml.html.document_fromstring(html)
    targets = [dl for dl in tree.xpath("//dl") if dl.xpath(_DEFINITION_HEADING_XPATH)]
    if not targets:
        # ``<dl`` встретился без заголовков (или только в тексте/комментарии) — не трогаем.
        return html
    for definition_list in targets:
        for link in definition_list.xpath(".//a"):
            link.drop_tag()
        # Вложенные списки внутри заменяемого тоже разворачиваются: иначе ``convert_lists``
        # найдёт их и снова склеит ``<item>`` в строку.
        for node in definition_list.iter("dl", "dt", "dd"):
            node.tag = _DEFINITION_TAGS[node.tag]
    return lxml.html.tostring(tree, encoding="unicode")


class HtmlExtractor:
    """HTML → markdown (trafilatura). Реализация ``ExtractorPort`` (Ш5).

    Входной пре-клин — :func:`core.util.sanitize.strip_active_html`: структура
    (``class``/``id``/``role``) доходит до ``trafilatura``, поэтому main-content
    отличается от chrome; удаляется только активное. Прежний allow-list
    (:func:`core.util.sanitize.sanitize_html`) снимал структуру и тянул cookie-баннер
    в текст (замер 02.11.2026: 400/400 страниц против 99/400 без чистки).
    """

    version = "trafilatura-html-6"

    #: B3: адаптер читает байты **локального** файла (``page.source_path``), поэтому годится
    #: для членов контейнеров (спуск). Шов ``split`` умеет то же с ``ae0f933``:
    #: ``RemoteExtractor.supports_source_path`` отправляет те же байты в ``extract/submit``.
    supports_source_path = True

    def __init__(self, extract_fn: ExtractFn | None = None, *, sanitize: bool = True) -> None:
        """``extract_fn`` — внедряемая функция извлечения (для тестов/AB).

        Если не задана — используется обёртка над ``trafilatura`` (ленивая
        необязательная зависимость). ``sanitize=False`` отключает **только** пре-клин
        (``strip_active_html``) — нужен для замеров «что даёт пре-клин». Структурные
        правки входа от него не зависят и применяются всегда: ``_declare_main_region``
        (D2) и ``_neutralize_definition_lists`` (D3) стоят вне ``if self._sanitize``,
        иначе замер сравнивал бы не чистку, а два разных разбора. Финальная очистка
        текста (:func:`core.util.sanitize.finalize_extracted_text`) тоже выполняется
        всегда: это граница evidence, а не опция удобства.
        """
        self._extract_fn: ExtractFn = extract_fn or _default_extract
        self._sanitize = sanitize

    @property
    def available(self) -> bool:
        """Доступен ли экстрактор (внедрённая функция или установленная trafilatura)."""
        return self._extract_fn is not _default_extract or _TRAFILATURA_AVAILABLE

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc``. Никогда не бросает: сбой → ``error``.

        ``route`` здесь не влияет на путь: HTML-экстрактор всегда трактует страницу как
        HTML (маршрут ``document``/``auto`` разрешается композитом в composition root).
        """
        try:
            html = _decode_page(page)
        except Exception as exc:  # noqa: BLE001 - грациозная деградация адаптера
            return self._error(f"декодирование: {exc}")

        if self._sanitize:
            try:
                # D1: пре-клин структуры, не allow-list — trafilatura нужны class/id/role.
                html = strip_active_html(html)
            except Exception as exc:  # noqa: BLE001
                return self._error(f"пре-клин HTML: {exc}")

        # D2: страница, не объявившая основной блок, получает его от нас — иначе
        # ``trafilatura`` теряет заголовки (ATX-маркеры) и тянет каркас. Маркированные
        # страницы проходят байт в байт.
        html = _declare_main_region(html)

        # D3: списки определений до ``trafilatura`` — иначе markdown-сериализатор печатает
        # ``<item>`` одной строкой и заголовки из ``<dd>`` инлайнятся в абзац. Как и D2,
        # это структура, а не чистка: в замерах присутствует во всех политиках, иначе
        # колонки сравнивали бы разные входы.
        try:
            html = _neutralize_definition_lists(html)
        except Exception as exc:  # noqa: BLE001 - ошибка разбора → существующий _error-путь
            return self._error(f"структура <dl> HTML: {exc}")

        try:
            extracted = self._extract_fn(html)
        except Exception as exc:  # noqa: BLE001 - любой сбой парсера → error
            return self._error(f"trafilatura: {exc}")

        if extracted is None:
            return self._error("trafilatura: извлечение не дало текста")

        text = self._postprocess(extracted)
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
        )

    @staticmethod
    def _postprocess(text: str) -> str:
        """Чистит управляющие символы и каркас инъекций, не трогая прозу (T7.5).

        Единая граница очистки текста
        (:func:`core.util.sanitize.finalize_extracted_text`) — та же, что у текстового
        и документного читателей: текст, который вернул адаптер, уже нельзя «забыть
        очистить». Вызывается **до** ``content_hash``.
        """
        return finalize_extracted_text(text)

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=content_hash(""),
            extractor_version=self.version,
            error=message,
        )


# Обратная совместимость по стилю именования с документным адаптером.
TrafilaturaHtmlExtractor = HtmlExtractor
