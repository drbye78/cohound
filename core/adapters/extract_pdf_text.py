"""PDF по **текстовому слою** — второй механизм извлечения PDF (``pdf_pipeline=text_layer``).

**Зачем второй конвейер.** ``docling`` (механизм ``structured``) реконструирует структуру
документа: layout-модель, таблицы, OCR. Это верный путь для фич-матриц и сканов, но его
цена — гигабайты torch и секунды на страницу — платится **на каждом** PDF, включая
документацию, где достаточно текстового слоя. Ось ``crawl.pdf_pipeline``
(:class:`~core.domain.enums.PdfPipeline`) объявляет **механизм**, а не библиотеку; этот
модуль — реализация механизма ``text_layer``: только текст, без моделей.

**Что механизм НЕ делает (честно).** Порядок чтения — тот, что даёт сам файл (поток
содержимого), таблицы не восстанавливаются, сканы (страницы без текстового слоя) не
распознаются. Поэтому ``document_level`` (лестница качества docling) к ``text_layer``
неприменима: у механизма нет уровней, и выдумывать их значило бы объявить несуществующее
поведение (A3).

**Управляющие символы и переводы строк (замер, не предвидение).** Текст pdfium приходит
байт-в-байт из файла, без нормализации: в retained-корпусе (40 PDF) — **31 056** переводов
строк в виде ``\\r\\n``. Абзацная граница чанкера — ``\\n[ \\t]*\\n`` — на ``\\r\\n\\r\\n``
**не совпадает**, то есть без нормализации весь документ становится одним абзацем и
нарезка вырождается в деление по размеру. Поэтому здесь остаётся **только** нормализация
переводов строк (``\\r\\n``→``\\n``, одиночный ``\\r``→``\\n``). Управляющие символы (C0/DEL/
C1, включая ``\\f`` 0x0C) снимает единая граница очистки
:func:`~core.util.sanitize.finalize_extracted_text` — она же нейтрализует каркас
промпт-инъекций; вторая, своя чистка здесь разошлась бы с ней по набору символов и по
порядку. Замер по корпусу: **2** управляющих символа на 15 929 страниц (``0x02`` —
ноль); символ не пробел и не буква, слово распадается на обрывки, а цитата evidence не
находится по нормализованному тексту (25.10.2026,
``2026-10-25-pdf-backend-alternatives.md``).

**Пустая страница — видимый исход, а не пустой документ.** Замер по тому же корпусу:
**182** страницы из 15 929 (**1.14 %**, 25.10.2026, ``2026-10-25-pdf-backend-alternatives.md``)
не несут текстового слоя (сканы, векторная
графика). Механизм ``text_layer`` их не читает, и
молча вернуть пустой текст нельзя: документ «есть», а текста нет — это отказ извлечения
(``error="no_text_layer"``), который фаза переводит в видимую запись, а не тихий ноль в
корпусе.

**Отказ одной страницы не уносит документ (симметрия с ``PARTIAL_SUCCESS``).** Читатель
изолирует страницу: исключение pdfium на странице N не отменяет страницы 1…N−1.
Прочитанный текст остаётся документом, но помечается ``partial=True`` с причиной «страниц
не прочитано N из M» — иначе битая страница в 665-страничном вендорском PDF уничтожала бы
662 прочитанные. Полный отказ (текста нет ни на одной странице) — по-прежнему ``error``:
частичного успеха без текста не бывает. Отказ на уровне файла (``PdfDocument``) не
изолируется — он и есть «документ не читается».

**Ленивый импорт.** ``pypdfium2`` — лёгкий (колесо с pdfium, без torch), но импортируется
всё равно **внутри** методов, как docling: ``importlib.util.find_spec`` для
:attr:`PdfiumDocumentExtractor.available` достаточно, а импорт на уровне модуля привязывал
бы к бинарю pdfium любой процесс, который лишь импортировал composition root.

**Провенанс страниц.** Читатель отдаёт ``page_map`` — постраничные интервалы в координатах
итогового текста (контракт — :attr:`~core.ports.types.ExtractedDoc.page_map`). До этого
границы страниц стирались на ``"\\n\\n".join(...)``, и по чанку нельзя было сказать, из какой
страницы он взят. Карта **точная или отсутствует**: если постраничная очистка не
воспроизводит текст документа (шаблон промпт-инъекции перешагнул границу страницы), карта
— ``None``, потому что выдуманный номер хуже отсутствующего (A3).

Версия: ``pdfium-text-5:<версия pypdfium2>`` (:func:`pdfium_engine_build`). Ступени:
``-1`` → ``-2`` (02.11.2026) — добавлена единая очистка текста до ``content_hash``, то есть
изменился текст, а не только способ чтения; ``-2`` → ``-3`` (09.11.2026, план 26 O24) — в
штамп добавлена **сборка движка**. До этого докстринг обещал защиту от «другой сборки
pdfium», которой не было: апгрейд ``pypdfium2`` давал тот же ключ ``extract_cache``, и
текст, прочитанный другим движком, переиспользовался молча; ``-3`` → ``-4`` (15.11.2026,
план 26 O36) — читатель отдаёт **карту страниц**; ``-4`` → ``-5`` (16.11.2026, план 26
O36 фаза 2) — читатель отдаёт ещё и **блоки** (``ExtractedDoc.blocks``: по блоку на
страницу). Текст от этого не меняется ни на байт, но запись кэша прежнего читателя блоков
не несёт: без смены штампа прогрев молча отдавал бы документ без провенанса страниц.
Штамп входит в ключ
``extract_cache`` (``(content_hash, extractor_version, level)``) и в ``extractor_stack``
(слагаемое ``run_id``), поэтому текст, произведённый другой сборкой pdfium **или другой
очисткой**, не переиспользуется молча.

Нейтрален (§1.3): библиотека упомянута как факт исполнения, не как предметная семантика.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import tempfile
from pathlib import Path

from core.domain.doc_blocks import (
    BLOCK_KIND_PAGE,
    TEXT_SOURCE_READ,
    ExtractedBlock,
)
from core.ports.types import ExtractedDoc
from core.util.document_formats import FAMILY_TEXT, PDF_SUFFIX
from core.util.pdf_limits import (
    char_limit_note,
    current_pdf_limits,
    page_limit_note,
    truncate_text,
)
from core.util.sanitize import finalize_extracted_text

__all__ = [
    "PDFIUM_ADAPTER_FAMILY",
    "PDFIUM_ADAPTER_VERSION",
    "PDFIUM_DISTRIBUTION",
    "PdfiumDocumentExtractor",
    "pdfium_engine_build",
    "pdfium_library_version",
    "strip_control_characters",
]

#: Семейство механизма ``text_layer`` — **префикс** штампа, а не полная версия.
#:
#: Полная версия — :func:`pdfium_engine_build` (``pdfium-text-5:<версия дистрибутива>``):
#: одна и та же формула идёт в ``extractor_version`` (ключ ``extract_cache``/``pb``) и в
#: ``extractor_stack`` (слагаемое ``run_id``), иначе две формулы разошлись бы молча (Д-8).
#:
#: ``-2`` (02.11.2026): текст механизма меняется — добавлена единая очистка
#: (:func:`~core.util.sanitize.finalize_extracted_text`) **до** ``content_hash``.
#: ``-3`` (09.11.2026, план 26 O24): в штамп добавлена **сборка движка**. До этого
#: константа обещала защиту от «другой сборки pdfium», которой не было: апгрейд
#: ``pypdfium2`` давал тот же ключ кэша, и текст, прочитанный другим движком, молча
#: переиспользовался. Смена штампа — однократный промах кэша, а не ошибка.
#: ``-4`` (15.11.2026, план 26 O36): читатель отдаёт **карту страниц**
#: (``ExtractedDoc.page_map``).
#: ``-5`` (16.11.2026, план 26 O36 фаза 2): читатель отдаёт **блоки**
#: (``ExtractedDoc.blocks`` — по блоку ``kind="page"`` на страницу, ``text_source="read"``).
#: В обоих случаях текст не изменился ни на байт — меняется то, что запись кэша теперь
#: **несёт провенанс**. Прогретая запись прежнего штампа ни карты, ни блоков не содержит, и
#: хит молча отдал бы документ без провенанса именно тем документам, которые извлекались
#: чаще всех. Поэтому это ``execution_change`` намеренно: новый ``run_id`` и новый ключ
#: ``extract_cache``.
PDFIUM_ADAPTER_FAMILY = "pdfium-text-5"

#: Прежнее имя полной версии механизма — сохранено как **алиас семейства**.
#:
#: Читатели, которым нужна только «какая это ступень механизма» (докстринги, отчёты),
#: продолжают называть константу; значение полной версии даёт :func:`pdfium_engine_build`.
#: Держать здесь полную строку нельзя: она зависит от установленного дистрибутива, то есть
#: от хоста, а константа модуля — нет.
PDFIUM_ADAPTER_VERSION = PDFIUM_ADAPTER_FAMILY

#: Имя дистрибутива, чья версия входит в штамп (``importlib.metadata``, без импорта).
PDFIUM_DISTRIBUTION = "pypdfium2"

#: Разделитель страниц: пустая строка — граница абзаца для абзацного чанкера
#: (``FamilyChunker`` → ``TextChunker`` при ``family="text"``).
_PAGE_SEPARATOR = "\n\n"


def strip_control_characters(text: str) -> str:
    """Нормализует переводы строк pdfium: ``\\r\\n`` → ``\\n``, одиночный ``\\r`` → ``\\n``.

    **Переводы строк.** Замер по retained-корпусу (40 PDF): **31 056** ``\\r\\n``. Абзацная
    граница чанкера — ``\\n[ \\t]*\\n`` — на ``\\r\\n\\r\\n`` не совпадает (``\\r`` не входит в
    ``[ \\t]``), поэтому без нормализации документ становится одним абзацем и нарезка
    вырождается в деление по размеру. Семантики CR в текстовом слое нет.

    **Управляющие символы здесь не снимаются.** Это одна граница, а не две: удаление
    C0/DEL/C1 (включая ``\\f`` 0x0C) делает
    :func:`~core.util.sanitize.finalize_extracted_text`, которую вызывает
    :meth:`PdfiumDocumentExtractor._convert` до ``content_hash``. Своя подстановка здесь
    означала бы второй, независимо поддерживаемый набор символов: он разошёлся бы с общим
    (``\\f`` попал бы в корпус у одного механизма и не попал у другого), а текст до
    ``content_hash`` очищался бы по-разному в зависимости от механизма.

    Функция названа по прежнему контракту адаптера и остаётся точкой нормализации
    переводов строк для ``text_layer``.
    """
    return text.replace("\r\n", "\n").replace("\r", "\n")


def pdfium_library_version() -> str:
    """Версия установленного дистрибутива ``pypdfium2`` — **единственный** источник (O24).

    Читаются метаданные (``importlib.metadata``), а не ``import pypdfium2``: импорт грузит
    нативный бинарь и недопустим ни на старте прогона, ни в воркере ради строки версии.
    Отсутствие пакета — честный ``"unknown"``: версия не выдумывается (A3), и значение
    попадает в штамп как есть.

    Почему это вообще в штампе. До 09.11.2026 ``PDFIUM_ADAPTER_VERSION`` был константой,
    а докстринг модуля обещал, что текст «другой сборки pdfium» не переиспользуется
    молча. Обещания не было: две версии дистрибутива давали **один** ключ
    ``extract_cache``, и апгрейд движка переиспользовал старый текст под прежним
    провенансом. Штамп без сборки — это не «неточность», а ложная запись о том, чем
    прочитан документ.
    """
    try:
        return importlib.metadata.version(PDFIUM_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"
    except Exception:  # noqa: BLE001 — битые метаданные не роняют резолв версии
        return "unknown"


def pdfium_engine_build() -> str:
    """Сборка механизма: ``pdfium-text-5:<версия дистрибутива>`` (план 26, O24).

    Одна формула на два носителя — ``extractor_version`` документа (ключ
    ``extract_cache``, слагаемое ``pb``) и ``extractor_stack`` (слагаемое ``run_id``):
    две независимые формулы разошлись бы молча (Д-8).
    """
    return f"{PDFIUM_ADAPTER_FAMILY}:{pdfium_library_version()}"


def pdfium_installed() -> bool:
    """Есть ли ``pypdfium2`` в окружении — проверка **без импорта** (``find_spec``)."""
    try:
        return importlib.util.find_spec("pypdfium2") is not None
    except (ImportError, ValueError):  # pragma: no cover — сломанный __spec__
        return False


class PdfiumDocumentExtractor:
    """``ExtractorPort``-совместимый PDF-экстрактор по текстовому слою (без моделей).

    Реализует порт структурно, как и документный адаптер: ``extract_bytes``/
    ``extract_path`` — рабочие входы, обёртка маршрута — :class:`core.cli.wiring._LocalExtractor`
    (та же схема, что у docling).

    :param version: штамп провенанса; по умолчанию :func:`pdfium_engine_build` — то же
        значение, что уходит в ``extractor_stack``. Резолв идёт в конструкторе, а не в
        дефолте аргумента: дефолт вычислился бы при импорте модуля и застыл бы до конца
        процесса, тогда как сборка дистрибутива — свойство хоста.
    """

    supports_source_path = True

    def __init__(self, *, version: str | None = None) -> None:
        self.version = version if version is not None else pdfium_engine_build()

    @property
    def available(self) -> bool:
        """Механизм пригоден — пакет установлен (``find_spec``, без импорта бинаря).

        Свойство, а не метод: так же отвечает документный адаптер, и composition root
        спрашивает ``adapter.available`` единообразно. Импорт pdfium здесь недопустим —
        вопрос задаётся на старте каждого воркера пула.
        """
        return pdfium_installed()

    @staticmethod
    def supports(filename: str) -> bool:
        """Только PDF: механизм ``text_layer`` не извлекает Office (это docling)."""
        return Path(filename).suffix.lower() == PDF_SUFFIX

    def extract_bytes(self, content: bytes, filename: str = "document.pdf") -> ExtractedDoc:
        """Извлекает текст из байтов (pdfium читает путь — байты пишутся во временный файл).

        ``filename`` нужен только для проверки формата. Retained-файл (I19) дешевле
        читать :meth:`extract_path`: тело не копируется второй раз.
        """
        suffix_error = self._precheck(filename)
        if suffix_error is not None:
            return suffix_error
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=PDF_SUFFIX) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            return self._convert(tmp_path)
        except Exception as exc:  # noqa: BLE001 — грациозная деградация адаптера
            return self._error(f"pdfium: {exc}")
        finally:
            if tmp_path is not None:
                Path(tmp_path).unlink(missing_ok=True)

    def extract_path(
        self, path: str | Path, *, filename_hint: str | None = None
    ) -> ExtractedDoc:
        """Извлекает текст из retained-файла (``{sha256}.bin``), не поднимая тело в RAM.

        ``filename_hint`` задаёт формат: имя retained-файла суффикса не несёт, а механизм
        обязан отказать на не-PDF видимо, а не пытаться разобрать docx как PDF.
        """
        source = Path(path)
        suffix_error = self._precheck(filename_hint or source.name)
        if suffix_error is not None:
            return suffix_error
        if not source.is_file():
            # Видимая ошибка, а не падение: файл мог исчезнуть между retention и извлечением.
            return self._error(f"retained-файл не найден: {source}")
        try:
            return self._convert(str(source))
        except Exception as exc:  # noqa: BLE001 — грациозная деградация адаптера
            return self._error(f"pdfium: {exc}")

    # ------------------------------------------------------------------ #
    # Внутреннее
    # ------------------------------------------------------------------ #

    def _precheck(self, filename: str) -> ExtractedDoc | None:
        """Общая проверка входа: ``None`` — можно извлекать, иначе готовый отказ."""
        if Path(filename).suffix.lower() != PDF_SUFFIX:
            return self._error(
                f"формат не поддерживается механизмом text_layer: "
                f"{Path(filename).suffix.lower() or '<без суффикса>'}"
            )
        return None

    def _convert(self, path: str) -> ExtractedDoc:
        """``path`` → текст всех страниц → ``ExtractedDoc`` (``family="text"``).

        ``family`` обязателен: ``FamilyChunker`` выбирает чанкер по семейству, и
        незаполненное семейство молча включило бы markdown-чанкер с нулём заголовков —
        тихая деградация нарезки. Текст без разметки — это ``FAMILY_TEXT`` (абзацы).

        ``page_count`` — число страниц **документа** (``len(pdf)``), а не число страниц с
        текстом: пустые страницы — часть документа, и скрывать их значило бы завышать
        долю прочитанного.

        **Лист без текстового слоя — видимая частичность, а не тихий успех.** Прочитанная
        страница с пустым текстом не попадает в ``failed`` (pdfium не отказал), но и не
        даёт текста; без отдельного счётчика документ «прочитано 7 из 2763» был бы
        неотличим от прочитанного целиком. Поэтому такие страницы считаются, документ
        получает ``partial=True`` с ``pages_without_text_layer`` в причине, а
        ``pages_converted`` вычитает их. Документ, где пусты **все** страницы, остаётся
        ``no_text_layer`` (``partial`` без текста запрещён инвариантом). OCR-fallback здесь
        не вводится: это отдельный замер (план 26, O35), а не следствие видимости.

        **Очистка — до ``content_hash``.** Склеенный текст проходит
        :func:`~core.util.sanitize.finalize_extracted_text` (управляющие символы +
        нейтрализация каркаса промпт-инъекций) — ту же единую границу, что у HTML,
        текстового и docling-путей. Порядок важен: ``content_hash`` обязан считаться по
        тому тексту, который увидит чанкер и цитата (I22), иначе один и тот же документ
        давал бы разные хеши на разных механизмах.

        **D9: лимиты документа (план 26).** ``current_pdf_limits()`` — лимиты задания
        извлечения (``None`` — ось не объявлена, поведение байт-в-байт). Ось страниц
        читает **первые N**: страницы за пределом не открываются вовсе, поэтому лимит
        экономит и время. Ось символов обрезает **готовый** текст (после очистки), поэтому
        ``content_hash`` считается уже по обрезанному тексту — он описывает то, что
        попало в корпус. Оба исхода дают ``partial=True`` и причину с числами.
        """
        import pypdfium2 as pdfium  # локально: см. докстринг модуля

        limits = current_pdf_limits()
        page_limit = None if limits is None else limits.max_pages
        char_limit = None if limits is None else limits.max_extracted_chars

        pdf = pdfium.PdfDocument(path)
        # Пары «номер страницы, сырой текст», а не список строк: pdfium может отказать
        # на отдельной странице, и тогда список **короче** документа — индекс в нём
        # перестаёт быть номером страницы (отказ на второй из трёх дал бы «2» третьей).
        # Номер берётся в момент сбора, а не из позиции в списке.
        pages: list[tuple[int, str]] = []
        failed: list[int] = []
        # Страница **прочитана**, но текстового слоя в ней нет (``get_text_range()``
        # вернул пустую строку). Это не отказ pdfium и не отсутствие страницы: лист
        # сканирован. Считать такие страницы обязательно: пустая строка в склейке
        # неотличима от отсутствующей, поэтому без счётчика «прочитано 7 из 2763»
        # выглядело бы как «прочитано всё», а молчание страницы ниже по потоку
        # превратилось бы в «вендор не заявил» → ``❌`` (I2/I2c).
        pages_without_text_layer: list[int] = []
        try:
            page_count = len(pdf)
            # D9: лимит страниц — читаются **первые N**. Страницы за пределом не
            # открываются: лимит обязан экономить работу, а не только объём текста.
            read_pages = (
                page_count
                if page_limit is None or int(page_limit) >= page_count
                else int(page_limit)
            )
            for index in range(read_pages):
                try:
                    page = pdf[index]
                    try:
                        textpage = page.get_textpage()
                        try:
                            chunk = textpage.get_text_range()
                        finally:
                            textpage.close()
                    finally:
                        page.close()
                except Exception:  # noqa: BLE001 — страница изолирована, документ живёт
                    # Одна битая страница не должна уносить весь документ: до правки
                    # исключение pdfium на любой странице отдавало пустой ``ExtractedDoc``
                    # с ``error``, то есть прочитанные страницы терялись целиком. Страницы
                    # без текстового слоя сюда не попадают — у них текст пустой, а не отказ.
                    failed.append(index + 1)
                    continue
                pages.append((index + 1, chunk))
                if not chunk.strip():
                    pages_without_text_layer.append(index + 1)
        finally:
            pdf.close()
        page_limit_hit = read_pages < page_count
        # Сырые сегменты страниц: нормализация переводов строк и снятие краевых пробелов —
        # ровно те же шаги, что были до появления карты страниц, поэтому склейка ниже
        # остаётся **байт-в-байт** прежней (текст и ``content_hash`` не меняются).
        raw_segments = [
            (page_no, strip_control_characters(chunk).strip()) for page_no, chunk in pages
        ]
        # Склейка — источник истины для ``text``/``content_hash``: карта страниц ниже
        # строится **из** неё, а не наоборот. Иначе провенанс мог бы переписать текст.
        text = finalize_extracted_text(
            _PAGE_SEPARATOR.join(segment for _, segment in raw_segments)
        ).strip()
        if not text:
            if failed:
                # Страницы не прочитались и текста нет вовсе: это отказ, а не «пустой
                # документ» — причина называет объём отказа, а не маскируется под
                # ``no_text_layer`` (другой факт: страницы читались, но текста в них нет).
                return self._error(
                    f"pdfium: страниц не прочитано {len(failed)} из {page_count} "
                    f"({_page_list(failed)}), текстового слоя нет"
                )
            # Страницы без текстового слоя — 182 из 15 929 (1.14 % якорного PDF-корпуса,
            # замер 25.10.2026, `2026-10-25-pdf-backend-alternatives.md`): молчаливый
            # пустой документ сделал бы их неотличимыми от «прочитали и ничего не нашли».
            return ExtractedDoc(
                text="",
                content_hash=_sha256(""),
                extractor_version=self.version,
                page_count=page_count,
                error="no_text_layer",
                family=FAMILY_TEXT,
            )
        # Карта страниц строится по **нетронутому** тексту: обрез D9 ниже сдвинул бы
        # границы, а сравнение «сегменты == текст» перестало бы быть сравнением с текстом.
        # Провенанс не влияет на текст: он вычисляется из него и молча деградирует в
        # ``None``, если точной карты нет.
        page_map = _build_page_map(raw_segments, text)
        # O36 фаза 2 (план 26): блоки — **постранично**, из той же карты. pdfium знает
        # границы страниц, но не абзацев: выдумывать абзацы (``kind="paragraph"``) значило
        # бы объявить структуру, которой читатель не видел. Текст прочитан слоем как есть,
        # поэтому ``text_source="read"``; ``bbox`` pdfium для страницы не даёт — ``None``.
        blocks = _build_page_blocks(page_map)
        # D9: ось символов обрезает **готовый** текст (после очистки и склейки страниц),
        # поэтому ``content_hash`` ниже считается уже по обрезанному тексту: хеш обязан
        # описывать то, что увидит чанкер и цитата (I22). Отброшенное число едет в причину.
        total_chars = len(text)
        text, dropped_chars = truncate_text(text, char_limit)
        if page_map is not None and len(text) != total_chars:
            # Обрез — после финализации, поэтому сегменты обрезаются по факту, а не по
            # обещанию: иначе ``char_end`` уехал бы за конец опубликованного текста.
            page_map = _clip_page_map(page_map, len(text))
            # Блоки — из той же карты, значит и тот же обрез: блок не вправе адресовать
            # текст, которого в корпусе нет.
            blocks = _build_page_blocks(page_map)
        if failed or pages_without_text_layer or page_limit_hit or dropped_chars:
            # Частичный результат — документ, а не отказ (симметрия с docling
            # ``PARTIAL_SUCCESS``): текст прочитанных страниц обязан попасть в корпус, но
            # частичность видна, чтобы «прочитано 3 из 665» не выдавалось за полный документ.
            # Причины называются раздельно, потому что это разные факты: страница не
            # прочиталась (отказ pdfium), страница прочитана без текстового слоя (скан),
            # документ обрезан объявленным лимитом оператора (D9).
            reasons: list[str] = []
            if failed:
                reasons.append(
                    f"страниц не прочитано {len(failed)} из {page_count} "
                    f"({_page_list(failed)})"
                )
            if pages_without_text_layer:
                reasons.append(
                    f"pages_without_text_layer: {len(pages_without_text_layer)} из "
                    f"{page_count} ({_page_list(pages_without_text_layer)})"
                )
            if page_limit_hit and page_limit is not None:
                reasons.append(page_limit_note(read_pages, page_count, int(page_limit)))
            if dropped_chars and char_limit is not None:
                reasons.append(char_limit_note(dropped_chars, total_chars, int(char_limit)))
            return ExtractedDoc(
                text=text,
                content_hash=_sha256(text),
                extractor_version=self.version,
                page_count=page_count,
                family=FAMILY_TEXT,
                partial=True,
                partial_reason=(
                    "pdfium: " + "; ".join(reasons) + "; текст прочитанных страниц"
                ),
                # Число прочитанных страниц — измеренная величина (документ минус отказы
                # минус листы без слоя), а не оценка: без неё «частично» не отличить от
                # «почти всё». Пустая страница здесь именно вычитается — она не дала
                # текста, и считать её прочитанной значило бы завысить объём. При
                # объявленном лимите страниц вычитается из **прочитанных**, а не из
                # размера документа: страницы за лимитом работой не были.
                pages_converted=(
                    read_pages - len(failed) - len(pages_without_text_layer)
                ),
                page_map=page_map,
                blocks=blocks,
            )
        return ExtractedDoc(
            text=text,
            content_hash=_sha256(text),
            extractor_version=self.version,
            page_count=page_count,
            family=FAMILY_TEXT,
            page_map=page_map,
            blocks=blocks,
        )

    def _error(self, message: str) -> ExtractedDoc:
        """Грациозная деградация: пустой текст, непустой хеш пустой строки, ``error``."""
        return ExtractedDoc(
            text="",
            content_hash=_sha256(""),
            extractor_version=self.version,
            error=message,
            family=FAMILY_TEXT,
        )


def _sha256(text: str) -> str:
    """``content_hash`` документа — sha256 текста (как в соседних адаптерах)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _page_list(failed: list[int], limit: int = 5) -> str:
    """Номера непрочитанных страниц для причины частичности (первые ``limit``).

    Список ограничен: причина — человекочитаемая строка в провенансе, а не дамп 662
    номеров; полный счётчик рядом («N из M») отвечает на вопрос об объёме.
    """
    shown = ", ".join(str(number) for number in failed[:limit])
    if len(failed) > limit:
        shown = f"{shown}, …"
    return shown


def _build_page_map(
    segments: list[tuple[int, str]], text: str
) -> tuple[tuple[int, int, int], ...] | None:
    """Карта страниц в координатах ``text`` — или ``None``, если точной карты не выходит.

    ``segments`` — пары «номер страницы, сегмент» уже после нормализации переводов строк и
    снятия краевых пробелов. Карта строится **постранично**: каждый сегмент финализируется
    сам, сегменты склеиваются тем же :data:`_PAGE_SEPARATOR`, и результат сравнивается с
    ``text`` — текстом, собранным склейкой **до** финализации.

    **Почему сравнение, а не доверие.** :func:`~core.util.sanitize.finalize_extracted_text`
    — не посимвольная замена: ``neutralize_prompt_injection_marks`` знает шаблоны с
    ``DOTALL`` (``<tool_call>…</tool_call>``), которые могут **перешагнуть** границу
    страницы. Тогда финализация по частям и финализация склейки дают разные строки, и
    постраничные смещения перестают относиться к опубликованному тексту. Выдуманный номер
    страницы хуже отсутствующего (A3), поэтому такой документ остаётся без карты — а текст
    остаётся ровно тем, что даёт склейка (она источник истины в обоих случаях).

    **Края.** ``text`` — результат ``.strip()`` склейки, а сегменты уже обрезаны по краям:
    ``.strip()`` снимает только разделители пустых крайних страниц, и эта длина сдвигает
    все смещения — она вычитается явно (``lead``), а не предполагается нулевой.

    **Пустая страница** даёт ``start == end``: она прочитана, текста в ней нет. Разделитель
    между страницами в карту не входит — это разрыв (см. контракт поля).
    """
    finalized = [
        (page_no, finalize_extracted_text(segment)) for page_no, segment in segments
    ]
    joined = _PAGE_SEPARATOR.join(segment for _, segment in finalized)
    if joined.strip() != text:
        # Постраничная очистка не воспроизводит текстовый документ — карты нет.
        return None
    lead = len(joined) - len(joined.lstrip())
    limit = len(text)
    page_map: list[tuple[int, int, int]] = []
    # Позиция сегмента в ``joined``; в координаты ``text`` её переводит вычитание ``lead``
    # (``.strip()`` снял разделители пустых крайних страниц — это те же символы в начале).
    cursor = 0
    for page_no, segment in finalized:
        start = max(0, cursor - lead)
        end = start + len(segment)
        # Обрезка по ``limit`` — только для крайних пустых страниц: их разделители ушли
        # вместе с ``.strip()``, и позиция за концом текста не является позицией.
        page_map.append((page_no, min(start, limit), min(end, limit)))
        cursor += len(segment) + len(_PAGE_SEPARATOR)
    return _drop_leading_bom(page_map, text)


def _drop_leading_bom(
    page_map: list[tuple[int, int, int]], text: str
) -> tuple[tuple[int, int, int], ...]:
    """Сдвигает смещения на −1, если текст начинается с BOM (контракт карты).

    Чанкер снимает ведущий ``\\ufeff`` (:func:`core.adapters.chunker_text._normalize`), а
    :func:`~core.util.sanitize.finalize_extracted_text` — нет. Без сдвига ``char_start``
    чанка указывал бы на символ **перед** нужным, то есть номер страницы уезжал бы на
    единицу на всём документе — молча.
    """
    if not text.startswith("\ufeff"):
        return tuple(page_map)
    return tuple(
        (page_no, max(0, start - 1), max(0, end - 1))
        for page_no, start, end in page_map
    )


def _clip_page_map(
    page_map: tuple[tuple[int, int, int], ...], limit: int
) -> tuple[tuple[int, int, int], ...]:
    """Обрезает карту по опубликованному тексту (D9: ``pdf_max_extracted_chars``).

    Сегмент, начинающийся **за** концом текста, в карту не входит: страница за объявленным
    лимитом не публиковалась. Сегмент, начавшийся внутри, обрезается по ``limit`` —
    ``char_end`` не имеет права выйти за текст, иначе номер страницы перестал бы быть
    проверяемым.
    """
    clipped: list[tuple[int, int, int]] = []
    for page_no, start, end in page_map:
        if start > limit:
            continue
        clipped.append((page_no, start, min(end, limit)))
    return tuple(clipped)


def _build_page_blocks(
    page_map: tuple[tuple[int, int, int], ...] | None,
) -> tuple[ExtractedBlock, ...] | None:
    """Блоки ``kind="page"`` из карты страниц: один блок на страницу (O36 фаза 2).

    **Почему страница, а не абзац.** pdfium знает границы страниц и не знает абзацев:
    текстовый слой приходит непрерывным потоком содержимого, а абзацная граница — это
    эвристика чанкера, а не факт файла. Объявить здесь ``kind="paragraph"`` значило бы
    выдать догадку за структуру (A3): блок с неверными границами выглядит проверяемым, но
    не проверяем.

    **Почему ``text_source="read"``.** Текст страницы прочитан текстовым слоем файла как
    есть — ни одна модель его не собирала. Признак ``reconstructed`` здесь был бы ложью о
    происхождении, а он и заведён, чтобы эти два случая различались.

    **``bbox`` — ``None``.** Рамки блока pdfium для страницы не отдаёт; координаты,
    вычисленные «по полю страницы», были бы догадкой о вёрстке.

    **Пустая страница** (``start == end``) даёт блок нулевой длины: страница прочитана,
    текста в ней нет — это факт, а не разрыв. ``order`` — позиция в порядке чтения, то есть
    номер страницы по порядку; ``page_no`` — настоящий номер из карты (после отказа
    pdfium на странице они расходятся, и именно поэтому оба поля нужны).

    ``page_map is None`` (постраничная очистка не воспроизвела документ) → ``None``:
    блоков без точных границ не бывает.
    """
    if page_map is None:
        return None
    return tuple(
        ExtractedBlock(
            block_id=f"b{index}",
            kind=BLOCK_KIND_PAGE,
            char_start=start,
            char_end=end,
            order=index,
            text_source=TEXT_SOURCE_READ,
            page_no=page_no,
        )
        for index, (page_no, start, end) in enumerate(page_map)
    )
