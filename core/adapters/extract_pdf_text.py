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
нарезка вырождается в деление по размеру. Заодно снимаются управляющие символы (по всему
корпусу: **2** на 16 073 страницы, ``0x02`` — ноль): символ управления в индексе не пробел
и не буква, слово распадается на обрывки, а цитата evidence не находится по
нормализованному тексту. Табуляция и ``\\f`` сохраняются — это структура текста, а не
мусор.

**Пустая страница — видимый исход, а не пустой документ.** Замер по тому же корпусу:
**183** страницы из 16 073 (**1.14 %**) не несут текстового слоя (сканы, векторная
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

Версия: ``pdfium-text-1``. Она — единственный штамп провенанса этого механизма и входит в
ключ ``extract_cache`` (``(content_hash, extractor_version, level)``), поэтому текст,
произведённый другой сборкой pdfium, не переиспользуется молча.

Нейтрален (§1.3): библиотека упомянута как факт исполнения, не как предметная семантика.
"""

from __future__ import annotations

import hashlib
import importlib.util
import re
import tempfile
from pathlib import Path

from core.ports.types import ExtractedDoc
from core.util.document_formats import FAMILY_TEXT, PDF_SUFFIX

__all__ = [
    "PDFIUM_ADAPTER_VERSION",
    "PdfiumDocumentExtractor",
    "strip_control_characters",
]

#: Штамп версии механизма ``text_layer`` (провенанс + ключ ``extract_cache``).
PDFIUM_ADAPTER_VERSION = "pdfium-text-1"

#: Управляющие символы (C0, кроме ``\t``/``\n``/``\f``, и ``DEL``). ``\r`` (0x0D) не
#: входит: переводы строк нормализуются к ``\n`` **до** этой подстановки.
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0e-\x1f\x7f]")

#: Разделитель страниц: пустая строка — граница абзаца для абзацного чанкера
#: (``FamilyChunker`` → ``TextChunker`` при ``family="text"``).
_PAGE_SEPARATOR = "\n\n"


def strip_control_characters(text: str) -> str:
    """Нормализует текст pdfium: переводы строк → ``\\n``, управляющие символы удаляются.

    **Переводы строк.** Замер по retained-корпусу (40 PDF): **31 056** ``\\r\\n``. Абзацная
    граница чанкера — ``\\n[ \\t]*\\n`` — на ``\\r\\n\\r\\n`` не совпадает (``\\r`` не входит в
    ``[ \\t]``), поэтому без нормализации документ становится одним абзацем и нарезка
    вырождается в деление по размеру. ``\\r\\n`` → ``\\n``, одиночный ``\\r`` → ``\\n``:
    семантики CR в текстовом слое нет.

    **Управляющие символы.** Замер по тому же корпусу: **2** символа на 16 073 страницы
    (``0x02`` — ноль; в других файлах pdfium может отдать его внутри слова,
    ``ab\\x02cd``). Символ не пробел и не буква, поэтому: (1) слово в индексе
    распадается на обрывки, лексический поиск не находит его целиком; (2) цитата
    evidence не совпадает с нормализованным текстом (I22). Замена на пробел не годится —
    она разрезала бы слово, поэтому символы **удаляются**.

    ``\\f`` (0x0C) сохраняется: это разрыв страницы, структура, а не мусор. ``\\n``/``\\t``
    сохраняются по той же причине.
    """
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return _CONTROL_CHARS.sub("", normalized)


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

    :param version: штамп провенанса; по умолчанию :data:`PDFIUM_ADAPTER_VERSION`.
    """

    supports_source_path = True

    def __init__(self, *, version: str = PDFIUM_ADAPTER_VERSION) -> None:
        self.version = version

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
        """
        import pypdfium2 as pdfium  # локально: см. докстринг модуля

        pdf = pdfium.PdfDocument(path)
        pages: list[str] = []
        failed: list[int] = []
        try:
            page_count = len(pdf)
            for index in range(page_count):
                try:
                    page = pdf[index]
                    try:
                        textpage = page.get_textpage()
                        try:
                            pages.append(textpage.get_text_range())
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
        finally:
            pdf.close()
        text = _PAGE_SEPARATOR.join(
            strip_control_characters(chunk).strip() for chunk in pages
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
            # Страницы без текстового слоя — 183 из 16 073 (1.14% корпуса): молчаливый
            # пустой документ
            # сделал бы их неотличимыми от «прочитали и ничего не нашли».
            return ExtractedDoc(
                text="",
                content_hash=_sha256(""),
                extractor_version=self.version,
                page_count=page_count,
                error="no_text_layer",
                family=FAMILY_TEXT,
            )
        if failed:
            # Частичный результат — документ, а не отказ (симметрия с docling
            # ``PARTIAL_SUCCESS``): текст прочитанных страниц обязан попасть в корпус, но
            # частичность видна, чтобы «прочитано 3 из 665» не выдавалось за полный документ.
            return ExtractedDoc(
                text=text,
                content_hash=_sha256(text),
                extractor_version=self.version,
                page_count=page_count,
                family=FAMILY_TEXT,
                partial=True,
                partial_reason=(
                    f"pdfium: страниц не прочитано {len(failed)} из {page_count} "
                    f"({_page_list(failed)}); текст прочитанных страниц"
                ),
                # Число прочитанных страниц — измеренная величина (документ минус
                # отказы), а не оценка: без неё «частично» не отличить от «почти всё».
                pages_converted=page_count - len(failed),
            )
        return ExtractedDoc(
            text=text,
            content_hash=_sha256(text),
            extractor_version=self.version,
            page_count=page_count,
            family=FAMILY_TEXT,
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
