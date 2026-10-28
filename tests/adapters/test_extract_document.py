"""Тесты fail-fast OCR в ``DoclingDocumentExtractor`` (дешёвые, без весов docling).

Баг: ``document_level=full`` обещает OCR, но при несовместимой установке ``rapidocr``
docling отвергает все OCR-кандидаты и молча извлекает текст **без** OCR, помечая его
``full``. Здесь закреплено правило: ``full`` без рабочего OCR-движка — явная ошибка
(:class:`OcrEngineUnavailableError`), а уровни, где OCR выключен осознанно
(``layout``/``layout_tables``/``text``), проверку не выполняют и строятся как прежде.

Docling подменяется фейками: тест не грузит модели и не зависит от ``extract-pdf``.
"""

from __future__ import annotations

import errno
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.adapters import extract_document
from core.adapters.extract_document import (
    DoclingDocumentExtractor,
    OcrEngineUnavailableError,
    _OcrProbeUnsupported,
    _page_count,
    _probe_ocr_engine,
)
from core.domain.enums import DocumentLevel, PdfDevice
from core.util.hashing import content_hash


class _FakeConverter:
    """Минимальная замена ``DocumentConverter`` (только хранение опций)."""

    def __init__(self, format_options: object = None) -> None:
        self.format_options = format_options


def _patch_docling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Подменяет docling-классы фейками (модели не загружаются)."""
    monkeypatch.setattr(extract_document, "DocumentConverter", _FakeConverter)
    monkeypatch.setattr(extract_document, "InputFormat", SimpleNamespace(PDF="pdf"))
    monkeypatch.setattr(
        extract_document, "PdfFormatOption", lambda pipeline_options: ("pdf", pipeline_options)
    )
    monkeypatch.setattr(
        extract_document,
        "NativePdfFormatOption",
        lambda pipeline_options: ("native-pdf", pipeline_options),
    )
    monkeypatch.setattr(DoclingDocumentExtractor, "_pdf_options", lambda self: object())


# --------------------------------------------------------------------------- #
# Fail-fast: full без OCR-движка — ошибка, а не «текст как full»
# --------------------------------------------------------------------------- #


def test_full_without_ocr_engine_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_probe_ocr_engine", lambda pdf_options: None)

    extractor = DoclingDocumentExtractor(DocumentLevel.FULL)
    with pytest.raises(OcrEngineUnavailableError, match="full"):
        extractor._build_converter()


def test_layout_tables_does_not_probe_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться: уровень выключает OCR")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    converter = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)._build_converter()
    assert isinstance(converter, _FakeConverter)


def test_layout_and_text_do_not_probe_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться: уровень выключает OCR")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    for level in (DocumentLevel.LAYOUT, DocumentLevel.TEXT):
        converter = DoclingDocumentExtractor(level)._build_converter()
        assert isinstance(converter, _FakeConverter), level


def test_full_with_ocr_engine_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)
    engine = object()
    monkeypatch.setattr(
        extract_document, "_probe_ocr_engine", lambda pdf_options: engine
    )

    converter = DoclingDocumentExtractor(DocumentLevel.FULL)._build_converter()
    assert isinstance(converter, _FakeConverter)


def test_full_office_build_does_not_probe_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Офисный документ OCR не использует: окружение ``extract`` без OCR работает."""
    _patch_docling(monkeypatch)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться для офисного документа")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    converter = DoclingDocumentExtractor(DocumentLevel.FULL)._build_converter(pdf=False)
    assert isinstance(converter, _FakeConverter)


def test_full_office_extract_does_not_require_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сквозной офисный путь ``full`` не упирается в отсутствие OCR."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться для .docx")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    doc = DoclingDocumentExtractor(DocumentLevel.FULL).extract_bytes(b"office", "spec.docx")
    # Фейковый конвертер не умеет convert — это ожидаемая грациозная деградация,
    # важно, что путь не потребовал OCR.
    assert doc.error is not None


def test_probe_unsupported_degrades_to_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Переименование внутренностей docling — warning, а не падение извлечения."""
    _patch_docling(monkeypatch)

    def _renamed(pdf_options: object) -> object:
        raise _OcrProbeUnsupported("внутренний API переименован")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _renamed)

    converter = DoclingDocumentExtractor(DocumentLevel.FULL)._build_converter()
    assert isinstance(converter, _FakeConverter)
    assert any("проверка пропущена" in record.message for record in caplog.records)


# --------------------------------------------------------------------------- #
# Сам детектор: деградация проверки — не отказ
# --------------------------------------------------------------------------- #


def test_probe_without_ocr_options_is_unsupported() -> None:
    """Опции без ``ocr_options`` (смена внутреннего API) → деградация, не отказ."""
    with pytest.raises(_OcrProbeUnsupported):
        _probe_ocr_engine(object())


# --------------------------------------------------------------------------- #
# queue_max_size — замеренная ручка памяти (execution_change, не methodology)
# --------------------------------------------------------------------------- #


def test_pdf_options_set_queue_max_size_for_pdf_pipelines() -> None:
    """``layout``/``layout_tables``/``full`` несут замеренное ``queue_max_size=1``.

    Тест **вызывает** ``_pdf_options`` (а не мокает её): проверяется реальное поле
    ``PdfPipelineOptions``, которое увидит docling. Дефолт docling 2.135 — 100 страниц
    в полёте (≈3.2 GB peak RSS на 665-стр. документе); 1 даёт −44% при идентичном markdown.
    """
    for level in (
        DocumentLevel.LAYOUT,
        DocumentLevel.LAYOUT_TABLES,
        DocumentLevel.FULL,
    ):
        options = DoclingDocumentExtractor(level)._pdf_options()
        assert options.queue_max_size == 1, level


def test_text_level_has_no_queue_max_size_knob() -> None:
    """Нативный пайплайн (``text``) ручки не имеет — её и не выставляем."""
    options = DoclingDocumentExtractor(DocumentLevel.TEXT)._pdf_options()
    assert not hasattr(options, "queue_max_size")


def test_options_provenance_is_level_aware() -> None:
    """Провенанс ручки: значение для PDF-уровней, ``None`` для ``text``."""
    provenance = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES).options_provenance
    assert provenance["layout"] == 1
    assert provenance["layout_tables"] == 1
    assert provenance["full"] == 1
    assert provenance["text"] is None


# --------------------------------------------------------------------------- #
# P9: устройство моделей объявлено в accelerator_options, а не выведено
# --------------------------------------------------------------------------- #


def test_accelerator_options_carry_declared_device() -> None:
    """``cpu``/``cuda`` доезжают до docling явно: дефолт ``auto`` не используется.

    Проверяется **фактическое** поле ``PdfPipelineOptions.accelerator_options``, которое
    увидит пайплайн: ``device`` — значение оси, ``num_threads`` — :data:`ACCELERATOR_NUM_THREADS`
    (дефолт библиотеки, переданный явно, чтобы значение не приходило из env).
    """
    for level in (
        DocumentLevel.LAYOUT,
        DocumentLevel.LAYOUT_TABLES,
        DocumentLevel.FULL,
    ):
        for device in (PdfDevice.CPU, PdfDevice.CUDA):
            options = DoclingDocumentExtractor(level, pdf_device=device)._pdf_options()
            accelerator = options.accelerator_options
            assert accelerator is not None, (level, device)
            assert accelerator.device == device.value, (level, device)
            assert accelerator.num_threads == extract_document.ACCELERATOR_NUM_THREADS


def test_accelerator_num_threads_is_not_increased_over_default() -> None:
    """Потоки не поднимаются выше дефолта docling: замер запрещает рост на этом стеке.

    Замер ``layout_tables``: 4 потока — 1.145 s/стр., 11 потоков — 1.800 s/стр. Тест
    держит константу на месте: иначе «ускорение GPU» незаметно тянуло бы за собой
    замедление CPU-пути через второй, неизмеренный параметр.
    """
    assert extract_document.ACCELERATOR_NUM_THREADS == 4


def test_text_level_does_not_force_a_device() -> None:
    """Нативный пайплайн моделей не грузит: устройство ему не навязывается.

    Поле ``accelerator_options`` у ``NativePdfPipelineOptions`` существует (общий базовый
    класс), но остаётся **дефолтом docling** (``auto``): моделей, которым нужно
    устройство, у уровня нет, поэтому объявлять там нечего. Если библиотека сменит
    дефолт, тест сломается явно — это сигнал, а не тихая регрессия.
    """
    options = DoclingDocumentExtractor(
        DocumentLevel.TEXT, pdf_device=PdfDevice.CUDA
    )._pdf_options()
    assert options.accelerator_options.device == "auto"
    # Устройство при этом всё равно в ключе кэша: уровень ``text`` тоже объявлен на оси.
    assert "/cuda/" in DoclingDocumentExtractor(
        DocumentLevel.TEXT, pdf_device=PdfDevice.CUDA
    ).version


# --------------------------------------------------------------------------- #
# page_count — счётчик документа, а не конвертированных страниц
# --------------------------------------------------------------------------- #


class _FakeConvertResult:
    """Минимальный ``ConversionResult``: статус, документ и ``input.page_count``."""

    def __init__(self, *, total_pages: int | None, converted_pages: int) -> None:
        self.status = SimpleNamespace(name="SUCCESS")
        self.document = SimpleNamespace(export_to_markdown=lambda: "# текст")
        self.pages = [object() for _ in range(converted_pages)]
        if total_pages is None:
            self.input = None
        else:
            self.input = SimpleNamespace(page_count=total_pages)


class _ConvertOnlyConverter:
    """Конвертер-фейк: возвращает заранее заданный результат (без docling)."""

    def __init__(self, result: object) -> None:
        self._result = result

    def convert(self, path: str) -> object:  # noqa: ARG002 - путь не важен
        return self._result


def test_page_count_prefers_input_page_count() -> None:
    """Полный счётчик берётся из ``result.input`` (не из числа конвертированных страниц).

    Это защита от будущего ``page_range``: ``len(result.pages)`` вернул бы 2, а документ —
    665 страниц, и ``page_count`` перестал бы описывать документ.
    """
    result = _FakeConvertResult(total_pages=665, converted_pages=2)
    assert _page_count(result) == 665


def test_page_count_falls_back_to_len_pages_without_input() -> None:
    """Нет ``result.input`` → fallback на ``len(result.pages)``."""
    result = _FakeConvertResult(total_pages=None, converted_pages=3)
    assert _page_count(result) == 3


def test_page_count_falls_back_when_input_count_unset() -> None:
    """``input.page_count == 0`` — незаполненный дефолт ``InputDocument``, не счётчик."""
    result = _FakeConvertResult(total_pages=0, converted_pages=4)
    assert _page_count(result) == 4


def test_extract_bytes_uses_document_page_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сквозной путь ``extract_bytes`` отдаёт счётчик документа, а не число страниц."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    result = _FakeConvertResult(total_pages=665, converted_pages=1)
    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    extractor._converter = _ConvertOnlyConverter(result)

    doc = extractor.extract_bytes(b"%PDF-1.7", "doc.pdf")

    assert doc.error is None
    assert doc.page_count == 665


# --------------------------------------------------------------------------- #
# Путь к retained-файлу: ``extract_path`` без копии тела vs ``extract_bytes``
# --------------------------------------------------------------------------- #

class _PathReadingConverter:
    """Фейк-конвертер: читает файл по пути и строит markdown из его байтов.

    Оба входа (байты/путь) проверяются на **одних и тех же** байтах: если путь подставлен
    неверно или тело скопировано с потерей, markdown разойдётся. Заодно фиксируются
    свойства подставленного файла — суффикс, симлинк/жёсткая ссылка, inode.
    """

    def __init__(self) -> None:
        self.seen: list[dict[str, object]] = []

    def convert(self, path: str) -> object:
        staged = Path(path)
        data = staged.read_bytes()
        self.seen.append(
            {
                "path": staged,
                "suffix": staged.suffix,
                "is_symlink": staged.is_symlink(),
                "inode": staged.stat().st_ino,
                "data": data,
            }
        )
        markdown = f"# документ\n\n{len(data)} байт: {data[:16].decode('utf-8', 'replace')}"
        return SimpleNamespace(
            status=SimpleNamespace(name="SUCCESS"),
            document=SimpleNamespace(export_to_markdown=lambda: markdown),
            pages=[object()],
            input=SimpleNamespace(page_count=7),
        )

def test_extract_path_matches_extract_bytes_byte_for_byte(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``extract_path`` даёт **тот же** markdown, что ``extract_bytes`` на тех же байтах."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    data = b"PK\x03\x04office-document-bytes"
    source = tmp_path / f"{content_hash(data)}.bin"  # раскладка retention (I19)
    source.write_bytes(data)

    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    converter = _PathReadingConverter()
    extractor._converter = converter

    by_bytes = extractor.extract_bytes(data, "spec.docx")
    by_path = extractor.extract_path(source, filename_hint="spec.docx")

    assert by_bytes.error is None and by_path.error is None
    assert by_path.text == by_bytes.text
    assert by_path.text.encode("utf-8") == by_bytes.text.encode("utf-8")
    assert by_path.content_hash == by_bytes.content_hash
    assert by_path.page_count == by_bytes.page_count == 7
    assert by_path.extractor_version == by_bytes.extractor_version
    assert by_path.extractor_version == (
        "docling-doc-3:layout_tables/cpu/"
        f"docling-{extract_document.docling_library_version()}"
    )
    # Оба пути отдали конвертеру одни и те же байты.
    assert [entry["data"] for entry in converter.seen] == [data, data]

def test_extract_path_stages_by_reference_with_right_suffix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Формат — из ``filename_hint`` (retained-файл ``.bin``), тело — по ссылке, не копией."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    data = b"%PDF-1.7 retained-body"
    source = tmp_path / f"{content_hash(data)}.bin"
    source.write_bytes(data)

    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    converter = _PathReadingConverter()
    extractor._converter = converter

    doc = extractor.extract_path(source, filename_hint="report.pdf")

    assert doc.error is None
    staged = converter.seen[0]
    assert staged["suffix"] == ".pdf"
    if staged["is_symlink"]:
        # tmp и corpus_raw на разных ФС — симлинк: байты всё равно не копировались.
        assert staged["path"].resolve() == source.resolve()
    else:
        # Жёсткая ссылка: тот же inode, ноль скопированных байт.
        assert staged["inode"] == source.stat().st_ino
    # Временный каталог убран: ссылка (и копия, если была) не остаётся на диске.
    assert not staged["path"].exists()

def test_extract_path_without_hint_rejects_bin(tmp_path: Path) -> None:
    """Без ``filename_hint`` retained-файл ``.bin`` — честная ошибка формата, не догадка."""
    source = tmp_path / "abc.bin"
    source.write_bytes(b"%PDF-1.7")

    doc = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES).extract_path(source)

    assert doc.error == "формат не поддерживается: .bin"
    assert doc.text == ""

def test_extract_path_missing_file_is_graceful(tmp_path: Path) -> None:
    """Пропавший retained-файл — ``ExtractedDoc.error``, а не исключение."""
    missing = tmp_path / "gone.bin"

    doc = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES).extract_path(
        missing, filename_hint="spec.docx"
    )

    assert doc.error == f"retained-файл не найден: {missing}"
    assert doc.text == ""
    assert doc.extractor_version == (
        "docling-doc-3:layout_tables/cpu/"
        f"docling-{extract_document.docling_library_version()}"
    )

def test_extract_path_full_pdf_keeps_ocr_guard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Та же OCR-проверка, что у ``extract_bytes``: ``full`` без движка — отказ, не текст."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_probe_ocr_engine", lambda pdf_options: None)
    source = tmp_path / "scan.bin"
    source.write_bytes(b"%PDF-1.7")

    with pytest.raises(OcrEngineUnavailableError, match="full"):
        DoclingDocumentExtractor(DocumentLevel.FULL).extract_path(
            source, filename_hint="scan.pdf"
        )

def test_stage_by_reference_falls_back_link_symlink_copy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Цепочка подстановки: hard link → symlink → копия (порядок зафиксирован)."""
    source = tmp_path / "src.bin"
    source.write_bytes(b"body")
    target = tmp_path / "out" / "document.docx"
    target.parent.mkdir()

    # 1. Штатный путь: жёсткая ссылка (байты не копируются).
    extract_document._stage_by_reference(source, target)
    assert not target.is_symlink()
    assert target.stat().st_ino == source.stat().st_ino
    target.unlink()

    # 2. Разные ФС (EXDEV) → симлинк: тоже без копирования байтов.
    def _no_hardlink(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(extract_document.os, "link", _no_hardlink)
    extract_document._stage_by_reference(source, target)
    assert target.is_symlink()
    assert target.read_bytes() == b"body"
    target.unlink()

    # 3. ФС без ссылок → потоковая копия (последний резерв).
    def _no_symlink(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EPERM, "symlink not permitted")

    monkeypatch.setattr(extract_document.os, "symlink", _no_symlink)
    extract_document._stage_by_reference(source, target)
    assert not target.is_symlink()
    assert target.stat().st_ino != source.stat().st_ino
    assert target.read_bytes() == b"body"


# --------------------------------------------------------------------------- #
# Ленивый импорт docling: импорт адаптера не тянет torch/transformers
# --------------------------------------------------------------------------- #

#: Дочерний процесс: импорт адаптера **не должен** импортировать docling (403 MB RSS).
#: Проверка в отдельном процессе — единственный способ увидеть «до» импорта: в этом
#: процессе docling уже мог быть импортирован другими тестами файла.
_LAZY_IMPORT_SCRIPT = """
import importlib.util, json, sys
import core.adapters.extract_document as m
from core.adapters.extract_document import DoclingDocumentExtractor
print(json.dumps({
    "flag": m._DOCLING_AVAILABLE,
    "converter": m.DocumentConverter,
    "pipelines": m.PdfPipelineOptions,
    "available": DoclingDocumentExtractor().available,
    "has_docling": importlib.util.find_spec("docling") is not None,
    "docling_loaded": "docling" in sys.modules,
    "torch_loaded": "torch" in sys.modules,
}))
"""


def test_importing_adapter_does_not_import_docling() -> None:
    """Импорт адаптера и ``available`` — без docling: 86% корпуса идёт HTML-путём.

    Замер 22.10: ``import core.adapters.extract_document`` стоил 403 MB RSS на воркер
    (torch + transformers), а 1038 из 1203 документов — HTML. Поэтому символы
    разрешаются при первом извлечении (:func:`_resolve_docling`), а не при импорте.
    ``available`` обязан отвечать **без** импорта (его спрашивает инициализатор воркера
    пула в ``build_worker_extractor``), иначе ленивость не даёт ничего.
    """
    import json
    import os
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env.setdefault("PYTHONPATH", str(root))
    proc = subprocess.run(
        [sys.executable, "-c", _LAZY_IMPORT_SCRIPT],
        cwd=str(root),
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr
    seen = json.loads(proc.stdout.strip().splitlines()[-1])

    assert seen["docling_loaded"] is False, "импорт адаптера подтянул docling"
    assert seen["torch_loaded"] is False, "импорт адаптера подтянул torch"
    # Символы ещё не разрешены — ленивый импорт не состоялся.
    assert seen["flag"] is None
    assert seen["converter"] is None
    assert seen["pipelines"] is None
    # Ответ «есть ли чем извлекать» при этом правдив (пакет установлен / не установлен).
    assert seen["available"] is seen["has_docling"]


def test_resolve_docling_uses_injected_symbols_without_importing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Инъекция символа (контракт ``monkeypatch``) отменяет импорт, а не дополняет его.

    Тесты файла подменяют ``DocumentConverter``/``InputFormat``/… фейками; резолвер
    обязан уважать подстановку (шаг 1) и **не** импортировать docling. Проверяется
    отсутствием побочных эффектов: остальные символы остаются ``None``, а
    ``_DOCLING_AVAILABLE`` не переписывается (иначе после ``monkeypatch``-откатa модуль
    остался бы «разрешённым» и следующий тест увидел бы ``available=False``).
    """
    for name in extract_document._DOCLING_SYMBOLS:
        monkeypatch.setattr(extract_document, name, None)
    monkeypatch.setattr(extract_document, "DocumentConverter", _FakeConverter)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", None)

    assert extract_document._resolve_docling() is True

    assert extract_document.DocumentConverter is _FakeConverter
    # Импорта не было: остальные символы не заполнились.
    assert extract_document.PdfPipelineOptions is None
    assert extract_document.InputFormat is None
    # Флаг не переписан инъекцией.
    assert extract_document._DOCLING_AVAILABLE is None


# --------------------------------------------------------------------------- #
# Д-8: extractor_version называет сборку движка (ключ extract_cache)
# --------------------------------------------------------------------------- #


def test_extractor_version_names_level_and_engine_build() -> None:
    """``docling-doc-3:<level>/<device>/docling-<build>``: все три слагаемых видны.

    Все три обязательны: уровень различает сборки на оси ``document_level``, устройство
    — на оси ``pdf_device`` (P9), сборка — версию установленного парсера. Без последней
    прогон мог бы объявить ``docling-2.140`` и переиспользовать текст ``docling-2.135``
    (Д-8); без устройства CUDA-прогон нашёл бы в ``extract_cache`` CPU-текст (Д-8 в
    новом виде).
    """
    build = f"docling-{extract_document.docling_library_version()}"
    for level in (
        DocumentLevel.OFF,
        DocumentLevel.TEXT,
        DocumentLevel.LAYOUT,
        DocumentLevel.LAYOUT_TABLES,
        DocumentLevel.FULL,
    ):
        version = DoclingDocumentExtractor(level).version
        assert version == f"docling-doc-3:{level.value}/cpu/{build}"
        assert version.startswith(f"docling-doc-3:{level.value}/cpu/")
        assert version.endswith(f"/{build}")


def test_extractor_version_differs_by_device() -> None:
    """``cpu`` и ``cuda`` дают **разные** ключи ``extract_cache`` (P9/Д-8).

    Текст документа зависит от устройства (численные различия моделей меняют разметку
    на границах), поэтому один ключ на два устройства вернул бы из кэша текст, которого
    объявленное устройство не производило.
    """
    for level in (DocumentLevel.LAYOUT, DocumentLevel.LAYOUT_TABLES, DocumentLevel.FULL):
        cpu = DoclingDocumentExtractor(level, pdf_device=PdfDevice.CPU).version
        cuda = DoclingDocumentExtractor(level, pdf_device=PdfDevice.CUDA).version
        assert cpu != cuda, level
        assert "/cpu/" in cpu and "/cuda/" in cuda, level
        # Сборка движка у обоих одна: устройство не подменяет версию библиотеки.
        assert cpu.rsplit("/", 1)[-1] == cuda.rsplit("/", 1)[-1]


def test_engine_build_reflects_installed_library_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Сборка — фактическая версия библиотеки, а не константа в коде."""
    monkeypatch.setattr(extract_document, "docling_library_version", lambda: "2.140.1")

    assert extract_document.docling_engine_build() == "docling-2.140.1"
    assert (
        DoclingDocumentExtractor(DocumentLevel.FULL).version
        == "docling-doc-3:full/cpu/docling-2.140.1"
    )


def test_missing_library_does_not_invent_a_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Нет пакета — ``unknown``, а не правдоподобный номер (A3)."""
    monkeypatch.setattr(extract_document, "_DOCLING_DISTRIBUTIONS", ())

    assert extract_document.docling_library_version() == "unknown"
    assert (
        DoclingDocumentExtractor(DocumentLevel.FULL).version
        == "docling-doc-3:full/cpu/docling-unknown"
    )


# --------------------------------------------------------------------------- #
# PARTIAL_SUCCESS: прочитанная часть не выбрасывается, а частичность — видна
# --------------------------------------------------------------------------- #
#
# Дефект (живой случай, ``docs/reports/crawl-trace-2026-10-21``): адаптер считал любой
# статус кроме ``SUCCESS`` ошибкой, а docling при ``PARTIAL_SUCCESS`` отдаёт текст
# сконвертированных страниц. Документ исчезал из корпуса целиком, и ячейка получала
# «материала нет» при существующем доказательстве. Обратная ошибка — принять пустой
# частичный результат за документ — здесь тоже закрыта: без текста это провал.


class _StatusConvertResult:
    """``ConversionResult`` с явным статусом: markdown, страницы и счётчик документа."""

    def __init__(
        self,
        *,
        status: str,
        markdown: str,
        total_pages: int | None,
        converted_pages: int,
    ) -> None:
        self.status = SimpleNamespace(name=status)
        self.document = SimpleNamespace(export_to_markdown=lambda: markdown)
        self.pages = [object() for _ in range(converted_pages)]
        self.input = None if total_pages is None else SimpleNamespace(page_count=total_pages)


class _RaisingConverter:
    """Конвертер-фейк: конвертация бросает (провенанс грациозной деградации)."""

    def convert(self, path: str) -> object:  # noqa: ARG002 - путь не важен
        raise RuntimeError("движок упал")


def _extract_bytes_with(
    monkeypatch: pytest.MonkeyPatch, result: object, level: DocumentLevel
) -> object:
    """``extract_bytes`` на подставленном результате конвертации (без docling)."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    extractor = DoclingDocumentExtractor(level)
    extractor._converter = _ConvertOnlyConverter(result)
    return extractor.extract_bytes(b"%PDF-1.7", "doc.pdf")


def test_partial_success_keeps_text_and_marks_partiality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``PARTIAL_SUCCESS`` с текстом — документ с признаком частичности, **не** ``error``."""
    result = _StatusConvertResult(
        status="PARTIAL_SUCCESS",
        markdown="# часть документа\n\ntext",
        total_pages=665,
        converted_pages=3,
    )

    doc = _extract_bytes_with(monkeypatch, result, DocumentLevel.LAYOUT_TABLES)

    assert doc.error is None, "прочитанная часть документа не может быть ошибкой"
    assert doc.text == "# часть документа\n\ntext"
    assert doc.partial is True
    assert doc.page_count == 665
    assert doc.content_hash == content_hash(doc.text)


def test_partial_success_reason_names_status_pages_and_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Причина частичности читается человеком: статус, объём и **обещанный уровень**.

    ``layout``/``layout_tables``/``full`` обещают структуру целого документа: их частичный
    результат не вправе читаться как «full», не будучи им.
    """
    result = _StatusConvertResult(
        status="PARTIAL_SUCCESS",
        markdown="часть",
        total_pages=665,
        converted_pages=3,
    )

    for level in (DocumentLevel.LAYOUT, DocumentLevel.LAYOUT_TABLES, DocumentLevel.FULL):
        extractor = DoclingDocumentExtractor(level)
        doc = extractor._partial(result, "PARTIAL_SUCCESS")  # noqa: SLF001 - прямой вызов

        assert doc.partial is True
        assert doc.error is None
        assert "PARTIAL_SUCCESS" in doc.partial_reason
        assert "3" in doc.partial_reason and "665" in doc.partial_reason
        assert level.value in doc.partial_reason
        # Структурный уровень: обещание структуры названо, чтобы «часть» не читалась как «full».
        assert "структура" in doc.partial_reason
        # Версия остаётся провенансом движка/оси уровня — объём работы в неё не подмешиваем.
        assert doc.extractor_version == extractor.version


def test_partial_reason_does_not_promise_structure_for_text_level() -> None:
    """``text`` структуры не обещает: причина частичности не приписывает её уровню."""
    result = _StatusConvertResult(
        status="PARTIAL_SUCCESS",
        markdown="часть",
        total_pages=10,
        converted_pages=4,
    )

    doc = DoclingDocumentExtractor(DocumentLevel.TEXT)._partial(  # noqa: SLF001
        result, "PARTIAL_SUCCESS"
    )

    assert doc.partial is True
    assert "структура" not in doc.partial_reason
    assert "уровень text" in doc.partial_reason
    assert "4" in doc.partial_reason and "10" in doc.partial_reason


def test_partial_success_without_text_is_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Частичного успеха без текста не бывает: пустой результат — прежний ``_error``."""
    result = _StatusConvertResult(
        status="PARTIAL_SUCCESS",
        markdown="   \n\t ",
        total_pages=665,
        converted_pages=3,
    )

    doc = _extract_bytes_with(monkeypatch, result, DocumentLevel.LAYOUT_TABLES)

    assert doc.error is not None
    assert "PARTIAL_SUCCESS" in doc.error
    assert doc.text == ""
    assert doc.partial is False
    assert doc.partial_reason is None


def test_failure_status_keeps_previous_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """``FAILURE`` — прежняя строка ошибки (регрессии нет)."""
    result = _StatusConvertResult(
        status="FAILURE",
        markdown="# не должно попасть в корпус",
        total_pages=10,
        converted_pages=10,
    )

    doc = _extract_bytes_with(monkeypatch, result, DocumentLevel.LAYOUT_TABLES)

    assert doc.error == "docling: статус конвертации FAILURE"
    assert doc.text == ""
    assert doc.partial is False


def test_converter_exception_keeps_previous_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Исключение конвертера — ``docling: <причина>``, как прежде."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    extractor._converter = _RaisingConverter()

    doc = extractor.extract_bytes(b"%PDF-1.7", "doc.pdf")

    assert doc.error == "docling: движок упал"
    assert doc.text == ""
    assert doc.partial is False


def test_success_status_stays_non_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    """``SUCCESS`` — прежний документ: ``partial=False``, причина не заполнена."""
    result = _StatusConvertResult(
        status="SUCCESS",
        markdown="# полный",
        total_pages=7,
        converted_pages=7,
    )

    doc = _extract_bytes_with(monkeypatch, result, DocumentLevel.LAYOUT_TABLES)

    assert doc.error is None
    assert doc.partial is False
    assert doc.partial_reason is None
    assert doc.page_count == 7


def test_converted_page_count_is_result_pages_not_document_size() -> None:
    """Объём работы (``len(result.pages)``) и размер документа — разные числа."""
    result = _StatusConvertResult(
        status="PARTIAL_SUCCESS",
        markdown="x",
        total_pages=665,
        converted_pages=3,
    )

    assert extract_document._converted_page_count(result) == 3
    assert _page_count(result) == 665


@pytest.mark.parametrize(
    "kwargs",
    [
        {"text": "", "partial": True},
        {"text": "   ", "partial": True, "partial_reason": "причина"},
        {"text": "текст", "partial": True, "error": "сбой"},
        {"text": "текст", "partial": False, "partial_reason": "причина"},
    ],
)
def test_extracted_doc_partiality_invariant(kwargs: dict[str, object]) -> None:
    """``ExtractedDoc`` не даёт собрать ложный провенанс частичности.

    Четыре невозможных состояния: частичность без текста, частичность с пустым текстом,
    частичность вместе со сбоем и причина частичности без самой частичности.
    """
    from core.ports.types import ExtractedDoc

    with pytest.raises(ValueError):
        ExtractedDoc(
            content_hash=content_hash(""),
            extractor_version="v1",
            **kwargs,  # type: ignore[arg-type]
        )



# --------------------------------------------------------------------------- #
# Реестр форматов: pdf + office (§3.2)
# --------------------------------------------------------------------------- #


def test_supports_registry_covers_pdf_and_office() -> None:
    """``supports`` — тот же реестр, что отдаётся docling: pdf + office, не text/epub.

    PDF и Office извлекает docling, plain text/markdown и EPUB — собственные читатели
    (``extract_text``/``extract_epub``), поэтому ``False`` здесь не отказ формату, а
    указание, что это не его путь. Проверка не грузит docling: ``supports`` — предикат
    по суффиксу.
    """
    for name in ("a.pdf", "a.docx", "a.pptx", "a.xlsx"):
        assert DoclingDocumentExtractor.supports(name) is True, name
    for name in ("a.txt", "a.epub"):
        assert DoclingDocumentExtractor.supports(name) is False, name
