"""Conformance-приёмка markdown-грамматики: блочная структура на наборах CommonMark и GFM.

**Что проверяется.** ``core.adapters.md_ast.parse_markdown`` разбирает markdown в блоки со
спанами. Этот харнесс берёт официальные наборы примеров (CommonMark 0.31.2 ``spec.json`` и
GFM ``test/spec.txt``), выводит из ожидаемого HTML **последовательность блочных видов**
(``heading``/``paragraph``/``blockquote``/``list``/``table``/``fence``/``html``/``math``/
``thematic_break``) простым классификатором и сравнивает её с тем, что отдаёт ``md_ast``.

**Чего это НЕ проверяет.** Здесь **нет сверки HTML-рендеринга**: рендерера у проекта нет, а
``md_ast`` отдаёт блоки, не байты HTML. Классификатор ожидаемого HTML приблизителен: он
узнаёт верхнеуровневые элементы и не различает, например, fenced и indented code (оба дают
``<pre><code>``), поэтому оба вида сводятся к ``fence`` (код-блок). Примеры, где классификатор
не уверен, **исключаются** и печатаются числом с категорией — «0 расхождений» без знаменателя
было бы заявлением, а не измерением.

**Независимо от сравнения** проверяются два свойства разбора:

* **P13** — ``counts["unknown_lines"] == 0``: каждая непустая строка попала в блок;
* **P1-подобное** — каждый непустой (не пробельный) символ исходного текста покрыт спаном
  какого-либо блока верхнего уровня.

**Вход и кэш.** Наборы скачиваются по HTTPS в каталог **вне репозитория**:
``$COHOUND_CONFORMANCE_DIR`` или ``~/.cohound/conformance``. В репозиторий спеки не
вендорятся: у CommonMark лицензия CC-BY-SA. Без кэша и без сети харнесс завершается с
кодом 2 и печатает инструкцию; ``--offline`` запрещает сеть и не превращает отсутствие
данных в «0 примеров».

**Коды возврата.** 0 — прогон состоялся (даже с расхождениями); 2 — данных нет
(нет кэша/сети и ни один набор не прочитался, явно указанный путь не существует).
Если при ``--offline`` доступен только один набор, второй печатается как пропущенный с
причиной, а код остаётся 0: отчёт не выдаёт отсутствие данных за ноль примеров.

**Границы, которые не прячутся.** Сравнение верхнеуровневое: вложенность внутри
``blockquote``/``list``/``table`` им не проверяется. ``reference``/``footnote_label`` из
разбора выбрасываются как невидимые в HTML. Подряд идущие ``html`` схлопываются с обеих
сторон: у сырого HTML границы блоков по финальному выводу не восстановить. Примеры
исключаются с названной категорией: ``html_raw_container`` (сырой HTML-элемент, внутрь
которого браузер вложил отрендеренный markdown), ``html_top_level_text`` (значимый текст
верхнего уровня), ``html_unparsed`` (``HTMLParser`` потерял незакрытый тег),
``parser_block_kind:<вид>`` (разбор дал вид вне канонического словаря). GFM-набор
парсится в формате ``cmark-gfm``: забор из 32 обратных кавычек со словом ``example``,
затем markdown, строка ``.``, ожидаемый HTML и закрывающий забор; ``→`` в нём означает
табуляцию.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Final

# Корень репозитория на sys.path: скрипт запускается как `python scripts/conformance_md.py`,
# и тогда sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT: Final = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.adapters.md_ast import MdDocument, parse_markdown  # noqa: E402

__all__ = [
    "COMMONMARK_URL",
    "GFM_URL",
    "CorpusReport",
    "ExampleOutcome",
    "SpecExample",
    "classify_html_blocks",
    "conformance_dir",
    "document_block_kinds",
    "load_gfm_examples",
    "load_spec_json",
    "main",
    "parse_spec_txt",
    "run_corpus",
    "uncovered_chars",
]

#: CommonMark пинован версией в URL (0.31.2 — та же, что объявляет ``md_ast``).
COMMONMARK_URL: Final = "https://spec.commonmark.org/0.31.2/spec.json"
#: GFM пинован **коммитом**, а не ``master``: набор меняется, и «670 примеров» без пина
#: означали бы разное измерение в разные дни (0.29-спека cmark-gfm на этом коммите).
GFM_COMMIT: Final = "27d942c8b0a62d192f616e5bf3578f4b6a89e180"
GFM_URL: Final = f"https://raw.githubusercontent.com/github/cmark-gfm/{GFM_COMMIT}/test/spec.txt"
COMMONMARK_FILENAME: Final = "commonmark-0.31.2.spec.json"
GFM_FILENAME: Final = "gfm.spec.txt"

#: Ожидаемые sha256 скачанных наборов. Сверяются и при загрузке, и при переиспользовании
#: кэша: если содержимое под пинованным адресом изменилось, это событие цепочки поставки,
#: а не «обновились примеры», и молча мерить другое — нельзя.
EXPECTED_SHA256: Final[dict[str, str]] = {
    COMMONMARK_FILENAME: "d431b29d97b6f73e69d547109cf5081578fac931e72afe95639ebe766c1b2a20",
    GFM_FILENAME: "7d8e5814befec287ac116786d81ff14e0adc9b13295b4494649e995408fd871c",
}

#: Ширина забора в формате cmark-gfm: ровно 32 обратных кавычки.
_SPEC_FENCE: Final = "`" * 32

#: Пустые HTML-элементы (не открывают контейнер).
_VOID_TAGS: Final = frozenset(
    {
        "area", "base", "br", "col", "embed", "hr", "img", "input",
        "link", "meta", "param", "source", "track", "wbr",
    }
)

_HEADING_TAGS: Final = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})

#: Виды блоков ``md_ast`` → канонический вид сравнения.
#:
#: ``code`` (indented code) сводится к ``fence``: ожидаемый HTML у обоих видов одинаков
#: (``<pre><code>…</code></pre>``), и классификатор HTML их не различает. Это объявленная
#: граница метрики, а не подмена проверки.
_PARSER_KIND_TO_CANONICAL: Final[dict[str, str]] = {
    "heading": "heading",
    "paragraph": "paragraph",
    "blockquote": "blockquote",
    "list": "list",
    "table": "table",
    "fence": "fence",
    "code": "fence",
    "html": "html",
    "math": "math",
    "thematic_break": "thematic_break",
}

#: Блоки разбора, невидимые в HTML: определения ссылок и метки сносок.
_SILENT_PARSER_KINDS: Final = frozenset({"reference", "footnote_label"})


# ---------------------------------------------------------------------------
# Наборы примеров
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SpecExample:
    """Один пример: markdown, ожидаемый HTML и место в спецификации."""

    example: int
    markdown: str
    html: str
    section: str = ""
    extensions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExampleOutcome:
    """Результат по одному примеру.

    ``status``: ``match`` — последовательности совпали; ``mismatch`` — обе стороны
    сопоставимы, но последовательности разные; ``excluded`` — классификатор или разбор
    не дал сопоставимой последовательности (``reason`` называет категорию).
    """

    example: int
    section: str
    status: str
    expected: tuple[str, ...]
    actual: tuple[str, ...]
    reason: str
    parsed: bool
    p13_ok: bool
    p1_ok: bool
    markdown: str


@dataclass(slots=True)
class CorpusReport:
    """Итог по одному набору."""

    name: str
    profile: str
    source: str
    outcomes: list[ExampleOutcome]
    disabled: int = 0

    def summary(self) -> dict[str, object]:
        matched = sum(1 for item in self.outcomes if item.status == "match")
        mismatched = [item for item in self.outcomes if item.status == "mismatch"]
        excluded = [item for item in self.outcomes if item.status == "excluded"]
        parsed = [item for item in self.outcomes if item.parsed]
        reasons = Counter(item.reason for item in excluded)
        reason_examples: dict[str, list[int]] = {}
        for item in excluded:
            reason_examples.setdefault(item.reason, []).append(item.example)
        p13 = [item.example for item in parsed if not item.p13_ok]
        p1 = [item.example for item in parsed if not item.p1_ok]
        return {
            "name": self.name,
            "profile": self.profile,
            "source": self.source,
            "total": len(self.outcomes) + self.disabled,
            "loaded": len(self.outcomes),
            "disabled": self.disabled,
            "comparable": matched + len(mismatched),
            "matched": matched,
            "mismatched": len(mismatched),
            "excluded": len(excluded),
            "excluded_reasons": dict(sorted(reasons.items())),
            "excluded_examples": dict(sorted(reason_examples.items())),
            "parsed": len(parsed),
            "p13_violations": p13,
            "p1_violations": p1,
            "mismatches": [
                {
                    "example": item.example,
                    "section": item.section,
                    "expected": list(item.expected),
                    "actual": list(item.actual),
                    "markdown": item.markdown,
                }
                for item in mismatched
            ],
        }


def load_spec_json(path: Path) -> list[SpecExample]:
    """Читает CommonMark ``spec.json``.

    Поддерживаются обе раскладки: список примеров (официальный файл) и объект с ключом
    ``examples``. Отсутствие обязательных полей — ``ValueError``, а не молчаливый пропуск:
    «0 примеров» из-за смены формата выглядело бы как успешный прогон.
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("examples") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        raise ValueError(f"{path}: ожидался список примеров или объект с ключом 'examples'")
    examples: list[SpecExample] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{path}: пример #{index} — не объект")
        markdown = item.get("markdown")
        html = item.get("html")
        if not isinstance(markdown, str) or not isinstance(html, str):
            raise ValueError(f"{path}: пример #{index} без строковых 'markdown'/'html'")
        number = item.get("example", index)
        examples.append(
            SpecExample(
                example=int(number),
                markdown=markdown,
                html=html,
                section=str(item.get("section", "")),
            )
        )
    return examples


def parse_spec_txt(text: str) -> list[SpecExample]:
    """Читает набор в формате ``cmark-gfm`` ``test/spec.txt``.

    Формат (проверено на файле cmark-gfm): строка из 32 обратных кавычек и слова
    ``example``, затем markdown, строка ``.``, затем ожидаемый HTML, затем та же строка
    из 32 кавычек. После ``example`` могут стоять имена расширений (``table``,
    ``strikethrough``, ``disabled``). ``→`` (U+2192) — представление табуляции, как в
    официальном раннере cmark-gfm; он заменяется на ``\\t`` и в markdown, и в HTML.

    Примеры с ``disabled`` пропускаются (``disabled`` в возврате не виден — вызывающий
    получает только активные; их число считает :func:`load_gfm_examples`). Заголовки
    разделов (``# …``/``## …``) запоминаются как контекст ближайшего заголовка.
    """
    examples: list[SpecExample] = []
    section = ""
    state = 0  # 0 — обычный текст, 1 — markdown примера, 2 — ожидаемый HTML
    markdown_lines: list[str] = []
    html_lines: list[str] = []
    extensions: tuple[str, ...] = ()
    number = 0

    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if state == 0:
            if stripped.startswith("#"):
                section = stripped.lstrip("#").strip()
            elif stripped.startswith(_SPEC_FENCE + " example"):
                state = 1
                tail = stripped[len(_SPEC_FENCE) + len(" example") :]
                extensions = tuple(tail.split())
                markdown_lines = []
                html_lines = []
            continue
        if state == 1:
            if stripped == ".":
                state = 2
            else:
                markdown_lines.append(line)
            continue
        # state == 2
        if stripped == _SPEC_FENCE:
            state = 0
            number += 1  # нумерация как в раннере cmark-gfm: отключённые тоже считаются
            if "disabled" not in extensions:
                examples.append(
                    SpecExample(
                        example=number,
                        markdown="".join(markdown_lines).replace("→", "\t"),
                        html="".join(html_lines).replace("→", "\t"),
                        section=section,
                        extensions=extensions,
                    )
                )
            continue
        html_lines.append(line)

    return examples


def load_gfm_examples(path: Path) -> tuple[list[SpecExample], int]:
    """Читает GFM ``spec.txt``; возвращает активные примеры и число отключённых.

    Если формат не распознан (ни одного примера), возвращается пустой список — вызывающий
    обязан показать это как честный skip с причиной, а не как «0 расхождений».
    """
    text = path.read_text(encoding="utf-8")
    examples = parse_spec_txt(text)
    # Число отключённых считаем по маркерам: parse_spec_txt их не возвращает.
    markers = sum(
        1 for line in text.splitlines() if line.strip().startswith(_SPEC_FENCE + " example")
    )
    disabled = max(0, markers - len(examples))
    return examples, disabled


# ---------------------------------------------------------------------------
# Кэш и загрузка
# ---------------------------------------------------------------------------


def conformance_dir() -> Path:
    """Каталог кэша спеков: ``$COHOUND_CONFORMANCE_DIR`` или ``~/.cohound/conformance``."""
    override = os.environ.get("COHOUND_CONFORMANCE_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".cohound" / "conformance"


def _download(url: str, target: Path) -> None:
    """Скачивает ``url`` в ``target`` атомарно (temp-файл + replace)."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 - HTTPS-константа
        data = response.read()
    if not data:
        raise OSError(f"пустой ответ от {url}")
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".download-", delete=False) as handle:
        handle.write(data)
        temp = Path(handle.name)
    temp.replace(target)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_corpus(path: Path) -> str | None:
    """``None`` — файл совпадает с ожидаемым; иначе причина расхождения."""
    expected = EXPECTED_SHA256.get(path.name)
    if expected is None:
        return None
    actual = _sha256(path)
    if actual == expected:
        return None
    return f"{path.name}: sha256 {actual} ≠ ожидаемого {expected}"


def resolve_corpus(path: Path, url: str, *, offline: bool) -> tuple[bool, str]:
    """Готовит файл набора: кэш или загрузка. Возвращает ``(есть_ли, причина)``.

    ``offline`` — сеть запрещена; отсутствие кэша тогда отказ с инструкцией, а не пустой
    набор. Ошибка сети не превращается в исключение: вызывающий покажет причину.
    Скачанный и закэшированный файл сверяется с объявленным sha256: иначе «652 примера»
    описывали бы неизвестно что.
    """
    if path.is_file():
        mismatch = verify_corpus(path)
        if mismatch is not None:
            return False, f"кэш не совпадает с объявленным набором: {mismatch}"
        return True, "cache"
    if offline:
        return False, f"нет кэша {path}; сеть запрещена (--offline)"
    try:
        _download(url, path)
    except Exception as error:  # noqa: BLE001 - отчёт обязан пережить любую сетевую ошибку
        return False, f"не удалось скачать {url}: {error}"
    mismatch = verify_corpus(path)
    if mismatch is not None:
        path.unlink(missing_ok=True)
        return False, f"скачанный набор не совпал с объявленным: {mismatch}"
    return True, "download"


# ---------------------------------------------------------------------------
# Классификатор HTML → блочные виды
# ---------------------------------------------------------------------------


class _HtmlNode:
    """Узел лёгкого дерева HTML (элемент, текст, комментарий/декларация)."""

    __slots__ = ("attrs", "children", "data", "kind", "tag")

    def __init__(self, tag: str, kind: str, attrs: dict[str, str | None] | None = None) -> None:
        self.tag = tag
        self.kind = kind
        self.attrs = attrs or {}
        self.children: list[_HtmlNode] = []
        self.data = ""


class _HtmlTree(HTMLParser):
    """Строит дерево из ожидаемого HTML; терпим к неидеальной разметке."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _HtmlNode("#root", "root")
        self._stack: list[_HtmlNode] = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = _HtmlNode(tag, "element", dict(attrs))
        self._stack[-1].children.append(node)
        if tag not in _VOID_TAGS:
            self._stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._stack[-1].children.append(_HtmlNode(tag, "element", dict(attrs)))

    def handle_endtag(self, tag: str) -> None:
        for position in range(len(self._stack) - 1, 0, -1):
            if self._stack[position].tag == tag:
                del self._stack[position:]
                return
        # Непарный закрывающий тег в ожидаемом HTML — игнорируется: структура важнее.

    def handle_data(self, data: str) -> None:
        if not data.strip():
            return
        node = _HtmlNode("#text", "text")
        node.data = data
        self._stack[-1].children.append(node)

    def handle_comment(self, data: str) -> None:
        node = _HtmlNode("#comment", "html")
        node.data = data
        self._stack[-1].children.append(node)

    def handle_decl(self, decl: str) -> None:
        node = _HtmlNode("#decl", "html")
        node.data = decl
        self._stack[-1].children.append(node)

    def handle_pi(self, data: str) -> None:
        node = _HtmlNode("#pi", "html")
        node.data = data
        self._stack[-1].children.append(node)

    def unknown_decl(self, data: str) -> None:
        node = _HtmlNode("#cdata", "html")
        node.data = data
        self._stack[-1].children.append(node)


def _has_descendant(node: _HtmlNode, tag: str) -> bool:
    for child in node.children:
        if child.kind == "element" and (child.tag == tag or _has_descendant(child, tag)):
            return True
    return False


#: Теги, которые в ожидаемом HTML обозначают **отрендеренный markdown** внутри сырого
#: HTML-контейнера. Именно они, а не структурные ``tr``/``td``/``li``, сигналят, что
#: границы блоков по финальному HTML уже не восстановить: браузер вложил следующий
#: markdown-блок внутрь незакрытого raw-элемента.
_MARKDOWN_BLOCK_TAGS: Final = frozenset(
    {
        "blockquote", "dl", "h1", "h2", "h3", "h4", "h5", "h6", "hr",
        "ol", "p", "pre", "table", "ul",
    }
)


def _has_markdown_block_descendant(node: _HtmlNode) -> bool:
    for child in node.children:
        if child.kind != "element":
            continue
        if child.tag in _MARKDOWN_BLOCK_TAGS or _has_markdown_block_descendant(child):
            return True
    return False


#: Блочные обёртки, которые встречаются только в сыром HTML (markdown их генерирует как
#: ``html``-блок внутри цитаты). Нужны, чтобы отличить настоящую цитату с raw-блоком
#: внутри от сырого ``<blockquote>`` из HTML-блока.
_RAW_BLOCK_TAGS: Final = frozenset(
    {
        "address", "article", "aside", "center", "details", "dialog", "dir", "div",
        "fieldset", "figcaption", "figure", "footer", "form", "header", "hgroup", "main",
        "menu", "nav", "section", "summary",
    }
)


def _has_raw_block_descendant(node: _HtmlNode) -> bool:
    for child in node.children:
        if child.kind != "element":
            continue
        if child.tag in _RAW_BLOCK_TAGS or _has_raw_block_descendant(child):
            return True
    return False


def _is_blank_element(node: _HtmlNode) -> bool:
    """Элемент без значимого содержимого (пустая цитата ``>`` даёт пустой blockquote)."""
    for child in node.children:
        if child.kind == "element":
            return False
        if child.kind == "text" and child.data.strip():
            return False
    return True


def _classify_element(node: _HtmlNode) -> str:
    """Верхнеуровневый элемент ожидаемого HTML → канонический вид блока.

    Всё, что не совпало с генерируемой CommonMark/GFM структурой, считается ``html``:
    на верхнем уровне ожидаемого вывода такие элементы появляются только из raw
    HTML-блоков. Отдельные уточнения существуют потому, что финальный HTML теряет
    информацию о том, чем элемент был в markdown:

    * ``table`` — таблица GFM только при наличии ``thead`` и ``th``; иначе это сырой
      HTML-``<table>``;
    * ``pre`` — код-блок только без собственных атрибутов и с единственным ``<code>``
      (сгенерированный код выглядит как ``<pre><code class="language-…">``); ``<pre
      language="…">`` из примера HTML-блока — сырой HTML;
    * ``blockquote`` — markdown-цитата только с блочным потомком **или пустая**: цитата
      с содержимым всегда содержит хотя бы абзац/заголовок/список, а сырой
      ``<blockquote>`` из HTML-блока обычно несёт только текст. Пустая ``>`` — тоже
      настоящая цитата, поэтому её не отличить от сырого пустого элемента и она
      остаётся ``blockquote``.
    """
    tag = node.tag
    if tag in _HEADING_TAGS:
        return "heading"
    if tag == "p":
        return "paragraph"
    if tag == "blockquote":
        if (
            _has_markdown_block_descendant(node)
            or _has_raw_block_descendant(node)
            or _is_blank_element(node)
        ):
            return "blockquote"
        return "html"
    if tag in ("ul", "ol"):
        return "list"
    if tag == "table":
        if _has_descendant(node, "thead") and _has_descendant(node, "th"):
            return "table"
        return "html"
    if tag == "pre":
        if node.attrs:
            return "html"
        children = [
            child for child in node.children if not (child.kind == "text" and not child.data.strip())
        ]
        if len(children) == 1 and children[0].kind == "element" and children[0].tag == "code":
            code_class = children[0].attrs.get("class") or ""
            if not children[0].attrs or code_class.startswith("language-"):
                return "fence"
        return "html"
    if tag == "hr":
        return "thematic_break"
    return "html"


def _normalize_malformed_comments(html: str) -> str:
    """Не даёт ``HTMLParser`` проглотить ``<!-->``/``<!--->`` как комментарий.

    По CommonMark это не комментарии, а текст (ожидаемый HTML печатает их дословно).
    ``html.parser`` же начинает комментарий и съедает остаток документа, из-за чего
    теряются последующие блоки. Замена — только для этих двух точных
    последовательностей; настоящие ``<!-- … -->`` не затрагиваются.
    """
    return html.replace("<!--->", "&lt;!---&gt;").replace("<!-->", "&lt;!--&gt;")


def classify_html_blocks(html: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Ожидаемый HTML → ``(виды блоков, причины_неуверенности)``.

    Пустая вторая компонента означает «классификатор уверен», и пример сопоставим.
    Непустая — пример исключается с названной причиной:

    * ``html_top_level_text`` — значимый текст на верхнем уровне (например, ``tagfilter``);
    * ``html_unparsed`` — в HTML был непустой текст, но дерево не дало ни одного узла
      (незакрытый тег, который ``HTMLParser`` буферизует и теряет);
    * ``html_raw_container`` — сырой HTML-элемент с блочными потомками: границы
      markdown-блоков внутри него по финальному HTML не восстановить, сравнение было бы
      сравнением двух разных вещей.
    """
    tree = _HtmlTree()
    tree.feed(_normalize_malformed_comments(html))
    tree.close()
    kinds: list[str] = []
    reasons: list[str] = []
    for child in tree.root.children:
        if child.kind == "text":
            if child.data.strip():
                reasons.append("html_top_level_text")
            continue
        kind = _classify_element(child)
        if kind == "html" and _has_markdown_block_descendant(child):
            reasons.append("html_raw_container")
            continue
        kinds.append(kind)
    if not kinds and not reasons and html.strip():
        reasons.append("html_unparsed")
    return tuple(kinds), tuple(reasons)


def _merge_html_runs(kinds: tuple[str, ...]) -> tuple[str, ...]:
    """Схлопывает подряд идущие ``html``: у сырого HTML границы по выводу не видны.

    Один незакрытый raw-элемент и несколько отдельных raw-блоков дают в финальном HTML
    одну и ту же картину вложенности, поэтому различать их по выводу — значит выдавать
    догадку за измерение. Схлопывание применяется к обеим сторонам симметрично.
    """
    merged: list[str] = []
    for kind in kinds:
        if kind == "html" and merged and merged[-1] == "html":
            continue
        merged.append(kind)
    return tuple(merged)


def document_block_kinds(document: MdDocument) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Верхнеуровневые блоки ``md_ast`` → ``(виды, причины_неподдержки)``.

    ``reference`` и ``footnote_label`` выбрасываются: в HTML они не рендерятся, и
    классификатор их не увидит. Остальные незнакомые виды — причина исключения, а не
    повод считать пример совпавшим.
    """
    kinds: list[str] = []
    reasons: list[str] = []
    for block in document.blocks:
        if block.kind in _SILENT_PARSER_KINDS:
            continue
        canonical = _PARSER_KIND_TO_CANONICAL.get(block.kind)
        if canonical is None:
            reasons.append(f"parser_block_kind:{block.kind}")
        else:
            kinds.append(canonical)
    return tuple(kinds), tuple(reasons)


def uncovered_chars(document: MdDocument) -> int:
    """Число непустых символов, не покрытых ни одним блоком верхнего уровня (P1-свойство)."""
    covered = bytearray(len(document.text))
    for block in document.blocks:
        start = max(0, block.span.start_char)
        end = min(len(document.text), block.span.end_char)
        if end > start:
            covered[start:end] = b"\x01" * (end - start)
    return sum(
        1
        for position, char in enumerate(document.text)
        if not covered[position] and not char.isspace()
    )


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def _evaluate(example: SpecExample, profile: str) -> ExampleOutcome:
    """Один пример: разбор, классификация обеих сторон, свойства P13/P1."""
    try:
        document = parse_markdown(example.markdown, profile)
    except Exception as error:  # noqa: BLE001 - дефект парсера не должен ронять весь прогон
        return ExampleOutcome(
            example=example.example,
            section=example.section,
            status="excluded",
            expected=(),
            actual=(),
            reason=f"parser_exception:{type(error).__name__}",
            parsed=False,
            p13_ok=True,
            p1_ok=True,
            markdown=example.markdown,
        )

    expected, html_reasons = classify_html_blocks(example.html)
    actual, parser_reasons = document_block_kinds(document)
    expected = _merge_html_runs(expected)
    actual = _merge_html_runs(actual)
    p13_ok = int(document.counts.get("unknown_lines", 0)) == 0
    p1_ok = uncovered_chars(document) == 0

    reason = ""
    if html_reasons:
        reason = html_reasons[0]
    elif parser_reasons:
        reason = parser_reasons[0]
    if reason:
        status = "excluded"
    elif expected == actual:
        status = "match"
    else:
        status = "mismatch"

    return ExampleOutcome(
        example=example.example,
        section=example.section,
        status=status,
        expected=expected,
        actual=actual,
        reason=reason,
        parsed=True,
        p13_ok=p13_ok,
        p1_ok=p1_ok,
        markdown=example.markdown,
    )


def run_corpus(
    name: str,
    profile: str,
    source: str,
    examples: list[SpecExample],
    *,
    limit: int = 0,
    disabled: int = 0,
) -> CorpusReport:
    """Прогоняет набор под объявленным профилем. ``limit`` — первые N примеров (для проб)."""
    selected = examples[:limit] if limit > 0 else examples
    # При усечении набора счётчик отключённых примеров относится не к срезу, а ко всему
    # файлу; показывать его рядом с «загружено N» значило бы смешивать две базы.
    effective_disabled = disabled if len(selected) == len(examples) else 0
    outcomes = [_evaluate(example, profile) for example in selected]
    return CorpusReport(
        name=name,
        profile=profile,
        source=source,
        outcomes=outcomes,
        disabled=effective_disabled,
    )


# ---------------------------------------------------------------------------
# Вывод
# ---------------------------------------------------------------------------


def _print_report(report: CorpusReport) -> None:
    summary = report.summary()
    print(f"\n{report.name}  (profile={report.profile})")
    print(f"  источник            : {report.source}")
    print(f"  всего примеров      : {summary['total']}")
    if summary["disabled"]:
        print(f"  отключено в наборе  : {summary['disabled']}")
    print(f"  загружено           : {summary['loaded']}")
    print(f"  сопоставимо         : {summary['comparable']}")
    print(f"  совпало             : {summary['matched']}")
    print(f"  расхождения         : {summary['mismatched']}")
    print(f"  исключено           : {summary['excluded']}")
    examples_by_reason: dict[str, list[int]] = summary["excluded_examples"]  # type: ignore[assignment]
    for reason, count in summary["excluded_reasons"].items():  # type: ignore[union-attr]
        numbers = examples_by_reason.get(reason, [])
        shown = ", ".join(str(number) for number in numbers[:20])
        if len(numbers) > 20:
            shown += ", …"
        print(f"      {reason:<38}: {count}  [{shown}]")
    print(
        f"  P13 (unknown_lines=0): {summary['parsed'] - len(summary['p13_violations'])}"
        f"/{summary['parsed']} ok, нарушений {len(summary['p13_violations'])}"
    )
    print(
        f"  P1 (символы покрыты) : {summary['parsed'] - len(summary['p1_violations'])}"
        f"/{summary['parsed']} ok, нарушений {len(summary['p1_violations'])}"
    )
    p13: list[int] = summary["p13_violations"]  # type: ignore[assignment]
    p1: list[int] = summary["p1_violations"]  # type: ignore[assignment]
    if p13:
        print(f"      P13 примеры: {p13[:20]}{' …' if len(p13) > 20 else ''}")
    if p1:
        print(f"      P1 примеры : {p1[:20]}{' …' if len(p1) > 20 else ''}")
    mismatches: list[dict[str, object]] = summary["mismatches"]  # type: ignore[assignment]
    if mismatches:
        print("  первые расхождения (ожидалось → получено):")
        for item in mismatches[:40]:
            markdown = str(item["markdown"])
            snippet = repr(markdown if len(markdown) <= 120 else markdown[:117] + "…")
            print(
                f"    #{item['example']} [{item['section']}] "
                f"{tuple(item['expected'])} → {tuple(item['actual'])}  {snippet}"
            )
        if len(mismatches) > 40:
            print(f"    … ещё {len(mismatches) - 40} (полный список — в --jsonout)")


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Conformance markdown-грамматики: блочная структура CommonMark/GFM.",
    )
    parser.add_argument("--spec", type=Path, default=None, help="путь к CommonMark spec.json")
    parser.add_argument("--gfm", type=Path, default=None, help="путь к GFM spec.txt")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="не ходить в сеть; отсутствие кэша — отказ (код 2)",
    )
    parser.add_argument("--jsonout", type=Path, default=None, help="записать отчёт в JSON")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="ограничить число примеров на набор (0 — все)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Точка входа. Возвращает код: 0 — прогон состоялся, 2 — данных нет."""
    args = _build_parser().parse_args(argv)
    cache = conformance_dir()

    corpora: list[CorpusReport] = []
    unavailable: list[dict[str, str]] = []
    fatal = False

    # CommonMark: явный путь или кэш. Явно указанный несуществующий путь — отказ.
    if args.spec is not None:
        spec_path = args.spec
        if not spec_path.is_file():
            spec_ok, spec_reason = False, f"нет файла {spec_path}"
            fatal = True
        else:
            spec_ok, spec_reason = True, "path"
    else:
        spec_path = cache / COMMONMARK_FILENAME
        spec_ok, spec_reason = resolve_corpus(spec_path, COMMONMARK_URL, offline=args.offline)

    if spec_ok:
        try:
            examples = load_spec_json(spec_path)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            unavailable.append({"name": "CommonMark", "reason": f"не читается {spec_path}: {error}"})
            fatal = True
        else:
            if not examples:
                unavailable.append({"name": "CommonMark", "reason": f"{spec_path}: примеров нет"})
            else:
                corpora.append(
                    run_corpus(
                        "CommonMark 0.31.2",
                        "commonmark",
                        str(spec_path),
                        examples,
                        limit=args.limit,
                    )
                )
    else:
        unavailable.append({"name": "CommonMark", "reason": spec_reason})
        if args.offline:
            fatal = True

    # GFM: явный путь или кэш; при отсутствии — честный skip, если CommonMark прошёл.
    if args.gfm is not None:
        gfm_path = args.gfm
        if not gfm_path.is_file():
            gfm_ok, gfm_reason = False, f"нет файла {gfm_path}"
            fatal = True
        else:
            gfm_ok, gfm_reason = True, "path"
    else:
        gfm_path = cache / GFM_FILENAME
        gfm_ok, gfm_reason = resolve_corpus(gfm_path, GFM_URL, offline=args.offline)

    if gfm_ok:
        try:
            gfm_examples, gfm_disabled = load_gfm_examples(gfm_path)
        except (OSError, ValueError) as error:
            unavailable.append({"name": "GFM", "reason": f"не читается {gfm_path}: {error}"})
        else:
            if not gfm_examples:
                unavailable.append(
                    {"name": "GFM", "reason": f"{gfm_path}: формат spec.txt не распознан (0 примеров)"}
                )
            else:
                corpora.append(
                    run_corpus(
                        "GFM",
                        "gfm",
                        str(gfm_path),
                        gfm_examples,
                        limit=args.limit,
                        disabled=gfm_disabled,
                    )
                )
    else:
        unavailable.append({"name": "GFM", "reason": gfm_reason})

    if not corpora:
        print("conformance: данных нет — прогон не состоялся.", file=sys.stderr)
        for item in unavailable:
            print(f"  {item['name']}: {item['reason']}", file=sys.stderr)
        print(
            "  инструкция: задайте --spec/--gfm, либо дайте сети скачать в "
            f"{cache} (переменная COHOUND_CONFORMANCE_DIR переопределяет каталог).",
            file=sys.stderr,
        )
        return 2

    print("conformance markdown: сверка БЛОЧНОЙ СТРУКТУРЫ, не HTML-рендеринга")
    print(f"кэш спеков: {cache}")
    for report in corpora:
        _print_report(report)
    for item in unavailable:
        print(f"\nпропущено: {item['name']} — {item['reason']}")
    print(
        "\nграницы: сравнение верхнеуровневое; классификатор HTML приблизителен; "
        "исключённые примеры перечислены категориями, а не спрятаны."
    )

    if args.jsonout is not None:
        payload: dict[str, object] = {
            "tool": "conformance_md",
            "scope": "block-structure-not-html-rendering",
            "cache_dir": str(cache),
            "corpora": [report.summary() for report in corpora],
            "unavailable": unavailable,
        }
        _write_json(args.jsonout, payload)
        print(f"JSON: {args.jsonout}")

    return 2 if fatal else 0


if __name__ == "__main__":
    raise SystemExit(main())