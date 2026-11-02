"""Приёмка извлечения HTML: сколько текста страницы попало в корпус, а сколько — каркас.

Инструмент отвечает на три решения, которые до сих пор оценивались одноразовыми
скриптами (и один раз это дало ложный вывод — числа не воспроизводились):

* **D1 — входной санитайз.** ``sanitize_html`` до ``trafilatura`` может снять разметку
  (``class``), от которой зависит выделение основного блока. Сравнение политик —
  ``--extractor current`` против ``--extractor nosanitize`` (и ``allowlist`` — поведение
  до D1) на одних и тех же страницах.
* **D2 — восстановление заголовков.** Сколько ``<h1..h6>`` документа доживает до
  извлечения как ATX-заголовок (``headings_kept``).
* **SPA — объявлять ли ``fetch: browser``.** Страница-оболочка (меню/баннеры) даёт
  высокий ``chrome_share`` при низком ``body_coverage``: извлекается каркас, а не текст.

**Вход** — сохранённая БД прогона (``cohound.db``), читается **только на чтение**:
таблица ``crawl_session_items`` (``url``, ``raw_path``, ``content_type``), тела — по
``raw_path``. Отсутствие сети, записи в БД и изменений в ``core/`` — часть контракта.

**Метрики** (определения зафиксированы: это приёмка, а не исследование):

``body_coverage``
    Доля уникальных токенов основного блока страницы, найденных в тексте извлечения.
    Основной блок — первый из ``article``, ``main``, ``[role=main]``; если их нет —
    самый длинный по тексту ``div``/``section``. ``script``/``style``/``noscript``
    не учитываются. Токен — слово из букв длиной ≥6, регистр не важен.
``chrome_share``
    ``1 −`` (доля уникальных токенов извлечения, найденных в тексте основного блока).
    Высокое значение — извлечение несёт то, чего нет в основном блоке.
``headings_raw`` / ``headings_out`` / ``headings_kept``
    Число ``<h1..h6>`` в документе; число ATX-заголовков в извлечении; доля текстов
    заголовков документа, чей префикс (первые 40 символов, нижний регистр, пробелы
    схлопнуты) встречается в каком-либо ATX-заголовке извлечения.
``out_len``
    Длина текста извлечения в символах.
``error``
    Непустое ``ExtractedDoc.error``; такие страницы считаются отдельно и в медианы
    не входят.

**Чего метрики НЕ измеряют** (важно при чтении чисел):

* ``chrome_share`` не отделяет меню от сносок, копирайта и «похожих страниц» — это
  одна куча «текст вне основного блока»; доля меню в ней не вычисляется.
* ``body_coverage`` — покрытие **словаря**, а не смысла: пересказ, перестановка и
  потеря порядка токенов не видны. Блок, целиком состоящий из навигации (страница без
  содержательного ``article``), даёт высокий ``body_coverage`` при бесполезном тексте.
* ``headings_kept`` сравнивает по префиксу: инлайновая разметка в заголовке
  (``**жирный**``, ссылка) даёт «не найден», то есть число — нижняя оценка.
* Извлечение вызывается на **retained-байтах**, а не на живом фетче: если страница
  рендерилась браузером, здесь тот же HTML, что был сохранён, но повторной отрисовки
  JS нет — SPA-оболочка выглядит так же, как в прогоне.
* Объём корпуса не измеряется вовсе: чанкер, дедуп и покрытие корпуса — другие
  инструменты.

**Декодирование.** Цепочка BOM → ``Content-Type`` → ``<meta charset>`` → UTF-8
``errors="replace"`` повторена здесь **явно**: в ``core/adapters/extract_html.py`` она
приватная (``_decode_page``), и импорт подчёркнутого имени был бы ещё одной связью без
контракта. Копия обязана совпадать с адаптером; при изменении адаптера — сверить
(различие проявится как расхождение ``body_coverage`` с текстом извлечения).

**Детерминизм.** Хосты сортируются по имени, страницы — по URL. Если страниц на хост
больше ``--limit``, берётся детерминированная равномерная выборка по отсортированному
списку URL (не «первые N»: алфавитное начало дерева не должно задавать медиану).

Запуск (БД читается только на чтение, прогон можно не останавливать):

    python3 scripts/measure_html_extraction.py --db runs/{run_id}/cohound.db \\
        --host {url-substring} --host {url-substring} --limit 100

    # политика без входного санитайза — те же страницы, те же числа, другая колонка
    python3 scripts/measure_html_extraction.py --db runs/{run_id}/cohound.db \\
        --host {url-substring} --extractor nosanitize

    # машинный вывод для отчёта-замера
    python3 scripts/measure_html_extraction.py --db runs/{run_id}/cohound.db \\
        --out-json /tmp/measure.json
"""

from __future__ import annotations

import argparse
import codecs
import json
import os
import re
import sqlite3
import statistics
import sys
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import lxml.html  # noqa: E402  (зависимость trafilatura; см. докстринг)

from core.adapters.extract_html import HtmlExtractor  # noqa: E402
from core.config.paths import resolve_paths  # noqa: E402
from core.ports.types import RawPage  # noqa: E402
from core.util.sanitize import sanitize_html  # noqa: E402

#: Токен — непрерывная последовательность букв (``\w`` без цифр и ``_``) длиной ≥6.
_TOKEN_RE = re.compile(r"[^\W\d_]{6,}")

#: ATX-заголовок markdown: до трёх пробелов отступа, 1–6 ``#``, пробел, текст.
_ATX_RE = re.compile(r"^\s{0,3}#{1,6}\s+\S")

#: Длина префикса текста заголовка при сравнении (определение метрики, не настройка).
_HEADING_PREFIX = 40

#: BOM → кодек (порядок как в адаптере: UTF-8 первым).
_BOM_CODECS: tuple[tuple[bytes, str], ...] = (
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
)

_CHARSET_RE = re.compile(r"""charset\s*=\s*["']?([A-Za-z0-9._:-]+)""", re.IGNORECASE)
_META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)
_META_SNIFF_BYTES = 4096

#: Кандидаты основного блока в порядке приоритета (первое непустое совпадение).
_MAIN_SELECTORS: tuple[str, ...] = (
    "//article",
    "//main",
    "//*[@role='main']",
)


# ---------------------------------------------------------------------------
# Декодирование (явная копия цепочки адаптера)
# ---------------------------------------------------------------------------


def _bom_charset(content: bytes) -> str | None:
    """Кодек по BOM файла или ``None`` (без BOM кодировка не угадывается)."""
    for bom, charset in _BOM_CODECS:
        if content.startswith(bom):
            return charset
    return None


def _charset_from_content_type(content_type: str | None) -> str | None:
    """``charset`` из заголовка ``Content-Type`` (или ``None``)."""
    if not content_type:
        return None
    match = _CHARSET_RE.search(content_type)
    return match.group(1) if match else None


def _charset_from_meta(content: bytes) -> str | None:
    """``charset`` из ``<meta>`` в первых 4096 байтах (разбор по latin-1, без угадывания)."""
    head = content[:_META_SNIFF_BYTES].decode("latin-1")
    for tag in _META_TAG_RE.findall(head):
        match = _CHARSET_RE.search(tag)
        if match:
            return match.group(1)
    return None


def _decode_bytes(content: bytes, charset: str | None) -> str:
    """Декодирует объявленным кодеком; неизвестный/битый — UTF-8 ``replace``."""
    if charset:
        try:
            text = content.decode(charset, errors="replace")
        except (LookupError, UnicodeError):
            text = content.decode("utf-8", errors="replace")
    else:
        text = content.decode("utf-8", errors="replace")
    return text.removeprefix("\ufeff")


def _decode_page(content: bytes, content_type: str | None) -> str:
    """BOM → ``Content-Type`` → ``<meta>`` → UTF-8 ``replace`` (порядок = сила объявления)."""
    charset = (
        _bom_charset(content)
        or _charset_from_content_type(content_type)
        or _charset_from_meta(content)
    )
    return _decode_bytes(content, charset)


# ---------------------------------------------------------------------------
# Основной блок, токены, заголовки
# ---------------------------------------------------------------------------


def _tokens(text: str) -> set[str]:
    """Множество токенов: слова из букв длиной ≥6, нижний регистр."""
    return set(_TOKEN_RE.findall(text.lower()))


def _strip_non_content(tree: lxml.html.HtmlElement) -> None:
    """Убирает ``script``/``style``/``noscript`` из дерева (в тексте блока их быть не должно)."""
    for node in tree.xpath("//script|//style|//noscript"):
        node.drop_tree()


def _heading_text(element: lxml.html.HtmlElement) -> str:
    """Нормализованный текст заголовка: схлопнутые пробелы, нижний регистр."""
    return " ".join(element.text_content().split()).lower()


def _block_tokens(tree: lxml.html.HtmlElement) -> tuple[set[str], str]:
    """Токены основного блока и его «происхождение» (селектор — для диагностики)."""
    for selector in _MAIN_SELECTORS:
        nodes = tree.xpath(selector)
        if nodes:
            return _tokens(nodes[0].text_content()), selector
    candidates = tree.xpath("//div|//section")
    best: lxml.html.HtmlElement | None = None
    best_len = -1
    for node in candidates:
        length = len(node.text_content())
        if length > best_len:
            best, best_len = node, length
    if best is None:
        return set(), "none"
    return _tokens(best.text_content()), "longest-div"


def _atx_headings(text: str) -> list[str]:
    """ATX-заголовки извлечения: текст без ``#``, схлопнутые пробелы, нижний регистр."""
    out: list[str] = []
    for line in text.splitlines():
        if not _ATX_RE.match(line):
            continue
        body = line.strip().lstrip("#").strip()
        out.append(" ".join(body.split()).lower())
    return out


def _kept_share(raw_headings: Sequence[str], atx: Sequence[str]) -> float | None:
    """Доля текстов заголовков документа, найденных среди ATX-заголовков извлечения."""
    if not raw_headings:
        return None
    hits = 0
    for heading in raw_headings:
        prefix = heading[:_HEADING_PREFIX]
        if prefix and any(prefix in extracted for extracted in atx):
            hits += 1
    return hits / len(raw_headings)


# ---------------------------------------------------------------------------
# Обход страниц
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Item:
    """Строка ``crawl_session_items``, нужная замеру (тело поднимается отдельно)."""

    host: str
    url: str
    raw_path: str
    content_type: str | None


@dataclass(frozen=True)
class PageResult:
    """Результат одной страницы. ``None``-метрика — величина не определена (не ноль)."""

    host: str
    url: str
    body_coverage: float | None
    chrome_share: float | None
    out_len: int
    headings_raw: int
    headings_out: int
    headings_kept: float | None
    block_source: str
    error: str | None


def _host_of(url: str) -> str:
    """Хост URL без порта и в нижнем регистре (группировка таблицы)."""
    rest = url.split("://", 1)[-1]
    authority = rest.split("/", 1)[0].split("?", 1)[0]
    return authority.rsplit("@", 1)[-1].split(":", 1)[0].lower()


def _raw_base() -> Path:
    """Каталог, относительно которого резолвится относительный ``raw_path``.

    Приоритет — объявленная раскладка (``COHOUND_HOME``/``paths.yaml``): ``corpus_raw``
    из :func:`core.config.paths.resolve_paths`, если он абсолютный. Иначе — канонический
    ``~/.cohound/data`` (тела живут под ``data/corpus_raw``, см. AGENTS §3.1).
    """
    try:
        config = resolve_paths()
    except ValueError as exc:  # невалидный paths.yaml — печатаем, но не падаем
        print(f"measure: раскладка не прочитана ({exc}); беру ~/.cohound/data", file=sys.stderr)
        config = None
    if config is not None and config.corpus_raw.is_absolute():
        return config.corpus_raw
    home = os.environ.get("COHOUND_HOME") or "~/.cohound"
    return Path(home).expanduser() / "data"


def _resolve_raw(raw_path: str, base: Path) -> Path | None:
    """Абсолютный путь к retained-байтам; ``None`` — файла нет ни в одной трактовке."""
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path if path.is_file() else None
    for candidate in (base / path, base.parent / path):
        if candidate.is_file():
            return candidate
    return None


def _ro_connect(path: str) -> sqlite3.Connection:
    """Соединение только на чтение: запись в БД запрещена контрактом инструмента."""
    conn = sqlite3.connect(f"file:{Path(path).expanduser()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _load_items(conn: sqlite3.Connection, hosts: Sequence[str], limit: int) -> tuple[list[Item], int, int]:
    """Детерминированная выборка: фильтр по URL → группировка по хосту → ``limit`` на хост.

    Возвращает ``(items, skipped_no_raw, skipped_non_html)``: страницы без retained-байтов
    и не-HTML не измеряются, но их число печатается — «страниц нет» и «страницы не
    подходят» это разные факты.
    """
    rows = list(
        conn.execute("SELECT url, raw_path, content_type FROM crawl_session_items")
    )
    needles = [needle.casefold() for needle in hosts]
    grouped: dict[str, list[Item]] = {}
    skipped_no_raw = 0
    skipped_non_html = 0
    for row in rows:
        url = str(row["url"])
        if needles and not any(needle in url.casefold() for needle in needles):
            continue
        if not row["raw_path"]:
            skipped_no_raw += 1
            continue
        content_type = row["content_type"]
        if content_type and "html" not in str(content_type).lower():
            skipped_non_html += 1
            continue
        item = Item(
            host=_host_of(url),
            url=url,
            raw_path=str(row["raw_path"]),
            content_type=None if content_type is None else str(content_type),
        )
        grouped.setdefault(item.host, []).append(item)

    selected: list[Item] = []
    for host in sorted(grouped):
        items = sorted(grouped[host], key=lambda it: it.url)
        if len(items) <= limit:
            selected.extend(items)
            continue
        step = len(items) / limit
        selected.extend(items[int(index * step)] for index in range(limit))
    return selected, skipped_no_raw, skipped_non_html


def _empty_result(item: Item) -> PageResult:
    """Результат-заготовка: ни одна метрика не определена (``None``, не ноль)."""
    return PageResult(
        host=item.host,
        url=item.url,
        body_coverage=None,
        chrome_share=None,
        out_len=0,
        headings_raw=0,
        headings_out=0,
        headings_kept=None,
        block_source="none",
        error=None,
    )


def _measure_page(
    item: Item, path: Path, extractor: HtmlExtractor, policy: str = "current"
) -> PageResult:
    """Считает метрики одной страницы; сбой страницы — ``error``, а не исключение.

    ``policy="allowlist"`` воспроизводит поведение **до D1**: декодированный HTML проходит
    allow-list :func:`core.util.sanitize.sanitize_html` и отдаётся экстрактору без его
    собственного пре-клина. Это одна и та же страница и одна и та же ``trafilatura`` —
    меняется только вход, иначе «до/после» сравнивало бы разные выборки.
    """
    empty = _empty_result(item)
    try:
        content = path.read_bytes()
    except OSError as exc:
        return _with_error(empty, f"чтение raw_path: {exc}")

    try:
        html = _decode_page(content, item.content_type)
    except Exception as exc:  # noqa: BLE001 - битые байты не должны валить замер
        return _with_error(empty, f"декодирование: {exc}")

    if policy == "allowlist":
        content = sanitize_html(html).encode("utf-8")
        content_type = "text/html; charset=utf-8"
    else:
        content_type = item.content_type

    doc = extractor.extract(
        RawPage(url=item.url, content=content, fetched_at="", content_type=content_type)
    )
    text = doc.text

    try:
        tree = lxml.html.document_fromstring(html)
    except Exception as exc:  # noqa: BLE001 - lxml на битом HTML бросает разные типы
        return _with_error(empty, f"разбор html: {exc}")

    try:
        _strip_non_content(tree)
        raw_headings = [_heading_text(node) for node in tree.xpath("//h1|//h2|//h3|//h4|//h5|//h6")]
        block, block_source = _block_tokens(tree)
    except Exception as exc:  # noqa: BLE001
        return _with_error(empty, f"основной блок: {exc}")

    extracted = _tokens(text)
    covering = block & extracted
    body_coverage = len(covering) / len(block) if block else None
    chrome_share = 1 - (len(covering) / len(extracted)) if extracted else None
    atx = _atx_headings(text)
    return PageResult(
        host=item.host,
        url=item.url,
        body_coverage=body_coverage,
        chrome_share=chrome_share,
        out_len=len(text),
        headings_raw=len(raw_headings),
        headings_out=len(atx),
        headings_kept=_kept_share(raw_headings, atx),
        block_source=block_source,
        error=doc.error,
    )


def _with_error(result: PageResult, message: str) -> PageResult:
    """Копия результата с заполненным ``error`` (метрики остаются ``None``)."""
    return PageResult(
        host=result.host,
        url=result.url,
        body_coverage=None,
        chrome_share=None,
        out_len=0,
        headings_raw=0,
        headings_out=0,
        headings_kept=None,
        block_source=result.block_source,
        error=message,
    )


# ---------------------------------------------------------------------------
# Агрегация и вывод
# ---------------------------------------------------------------------------


def _median(values: Iterable[float | None]) -> float | None:
    """Медиана по определённым значениям (``None`` — не ноль, а «не измерено»)."""
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


@dataclass
class HostSummary:
    """Строка таблицы: хост и его агрегаты."""

    host: str
    n: int
    errors: int
    no_block: int
    med_body_coverage: float | None
    med_chrome_share: float | None
    med_out_len: float | None
    share_coverage_below_half: float | None
    headings_raw: int
    headings_kept: float | None


def _summarize(host: str, pages: Sequence[PageResult]) -> HostSummary:
    """Агрегирует страницы одного хоста; ошибочные страницы в медианы не входят."""
    measured = [page for page in pages if not page.error]
    coverages = [page.body_coverage for page in measured if page.body_coverage is not None]
    raw_total = sum(page.headings_raw for page in measured)
    kept_share = None
    if raw_total:
        # Сумма по страницам: доля заголовков, а не медиана долей (длинные страницы
        # несут больше заголовков и потому больше весят — это и требуется).
        kept_total = sum(
            round((page.headings_kept or 0) * page.headings_raw) for page in measured
        )
        kept_share = kept_total / raw_total
    return HostSummary(
        host=host,
        n=len(pages),
        errors=sum(1 for page in pages if page.error),
        no_block=sum(1 for page in measured if page.block_source == "none"),
        med_body_coverage=_median(page.body_coverage for page in measured),
        med_chrome_share=_median(page.chrome_share for page in measured),
        med_out_len=_median(float(page.out_len) for page in measured),
        share_coverage_below_half=(
            sum(1 for value in coverages if value < 0.5) / len(coverages) if coverages else None
        ),
        headings_raw=raw_total,
        headings_kept=kept_share,
    )


def _fmt(value: float | None, digits: int = 3) -> str:
    """Печать величины: ``None`` печатается как ``n/a`` (не как 0)."""
    return "n/a" if value is None else f"{value:.{digits}f}"


def _print_table(summaries: Sequence[HostSummary]) -> None:
    """Таблица по хостам: медианы, доля плохих страниц, заголовки, ошибки."""
    header = (
        f"{'host':<34}{'n':>5}{'err':>5}{'body':>8}{'chrome':>8}"
        f"{'len':>9}{'<0.5':>7}{'h_raw':>7}{'h_kept':>8}"
    )
    print(header)
    print("-" * len(header))
    for row in summaries:
        length = "n/a" if row.med_out_len is None else f"{row.med_out_len:.0f}"
        print(
            f"{row.host:<34}{row.n:>5}{row.errors:>5}"
            f"{_fmt(row.med_body_coverage):>8}{_fmt(row.med_chrome_share):>8}"
            f"{length:>9}{_fmt(row.share_coverage_below_half, 2):>7}"
            f"{row.headings_raw:>7}{_fmt(row.headings_kept):>8}"
        )


def _worst(pages: Sequence[PageResult], count: int) -> list[PageResult]:
    """Худшие страницы по ``body_coverage`` (ниже — хуже); текста страниц не печатает."""
    ranked = [page for page in pages if page.body_coverage is not None]
    ranked.sort(key=lambda page: (page.body_coverage or 0.0, page.url))
    return ranked[:count]


def _print_worst(pages: Sequence[PageResult], count: int) -> None:
    """Три (по умолчанию) худших страницы: URL и числа, без содержимого."""
    worst = _worst(pages, count)
    if not worst:
        print("\nхудшие страницы: нет измеренных")
        return
    print(f"\nхудшие {len(worst)} страниц по body_coverage:")
    for page in worst:
        print(
            f"  {page.url}\n"
            f"    body_coverage={_fmt(page.body_coverage)}"
            f" chrome_share={_fmt(page.chrome_share)}"
            f" out_len={page.out_len} headings={page.headings_raw}->{page.headings_out}"
            f" kept={_fmt(page.headings_kept, 2)} block={page.block_source}"
        )


def _json_payload(
    *,
    db: str,
    policy: str,
    limit: int,
    hosts: Sequence[str],
    skipped_no_raw: int,
    skipped_non_html: int,
    summaries: Sequence[HostSummary],
    pages: Sequence[PageResult],
    worst_count: int,
) -> dict[str, object]:
    """Машинный отчёт: параметры замера, агрегаты и все страницы (сортировка по URL)."""
    ordered = sorted(pages, key=lambda page: (page.host, page.url))
    return {
        "tool": "measure_html_extraction",
        "params": {
            "db": db,
            "extractor": policy,
            "limit_per_host": limit,
            "host_filter": sorted(hosts),
            "skipped_no_raw_path": skipped_no_raw,
            "skipped_non_html": skipped_non_html,
        },
        "hosts": [asdict(summary) for summary in summaries],
        "worst": [
            {
                "url": page.url,
                "body_coverage": page.body_coverage,
                "chrome_share": page.chrome_share,
                "out_len": page.out_len,
                "headings_raw": page.headings_raw,
                "headings_out": page.headings_out,
                "block_source": page.block_source,
            }
            for page in _worst(pages, worst_count)
        ],
        "pages": [asdict(page) for page in ordered],
    }


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Аргументы: БД (обязательна), фильтры хостов, размер выборки, политика, JSON."""
    parser = argparse.ArgumentParser(
        prog="measure_html_extraction.py",
        description=(
            "Приёмка извлечения HTML по сохранённой БД прогона (read-only): "
            "body_coverage, chrome_share, сохранность заголовков."
        ),
        epilog=(
            "Примеры:\n"
            "  python3 scripts/measure_html_extraction.py --db runs/{run_id}/cohound.db\n"
            "  python3 scripts/measure_html_extraction.py --db runs/{run_id}/cohound.db \\\n"
            "      --host example.org --host docs.example.org --limit 100\n"
            "  python3 scripts/measure_html_extraction.py --db runs/{run_id}/cohound.db \\\n"
            "      --host example.org --extractor nosanitize --out-json /tmp/m.json\n"
            "      --host example.org --extractor allowlist   # как было до D1\n"
            "\n"
            "Без --host измеряются все хосты БД (по --limit страниц на хост).\n"
            "БД не изменяется: соединение открывается в режиме mode=ro."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        required=True,
        help="путь к cohound.db прогона (читается только на чтение)",
    )
    parser.add_argument(
        "--host",
        action="append",
        default=[],
        metavar="SUBSTR",
        help="фильтр по URL, подстрока (можно повторять); без него — все хосты",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        help="страниц на хост, дефолт 300 (равномерная выборка по URL)",
    )
    parser.add_argument(
        "--extractor",
        choices=("current", "nosanitize", "allowlist"),
        default="current",
        help=(
            "политика извлечения: current — как в прогоне (пре-клин структуры); "
            "nosanitize — вход не чистится вовсе; allowlist — как было до D1 "
            "(allow-list sanitize_html перед trafilatura)"
        ),
    )
    parser.add_argument("--out-json", metavar="PATH", help="куда записать машинный отчёт")
    parser.add_argument(
        "--examples",
        type=int,
        default=3,
        metavar="N",
        help="сколько худших страниц показать (дефолт 3)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа: выборка страниц → замер → таблица → (опционально) JSON."""
    args = _parse_args(argv)
    if args.limit < 1:
        print("measure: --limit должен быть ≥ 1", file=sys.stderr)
        return 2
    if not Path(args.db).expanduser().is_file():
        print(f"measure: БД не найдена: {args.db}", file=sys.stderr)
        return 2

    extractor = HtmlExtractor(sanitize=args.extractor == "current")
    if not extractor.available:
        print(
            "measure: trafilatura не установлена — извлечение невозможно (extra 'extract')",
            file=sys.stderr,
        )
        return 2

    base = _raw_base()
    conn = _ro_connect(args.db)
    try:
        items, skipped_no_raw, skipped_non_html = _load_items(conn, args.host, args.limit)
    finally:
        conn.close()

    pages: list[PageResult] = []
    for item in items:
        path = _resolve_raw(item.raw_path, base)
        if path is None:
            pages.append(_with_error(_empty_result(item), f"нет файла по raw_path: {item.raw_path}"))
            continue
        pages.append(_measure_page(item, path, extractor, args.extractor))

    grouped: dict[str, list[PageResult]] = {}
    for page in pages:
        grouped.setdefault(page.host, []).append(page)
    summaries = [_summarize(host, grouped[host]) for host in sorted(grouped)]

    print(
        f"extractor={args.extractor} limit/per-host={args.limit} "
        f"pages={len(pages)} skipped(no_raw_path)={skipped_no_raw} "
        f"skipped(non_html)={skipped_non_html} base={base}"
    )
    _print_table(summaries)
    _print_worst(pages, max(args.examples, 0))

    if args.out_json:
        payload = _json_payload(
            db=args.db,
            policy=args.extractor,
            limit=args.limit,
            hosts=args.host,
            skipped_no_raw=skipped_no_raw,
            skipped_non_html=skipped_non_html,
            summaries=summaries,
            pages=pages,
            worst_count=max(args.examples, 0),
        )
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"\njson: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())