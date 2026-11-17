#!/usr/bin/env python3
"""Прибор: извлекает ли оценщик **тот же смысл** из ZH-документа, что и из EN (план 26).

Вопрос, на который отвечает замер: можно ли ставить ZH-продукт в когорту, если
``render_queries`` (``core/pipeline/prompt.py:402``) собирает запрос из ``title`` +
терминов **ru → en**. ZH-документ с русским запросом в лексическом плече R0 не
находится — это структурный ноль, а не «хуже». Чтобы отличить «оценщик не читает
по-китайски» от «поиск не находит по-китайски», прибор имеет два объявленных режима.

``aligned`` — изолирует **оценщика**. Обе стороны получают один и тот же участок
текста: страницу одного и того же мануала PostgreSQL в двух языках
(``postgresql.org/docs/18`` и ``postgres.cn/docs/18``), нарезанную чанкером проекта, а
слайс выбирается механически — по ASCII-якорю, который встречается в тексте обеих
сторон буквально (имена GUC/команд PG остаются латиницей и в переводе). Никакого
поиска здесь нет: если вердикты расходятся, это расхождение чтения, а не поиска.

``retrieval`` — весь путь R0: те же чанки, но слайс выбирает **запрос фазы Ш6**
(``run_queries_for_cell`` → ``prompt.render_queries``) по лексическому индексу
(``TantivyLexicalIndex``, профиль ``stemmed`` — как в прогоне). Ожидание на ZH —
структурный ноль; подтверждение этого ожидания есть результат, а не поломка.

Что прибор **не** меряет (и почему — в отчёте): dense-плечо (bge-m3 многоязычен, но
пере-эмбеддинг ≈7 ч) и нарезку ZH как таковую.

Честность входа:

* обе стороны читаются **читателем проекта** (``HtmlExtractor``/trafilatura), а не своим
  парсером: сравнение идёт на том тексте, который видит пайплайн;
* чанкер — ``MarkdownChunkerPort`` (``policy-6``), ``chunk_size`` — дефолт кода 1500:
  ключа ``chunk_size`` в ``config/deploy.yaml`` нет, значение печатается в отчёте;
* вердикт — ``core.pipeline.evaluate.evaluate_cell``, не своя копия промпта;
* модель — объявленный вход: ``--llm-model`` > ``deploy.llm.<slot>.model_id`` > отказ
  (``EXIT_USAGE``); ``COHOUND_LLM_MODEL`` оценщик не читает;
* вежливость: пауза между сетевыми запросами, кэш страниц на диске (повторный прогон
  не качает заново), никакого логина и обхода барьеров.

Запуск::

    .venv/bin/python scripts/measure_cross_lingual_eval.py --mode both --pairs 30
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import warnings
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # запуск как скрипт: `core` берётся из дерева репозитория
    sys.path.insert(0, str(ROOT))

from core.adapters.chunker_markdown import MarkdownChunkerPort  # noqa: E402
from core.adapters.extract_html import HtmlExtractor  # noqa: E402
from core.adapters.lexical_tantivy import TantivyLexicalIndex  # noqa: E402
from core.cli.errors import CliError  # noqa: E402
from core.cli.wiring import build_evaluator_llm  # noqa: E402
from core.config.deploy import load_deploy_config  # noqa: E402
from core.config.run_config import CatalogRow, load_catalog  # noqa: E402
from core.pipeline.evaluate import (  # noqa: E402
    evaluate_cell,
    retrieve_for_cell,
    run_queries_for_cell,
)
from core.ports.llm import EvaluatorPort, LLMRequest, LLMResponse  # noqa: E402
from core.ports.types import Chunk, ExtractedDoc, RawPage  # noqa: E402
from core.util.agreement import AnnotationRow, compute_agreement  # noqa: E402
from core.util.hashing import content_hash  # noqa: E402

#: Базы двух редакций одного мануала. Пути внутри ``/docs/18/`` совпадают.
EN_BASE = "https://www.postgresql.org"
ZH_BASE = "http://www.postgres.cn"
DOCS_PATH = "/docs/18/"
SITEMAP_URL = f"{EN_BASE}/sitemap.xml"
EN_INDEX_URL = f"{EN_BASE}{DOCS_PATH}bookindex.html"

#: Идентичность продукта в замере: обе редакции — один и тот же продукт PostgreSQL.
PRODUCT_ID = "postgresql"

#: Максимум чанков-якорей на сторону (объявлено заданием: «все такие чанки, но ≤ K»).
K_ANCHOR_CHUNKS = 3

#: Сколько кандидатов-страниц (из предметного указателя) пробовать на один якорь.
MAX_PATHS_PER_ANCHOR = 3

#: Порядок статусов в матрице переходов (словарь домена; чужие метки — в конец).
STATUS_ORDER: tuple[str, ...] = ("✅", "⚠️", "❌", "❓", "N/A")

#: Причины отбраковки пары (машинные токены; публикуются с числами).
DROP_REASONS: tuple[str, ...] = (
    "no_ascii_term",
    "anchor_not_in_index",
    "en_page_missing",
    "anchor_absent_en",
    "zh_page_missing",
    "anchor_absent_zh",
    "no_usable_page",
    "fetch_budget_exhausted",
)

#: ASCII-идентификатор: буква/подчёркивание + буквы/цифры/подчёркивания, ≥ 3 символов.
ASCII_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")

#: Попыток сетевого запроса на страницу: хост рвёт TLS пачками, одна неудача — не факт.
FETCH_ATTEMPTS = 2

#: Фиксированный seed бутстрэпа: интервал обязан воспроизводиться, а не «гулять».
DEFAULT_SEED = 20261117


# ---------------------------------------------------------------------------
# Чистые функции выбора и выравнивания (тестируются без сети)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexEntry:
    """Запись предметного указателя EN-мануала: имя термина → путь страницы.

    ``has_fragment`` — ссылка указывает на **раздел** (``page.html#ANCHOR``), а не на
    страницу целиком. Это единственный дешёвый структурный признак «это раздел, а не
    оглавление главы», доступный без скачивания.
    """

    name: str
    path: str
    has_fragment: bool


@dataclass(frozen=True)
class AnchorChoice:
    """Решение о якоре параметра: чем выравнивать и почему не вышло.

    ``anchor is None`` ⇔ выравнивание механически невозможно, и ``reason`` называет
    причину (:data:`DROP_REASONS`); «нет якоря» — это отбраковка, а не тихий пропуск.
    """

    anchor: str | None
    paths: tuple[str, ...]
    reason: str | None = None


def parse_sitemap(text: str) -> tuple[str, ...]:
    """URL-адреса документации 18 из sitemap (порядок файла, без дублей).

    Sitemap — объявленный перечень страниц, а не догадка: список страниц мануала
    берётся из него, поэтому новые разделы попадают в замер без правки кода.
    """
    found: list[str] = []
    seen: set[str] = set()
    for raw in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", text):
        url = raw.strip()
        if DOCS_PATH not in url or not url.endswith(".html"):
            continue
        if url in seen:
            continue
        seen.add(url)
        found.append(url)
    return tuple(found)


def parse_index_entries(html: str) -> tuple[IndexEntry, ...]:
    """Предметный указатель EN-мануала → записи ``(термин, путь, раздел?)``.

    Термин — текст записи ``<dt>`` до первой запятой (``max_connections configuration
    parameter, Connection Settings`` → ``max_connections``). Разбор регулярный и
    терпимый к разметке: указатель — стабильная структура мануала, а не наш формат.
    """
    entries: list[IndexEntry] = []
    for block in re.findall(r"<dt[^>]*>(.*?)</dt>", html, re.S):
        hrefs = re.findall(r'href="([^"]+\.html[^"]*)"', block)
        if not hrefs:
            continue
        text = re.sub(r"<[^>]+>", "", block)
        name = text.split(",")[0].strip().casefold()
        # Указатель подписывает параметры GUC как «<имя> configuration parameter»:
        # суффикс — часть описания, а не имени, и якорем быть не может.
        name = re.sub(r"\s+configuration parameters?$", "", name)
        if not name:
            continue
        href = hrefs[0]
        path, _, fragment = href.partition("#")
        entries.append(IndexEntry(name=name, path=path, has_fragment=bool(fragment)))
    return tuple(entries)


def anchor_candidates(row: CatalogRow) -> tuple[str, ...]:
    """ASCII-якоря параметра из ``concept_terms.en`` (детерминированный порядок).

    Ранг — «сила идентификатора», без словарей и порогов: сначала термин с
    подчёркиванием (``max_connections``), затем более длинный, затем по алфавиту. Имя
    GUC/команды PG остаётся латиницей и в переводе, поэтому совпадение по тексту
    означает «та же сущность», а не «то же слово языка».
    """
    terms: list[str] = []
    for term in (row.concept_terms or {}).get("en") or []:
        candidate = term.strip()
        if ASCII_IDENTIFIER_RE.fullmatch(candidate):
            terms.append(candidate)
    ordered = sorted(dict.fromkeys(terms), key=lambda a: ("_" not in a, -len(a), a.casefold()))
    return tuple(ordered)


def anchor_paths(entries: Sequence[IndexEntry], anchor: str) -> tuple[str, ...]:
    """Пути страниц, где указатель объявляет якорь: точное имя, затем «имя + слово».

    Порядок детерминирован: точные записи (в порядке указателя), затем записи,
    начинающиеся с якоря как слова (``wal`` → ``wal level``), затем остальное
    отбрасывается. Дубли путей схлопываются: страница — цель, а не запись.
    """
    key = anchor.casefold()
    exact: list[str] = []
    prefix: list[str] = []
    for entry in entries:
        name = entry.name
        if name == key:
            exact.append(entry.path)
        elif name.startswith(key + " ") or name.startswith(key + "_"):
            prefix.append(entry.path)
    ordered = list(dict.fromkeys(exact + prefix))
    return tuple(ordered)


def choose_anchor(row: CatalogRow, entries: Sequence[IndexEntry]) -> AnchorChoice:
    """Первый якорь параметра, для которого указатель знает страницу.

    Возвращает ``reason="no_ascii_term"``, если в ``concept_terms.en`` нет ни одного
    ASCII-идентификатора, и ``reason="anchor_not_in_index"``, если указатель не знает ни
    одного из них. Параметр без якоря **отбрасывается с причиной**, а не выравнивается
    «по заголовку»: заголовок русский, и такое выравнивание было бы догадкой.
    """
    candidates = anchor_candidates(row)
    if not candidates:
        return AnchorChoice(None, (), "no_ascii_term")
    for anchor in candidates:
        paths = anchor_paths(entries, anchor)
        if paths:
            return AnchorChoice(anchor, paths)
    return AnchorChoice(None, (), "anchor_not_in_index")


def anchor_pattern(anchor: str) -> re.Pattern[str]:
    """Шаблон якоря: границы ASCII-токена, регистр не значим.

    Имена GUC и команд PG в переводе остаются латиницей (``max_connections``,
    ``VACUUM``), поэтому совпадение по тексту — это совпадение сущности. Границы нужны,
    чтобы ``max`` не считался якорем в ``max_connections``.
    """
    return re.compile(rf"(?<![A-Za-z0-9_]){re.escape(anchor)}(?![A-Za-z0-9_])", re.IGNORECASE)


def anchor_chunks(
    chunks: Sequence[Chunk], anchor: str, limit: int = K_ANCHOR_CHUNKS
) -> tuple[Chunk, ...]:
    """Чанки, содержащие якорь (в порядке документа, не больше ``limit``).

    Пустой результат — сторона не выровнена: «якоря нет в этом тексте» — факт, который
    отбраковывает пару, а не превращается в пустой слайс.
    """
    if limit <= 0:
        return ()
    pattern = anchor_pattern(anchor)
    found: list[Chunk] = []
    for chunk in chunks:
        if pattern.search(chunk.text):
            found.append(chunk)
            if len(found) >= limit:
                break
    return tuple(found)


def status_pairs(records: Sequence[Mapping[str, Any]], mode: str) -> list[tuple[str, str]]:
    """Пары ``(status_EN, status_ZH)`` одного режима; ``None``-вердикты не входят в κ.

    Вердикт ``None`` — не «равенство» и не «расхождение»: ячейка не оценена
    (``failed``/``no_fragments`` без статуса), и в κ она не участвует (парность —
    условие расчёта, ``core/util/agreement.py``).
    """
    pairs: list[tuple[str, str]] = []
    for record in records:
        block = record.get(mode)
        if not isinstance(block, Mapping):
            continue
        en = block.get("status_en")
        zh = block.get("status_zh")
        if isinstance(en, str) and isinstance(zh, str):
            pairs.append((en, zh))
    return pairs


def transition_matrix(pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], int]:
    """Матрица переходов ``status_EN → status_ZH`` с числами (все встреченные пары)."""
    matrix: dict[tuple[str, str], int] = {}
    for en, zh in pairs:
        matrix[(en, zh)] = matrix.get((en, zh), 0) + 1
    return dict(sorted(matrix.items(), key=lambda item: (_status_rank(item[0][0]), _status_rank(item[0][1]), item[0])))


def _status_rank(label: str) -> int:
    """Порядок статуса в словаре домена; чужой ярлык — после всех известных."""
    try:
        return STATUS_ORDER.index(label)
    except ValueError:
        return len(STATUS_ORDER)


def bootstrap_kappa(
    pairs: Sequence[tuple[str, str]],
    *,
    iterations: int = 2000,
    seed: int = DEFAULT_SEED,
) -> tuple[float | None, float | None]:
    """Бутстрэп по парам: 2.5/97.5 перцентили κ (``None``, если κ не определён).

    Пересэмплирование — **по парам** (ячейкам), а не по агрегатам: интервал обязан
    описывать разброс согласия, а не разброс одного числа. Вырожденный ресэмпл (одна
    метка на обеих сторонах) κ не определяет — такие итерации отбрасываются, и если
    определяемых нет вовсе, интервала нет (``None``), а не ``0``.
    """
    if iterations <= 0 or not pairs:
        return None, None
    rng = random.Random(seed)  # noqa: S311 — воспроизводимость, не криптография
    values: list[float] = []
    n = len(pairs)
    for _ in range(iterations):
        sample = [pairs[rng.randrange(n)] for _ in range(n)]
        rows = [
            AnnotationRow(PRODUCT_ID, f"p{index}", en, zh)
            for index, (en, zh) in enumerate(sample)
        ]
        kappa = compute_agreement(rows).overall.kappa
        if kappa is not None:
            values.append(kappa)
    if not values:
        return None, None
    values.sort()
    low = values[int(0.025 * (len(values) - 1))]
    high = values[int(0.975 * (len(values) - 1))]
    return low, high


def summarize(
    records: Sequence[Mapping[str, Any]],
    mode: str,
    *,
    bootstrap: int = 2000,
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Сводка режима: парность, κ + интервал, матрица, два вида перехода ✅→.

    ``✅→❓`` (не понял перевод) и ``✅→❌`` (перевод сказал обратное) считаются
    **раздельно**: это разные дефекты, и складывать их в «расхождения» значило бы
    потерять именно тот, который решает судьбу ZH-продукта в когорте.
    """
    pairs = status_pairs(records, mode)
    rows = [
        AnnotationRow(PRODUCT_ID, f"r{index}", en, zh)
        for index, (en, zh) in enumerate(pairs)
    ]
    agreement = compute_agreement(rows)
    matrix = transition_matrix(pairs)
    low, high = bootstrap_kappa(pairs, iterations=bootstrap, seed=seed)
    return {
        "mode": mode,
        "records": len(records),
        "paired": len(pairs),
        "unpaired": len(records) - len(pairs),
        "kappa": agreement.overall.kappa,
        "kappa_ci95": {"low": low, "high": high},
        "observed_agreement": agreement.overall.observed_agreement,
        "expected_agreement": agreement.overall.expected_agreement,
        "undefined_reason": agreement.overall.undefined_reason,
        "matrix": [
            {"en": en, "zh": zh, "n": count} for (en, zh), count in matrix.items()
        ],
        "ok_to_unknown": matrix.get(("✅", "❓"), 0),
        "ok_to_absent": matrix.get(("✅", "❌"), 0),
        "disagreements": list(agreement.disagreements),
    }


def _one_per_slice(records: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """По одному представителю на группу ``(якорь, путь)`` — контроль корреляции.

    Параметры делят страницу (семь параметров меряются на ``plpgsql-porting.html`` с
    якорем ``oracle``), поэтому их вердикты не независимы. Сводка по представителям
    публикуется рядом: если κ держится и там, согласие — свойство чтения, а не следствие
    одного удачного слайса.
    """
    seen: set[tuple[str, str]] = set()
    unique: list[Mapping[str, Any]] = []
    for record in records:
        key = (str(record.get("anchor") or ""), str(record.get("path") or ""))
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


# ---------------------------------------------------------------------------
# Сеть и кэш страниц
# ---------------------------------------------------------------------------


@dataclass
class FetchStats:
    """Цена замера: сколько запросов ушло в сеть, сколько взято из кэша, байты, время."""

    network: int = 0
    cache_hits: int = 0
    bytes_network: int = 0
    seconds: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "pages_network": self.network,
            "pages_from_cache": self.cache_hits,
            "mb_network": round(self.bytes_network / (1024 * 1024), 2),
            "seconds": round(self.seconds, 2),
        }


class PageCache:
    """Кэш страниц на диск: ``sha256(url).bin`` + метаданные, повтор не качает заново.

    Пауза перед **сетевым** запросом — объявленная вежливость к хосту (значение
    печатается в отчёте). Кэш-хит паузы не требует: запроса нет.
    """

    def __init__(self, root: Path, *, pause: float, timeout: float, user_agent: str) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.pause = max(0.0, pause)
        self.timeout = timeout
        self.user_agent = user_agent
        self.stats = FetchStats()

    def _paths(self, url: str) -> tuple[Path, Path]:
        digest = content_hash(url)
        return self.root / f"{digest}.bin", self.root / f"{digest}.json"

    def get(self, url: str) -> tuple[bytes, str, bool]:
        """``(body, content_type, from_network)``; кэш — по адресу, а не по дате."""
        body_path, meta_path = self._paths(url)
        if body_path.exists() and meta_path.exists():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.stats.cache_hits += 1
            return body_path.read_bytes(), str(meta.get("content_type") or ""), False

        if self.pause:
            time.sleep(self.pause)
        started = time.monotonic()
        last_error: Exception | None = None
        body = b""
        content_type = ""
        # Две попытки — не «устойчивость ради зелёного»: хост отдаёт обрыв TLS и таймауты
        # пачками (наблюдено 17.11.2026: 2 из 3 запросов к postgresql.org), и одна
        # неудача не должна превращать замер в «страниц нет».
        for attempt in range(FETCH_ATTEMPTS):
            request = urllib.request.Request(  # noqa: S310 — только http(s) двух мануалов
                url, headers={"User-Agent": self.user_agent, "Accept-Language": "en,zh;q=0.8"}
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                    body = response.read()
                    content_type = str(response.headers.get("Content-Type") or "")
                last_error = None
                break
            except (urllib.error.URLError, OSError) as exc:
                last_error = exc
                if attempt + 1 < FETCH_ATTEMPTS:
                    time.sleep(self.pause or 1.0)
        self.stats.seconds += time.monotonic() - started
        if last_error is not None:
            raise last_error
        self.stats.network += 1
        self.stats.bytes_network += len(body)
        body_path.write_bytes(body)
        meta_path.write_text(
            json.dumps({"url": url, "content_type": content_type}, ensure_ascii=False),
            encoding="utf-8",
        )
        return body, content_type, True


@dataclass
class SideText:
    """Одна сторона одной страницы: текст читателя, чанки чанкера, провенанс чтения.

    ``text_normalized_len``/``text_identical`` — **контроль читателя**: на postgres.cn
    страницы приходят XHTML с объявлением кодировки, из-за чего пре-клин
    (``strip_active_html``) деградирует к allow-list, а на postgresql.org — нет. Текст
    после снятия объявления сравнивается с текстом пайплайна: если он тот же, деградация
    косметическая и языкового смещения не вносит. ``None`` — сторона не деградировала
    (контроль не нужен).
    """

    url: str
    text: str
    chunks: tuple[Chunk, ...]
    reader_degraded: bool
    error: str | None = None
    text_normalized_len: int | None = None
    text_identical: bool | None = None


def read_page(
    cache: PageCache,
    base: str,
    path: str,
    *,
    extractor: HtmlExtractor,
    chunker: MarkdownChunkerPort,
    lang: str,
) -> SideText:
    """Скачивает страницу (или берёт из кэша) и читает её **читателем проекта**.

    Читатель тот же, что в пайплайне, поэтому текст — тот, который увидит оценщик.
    Факт деградации пре-клина (``strip_active_html`` → allow-list) записывается: это
    свойство входа страницы, а не нашего разбора, и оно обязано быть видно в отчёте.
    Деградировавшую сторону читаем **второй раз** со снятым объявлением кодировки: это
    контроль «читатель или язык», и он ничего не стоит (сеть уже не нужна).
    """
    url = f"{base}{DOCS_PATH}{path}"
    try:
        body, content_type, _ = cache.get(url)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return SideText(url=url, text="", chunks=(), reader_degraded=False, error=str(exc))

    page = RawPage(
        url=url,
        content=body,
        fetched_at=datetime.now(UTC).isoformat(),
        content_type=content_type or None,
    )
    doc, degraded = _extract(extractor, page)
    if doc.error:
        return SideText(url=url, text="", chunks=(), reader_degraded=degraded, error=doc.error)
    chunks = tuple(
        _with_run_address(chunk, lang=lang, url=url)
        for chunk in chunker.chunk(doc, f"xling-{lang}")
    )
    normalized_len: int | None = None
    identical: bool | None = None
    if degraded:
        stripped = _strip_xml_declaration(body)
        if stripped != body:
            control, _ = _extract(
                extractor,
                RawPage(
                    url=url,
                    content=stripped,
                    fetched_at=page.fetched_at,
                    content_type=content_type or None,
                ),
            )
            if not control.error:
                normalized_len = len(control.text)
                identical = control.text == doc.text
    return SideText(
        url=url,
        text=doc.text,
        chunks=chunks,
        reader_degraded=degraded,
        text_normalized_len=normalized_len,
        text_identical=identical,
    )


def _extract(extractor: HtmlExtractor, page: RawPage) -> tuple[ExtractedDoc, bool]:
    """Читает страницу и сообщает, деградировал ли пре-клин к allow-list."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        doc = extractor.extract(page)
    return doc, any("allow-list" in str(item.message) for item in caught)


def _strip_xml_declaration(body: bytes) -> bytes:
    """Снимает ведущее ``<?xml ...?>`` — вход, а не разбор: читатель остаётся проектным.

    Нужно только как **контроль**: объявление кодировки заставляет ``lxml`` отказаться от
    разбора str-входа, и пре-клин падает к allow-list. Снятие объявления не подменяет
    читателя — оно возвращает ему тот вход, который он ожидает.
    """
    return re.sub(rb"^\s*<\?xml[^>]*\?>\s*", b"", body, count=1)


def _with_run_address(chunk: Chunk, *, lang: str, url: str) -> Chunk:
    """Адрес чанка в форме прогона: ``product:doc_version:chunk_no`` (O5).

    Без этого ``evaluate_cell`` пишет в заметки ``evidence_doc_version_unresolved``: адрес
    версии документа — провенанс принятого evidence, и выдумывать его незачем — он
    выводится из URL и языка, то есть из того, что мы действительно читали.
    """
    doc_version = f"dv_{content_hash(f'{url}|{lang}')[:16]}"
    return replace(chunk, chunk_id=f"{PRODUCT_ID}:{doc_version}:{chunk.chunk_no}")


# ---------------------------------------------------------------------------
# Оценка ячейки (настоящий путь оценщика)
# ---------------------------------------------------------------------------


class CountingEvaluator:
    """Обёртка порта оценщика: считает вызовы и делегирует всё остальное.

    Счётчик — цена замера, а не условие: ``evaluate_cell`` читает у адаптера
    ``model_id``/``seed``/``reasoning_effort``, и обёртка обязана их пропустить.
    """

    def __init__(self, inner: EvaluatorPort) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self.calls = 0

    def complete(self, req: LLMRequest) -> LLMResponse:
        with self._lock:
            self.calls += 1
        return self._inner.complete(req)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def evaluate_slice(
    *,
    param: CatalogRow,
    chunks: Sequence[Any],
    evaluator: CountingEvaluator,
    run_id: str,
) -> dict[str, Any]:
    """Оценивает слайс настоящим ``evaluate_cell`` и возвращает публикуемую сводку.

    Пустой слайс — законный вход: ``evaluate_cell`` не тратит вызов модели и отдаёт
    ``❓`` с заметкой ``no_fragments``. Именно так выглядит структурный ноль ZH-поиска,
    и он должен быть виден как факт, а не как ошибка прибора.
    """
    outcome = evaluate_cell(
        param=param,
        product_id=PRODUCT_ID,
        run_id=run_id,
        evaluator=evaluator,
        chunks=tuple(chunks),
    )
    status = outcome.cell.status.value if outcome.cell.status is not None else None
    quotes = [evidence.quote[:120] for evidence in outcome.cell.evidence if evidence.quote]
    return {
        "status": status,
        "quotes": quotes,
        "state": outcome.cell.state.value,
        "failure_kind": outcome.failure_kind,
        "notes": list(outcome.cell.validation_notes)[:4],
        "justification": (outcome.justification or "")[:200] or None,
    }


# ---------------------------------------------------------------------------
# Сбор пар: страница + якорь + слайсы обеих сторон
# ---------------------------------------------------------------------------


@dataclass
class Pair:
    """Одна измеряемая пара: параметр, якорь, путь страницы и обе стороны."""

    param: CatalogRow
    anchor: str
    path: str
    en: SideText
    zh: SideText
    en_slice: tuple[Chunk, ...]
    zh_slice: tuple[Chunk, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "param_id": self.param.param_id,
            "anchor": self.anchor,
            "path": self.path,
            "en_url": self.en.url,
            "zh_url": self.zh.url,
            "en_text_len": len(self.en.text),
            "zh_text_len": len(self.zh.text),
            "text_ratio": _ratio(len(self.zh.text), len(self.en.text)),
            "en_chunks": len(self.en.chunks),
            "zh_chunks": len(self.zh.chunks),
            "en_slice_chunks": len(self.en_slice),
            "zh_slice_chunks": len(self.zh_slice),
            "en_reader_degraded": self.en.reader_degraded,
            "zh_reader_degraded": self.zh.reader_degraded,
            "zh_reader_control": {
                "text_len_normalized": self.zh.text_normalized_len,
                "text_identical": self.zh.text_identical,
            },
        }


def _ratio(numerator: int, denominator: int) -> float | None:
    """Отношение длин; знаменатель ноль — ``None`` (не ``0`` и не ``inf``)."""
    if denominator <= 0:
        return None
    return round(numerator / denominator, 3)


def build_pairs(
    catalog: Sequence[CatalogRow],
    entries: Sequence[IndexEntry],
    cache: PageCache,
    *,
    extractor: HtmlExtractor,
    chunker: MarkdownChunkerPort,
    limit: int,
    max_fetches: int,
) -> tuple[list[Pair], dict[str, int], list[dict[str, str]]]:
    """Собирает до ``limit`` пар; возвращает пары, счётчик причин и журнал отбраковки.

    Страница выбирается **по данным, а не по догадке**: среди целей указателя для якоря
    (≤ ``MAX_PATHS_PER_ANCHOR``) берётся та, где текст EN длиннее — предметный указатель
    иногда ведёт на оглавление главы, и «самая содержательная цель» отделяет раздел от
    списка ссылок без порога в символах. Решение по якорю запоминается: параметры,
    делящие якорь (``ldap`` у четырёх), не пересматривают страницу.
    """
    pairs: list[Pair] = []
    drops: dict[str, int] = dict.fromkeys(DROP_REASONS, 0)
    dropped: list[dict[str, str]] = []
    resolved: dict[str, tuple[tuple[str, SideText, SideText] | None, str]] = {}

    def note(param_id: str, reason: str, anchor: str | None = None) -> None:
        drops[reason] = drops.get(reason, 0) + 1
        dropped.append({"param_id": param_id, "reason": reason, "anchor": anchor or ""})

    for param in catalog:
        if len(pairs) >= limit:
            break
        choice = choose_anchor(param, entries)
        if choice.anchor is None:
            note(param.param_id, choice.reason or "anchor_not_in_index")
            continue
        if cache.stats.network >= max_fetches:
            note(param.param_id, "fetch_budget_exhausted", choice.anchor)
            continue

        key = choice.anchor.casefold()
        if key not in resolved:
            resolved[key] = _resolve_pages(
                choice,
                cache,
                extractor=extractor,
                chunker=chunker,
                max_fetches=max_fetches,
            )
        resolved_pair, reason = resolved[key]
        if resolved_pair is None:
            note(param.param_id, reason or "no_usable_page", choice.anchor)
            continue
        path, en, zh = resolved_pair
        pairs.append(
            Pair(
                param=param,
                anchor=choice.anchor,
                path=path,
                en=en,
                zh=zh,
                en_slice=anchor_chunks(en.chunks, choice.anchor),
                zh_slice=anchor_chunks(zh.chunks, choice.anchor),
            )
        )
    return pairs, drops, dropped


def _resolve_pages(
    choice: AnchorChoice,
    cache: PageCache,
    *,
    extractor: HtmlExtractor,
    chunker: MarkdownChunkerPort,
    max_fetches: int,
) -> tuple[tuple[str, SideText, SideText] | None, str]:
    """Выбирает страницу якоря: самая содержательная цель EN, затем проверка ZH.

    Кандидаты (≤ ``MAX_PATHS_PER_ANCHOR``) читаются с обеих сторон; страница годится,
    только если якорь есть в тексте **обеих**. Порядок предпочтения — длина текста EN:
    так оглавление главы проигрывает разделу, где термин действительно описан.

    Заглушка (текст EN короче бюджета куска ``chunk_size``) отбрасывается: оглавление
    главы — список ссылок, а не участок документации, и слайс из него измерял бы чтение
    оглавления. Порог здесь не «на глаз»: это тот же бюджет, которым режет чанкер, то
    есть минимальный объём, из которого вообще получается структурный кусок.
    """
    assert choice.anchor is not None
    read: list[tuple[str, SideText, SideText]] = []
    anchor_missing_en = True
    for path in choice.paths[:MAX_PATHS_PER_ANCHOR]:
        if cache.stats.network >= max_fetches:
            break
        en = read_page(cache, EN_BASE, path, extractor=extractor, chunker=chunker, lang="en")
        if en.error or len(en.text) < chunker.chunk_size:
            continue
        if not anchor_chunks(en.chunks, choice.anchor):
            continue
        anchor_missing_en = False
        zh = read_page(cache, ZH_BASE, path, extractor=extractor, chunker=chunker, lang="zh")
        if zh.error or not anchor_chunks(zh.chunks, choice.anchor):
            continue
        read.append((path, en, zh))
    if not read:
        return None, "anchor_absent_zh" if not anchor_missing_en else "no_usable_page"
    read.sort(key=lambda item: (-len(item[1].text), item[0]))
    return read[0], ""


# ---------------------------------------------------------------------------
# Режимы замера
# ---------------------------------------------------------------------------


def run_aligned(
    pairs: Sequence[Pair], *, evaluator: CountingEvaluator, run_id: str, workers: int
) -> list[dict[str, Any]]:
    """Режим ``aligned``: обе стороны получают свой якорный слайс, поиска нет."""

    def measure(pair: Pair) -> dict[str, Any]:
        en = evaluate_slice(param=pair.param, chunks=pair.en_slice, evaluator=evaluator, run_id=run_id)
        zh = evaluate_slice(param=pair.param, chunks=pair.zh_slice, evaluator=evaluator, run_id=run_id)
        return {"status_en": en["status"], "status_zh": zh["status"], "en": en, "zh": zh}

    return _map(measure, pairs, workers)


def run_retrieval(
    pairs: Sequence[Pair],
    *,
    evaluator: CountingEvaluator,
    run_id: str,
    top_k: int,
    workers: int,
) -> list[dict[str, Any]]:
    """Режим ``retrieval``: слайс выбирает запрос Ш6 по лексическому плечу R0.

    Индекс строится **на чанках одной страницы одной стороны** (продукт-скоуп I17), а
    запрос — тот же, что в прогоне: ``run_queries_for_cell`` внутри
    ``retrieve_for_cell``. Профиль — ``R0`` (лексика), движок — ``TantivyLexicalIndex``
    с анализатором ``stemmed``, как в прогоне.
    """

    def measure(pair: Pair) -> dict[str, Any]:
        queries = _queries(pair.param)
        en_hits, en_index = _search_side(pair.en.chunks, pair.param, top_k)
        try:
            zh_hits, zh_index = _search_side(pair.zh.chunks, pair.param, top_k)
        except BaseException:
            en_index.close()
            raise
        try:
            en = evaluate_slice(
                param=pair.param, chunks=en_hits, evaluator=evaluator, run_id=run_id
            )
            zh = evaluate_slice(
                param=pair.param, chunks=zh_hits, evaluator=evaluator, run_id=run_id
            )
        finally:
            en_index.close()
            zh_index.close()
        pattern = anchor_pattern(pair.anchor)
        return {
            "queries": queries,
            "en_hits": len(en_hits),
            "zh_hits": len(zh_hits),
            "en_anchor_hit": any(pattern.search(chunk.text) for chunk in en_hits),
            "zh_anchor_hit": any(pattern.search(chunk.text) for chunk in zh_hits),
            "status_en": en["status"],
            "status_zh": zh["status"],
            "en": en,
            "zh": zh,
        }

    return _map(measure, pairs, workers)


def _queries(param: CatalogRow) -> list[str]:
    """Запросы Ш6 для параметра (тот же вызов, что в фазе оценки)."""
    return list(run_queries_for_cell(param))


def _build_index(chunks: Sequence[Chunk]) -> TantivyLexicalIndex:
    """Лексический индекс по чанкам одной стороны (продукт-скоупед, I17).

    Запись индекса — ``Mapping``, а не сам ``Chunk``: у чанка нет ``product_id`` (он
    принадлежит партиции, а не тексту), а индекс обязан нести продукт — скоуп читается
    из документа индекса, а не из гидртации.
    """
    store = {chunk.chunk_id: chunk for chunk in chunks}
    records = [
        {
            "chunk_id": chunk.chunk_id,
            "product_id": PRODUCT_ID,
            "text": chunk.text,
            "heading_path": chunk.heading_path,
            "content_hash": chunk.content_hash,
        }
        for chunk in chunks
    ]
    index = TantivyLexicalIndex(hydrate=lambda ids: {i: store[i] for i in ids if i in store})
    index.index(records, build_id=f"xling-{len(chunks)}")
    return index


def _search_side(
    chunks: Sequence[Chunk], param: CatalogRow, top_k: int
) -> tuple[list[Any], TantivyLexicalIndex]:
    """Индекс стороны + top-K среза по запросам Ш6 этого параметра."""
    index = _build_index(chunks)
    result = retrieve_for_cell(
        param,
        product_id=PRODUCT_ID,
        profile="R0",
        cutoff_k=top_k,
        lexical_index=index,
    )
    return list(result.chunks), index


def run_query_language_control(
    pairs: Sequence[Pair],
    catalog: Sequence[CatalogRow],
    entries: Sequence[IndexEntry],
    *,
    top_k: int,
    limit: int,
) -> dict[str, Any]:
    """Контроль языка запроса: русский запрос против **обеих** сторон, без латинского якоря.

    Режим ``retrieval`` на выбранных парах отвечает на вопрос «находит ли поиск ZH, когда
    в запросе есть латинский идентификатор» — а такие пары и есть весь замер (якорь
    обязателен для выравнивания). Этот контроль снимает смещение выбора: берутся
    параметры **без** ASCII-якоря (их запрос чисто русский) и те же страницы обеих сторон.
    Если русский запрос не находит ни EN-, ни ZH-страницу, то дело не в языке документа:
    лексическое плечо ищет на языке запроса, и ZH здесь не хуже EN — он так же не найден.

    Вызовов модели нет: мерится только выдача поиска.
    """
    control = [param for param in catalog if choose_anchor(param, entries).anchor is None][:limit]
    pages: dict[str, tuple[SideText, SideText]] = {}
    for pair in pairs:
        pages.setdefault(pair.path, (pair.en, pair.zh))
    cells: list[dict[str, Any]] = []
    for path, (en, zh) in pages.items():
        en_index = _build_index(en.chunks)
        zh_index = _build_index(zh.chunks)
        try:
            for param in control:
                en_hits = retrieve_for_cell(
                    param,
                    product_id=PRODUCT_ID,
                    profile="R0",
                    cutoff_k=top_k,
                    lexical_index=en_index,
                )
                zh_hits = retrieve_for_cell(
                    param,
                    product_id=PRODUCT_ID,
                    profile="R0",
                    cutoff_k=top_k,
                    lexical_index=zh_index,
                )
                cells.append(
                    {
                        "param_id": param.param_id,
                        "path": path,
                        "queries": _queries(param),
                        "en_hits": len(en_hits.chunks),
                        "zh_hits": len(zh_hits.chunks),
                    }
                )
        finally:
            en_index.close()
            zh_index.close()
    # Разделение по **языку запроса**, а не по языку документа: латинский токен в
    # запросе (``postgresql``, ``WAL``) — единственный мост между русским запросом и
    # латиницей в ZH-тексте. Ячейки с чисто русским запросом показывают, что делает
    # лексическое плечо, когда моста нет.
    latin = re.compile(r"[A-Za-z]")
    pure = [cell for cell in cells if not any(latin.search(q) for q in cell["queries"])]
    bridged = [cell for cell in cells if any(latin.search(q) for q in cell["queries"])]
    return {
        "params": [param.param_id for param in control],
        "pages": len(pages),
        "cells": len(cells),
        "en_cells_with_hits": sum(1 for cell in cells if cell["en_hits"] > 0),
        "zh_cells_with_hits": sum(1 for cell in cells if cell["zh_hits"] > 0),
        "pure_ru_query_cells": len(pure),
        "pure_ru_en_cells_with_hits": sum(1 for cell in pure if cell["en_hits"] > 0),
        "pure_ru_zh_cells_with_hits": sum(1 for cell in pure if cell["zh_hits"] > 0),
        "latin_token_cells": len(bridged),
        "latin_token_en_cells_with_hits": sum(1 for cell in bridged if cell["en_hits"] > 0),
        "latin_token_zh_cells_with_hits": sum(1 for cell in bridged if cell["zh_hits"] > 0),
        "detail": cells,
    }


def _map(fn: Any, items: Sequence[Any], workers: int) -> list[dict[str, Any]]:
    """Применяет ``fn`` к элементам, сохраняя порядок; пул — по объявленному числу."""
    if workers <= 1 or len(items) <= 1:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fn, items))


# ---------------------------------------------------------------------------
# Отчёт
# ---------------------------------------------------------------------------


def _fmt_ci(ci: Mapping[str, Any]) -> str:
    low, high = ci.get("low"), ci.get("high")
    if low is None or high is None:
        return "n/a"
    return f"[{low:.3f};{high:.3f}]"


def _fmt_matrix(matrix: Sequence[Mapping[str, Any]]) -> str:
    if not matrix:
        return "  (пусто)"
    return "\n".join(f"  {row['en']} → {row['zh']}: {row['n']}" for row in matrix)


def render_summary(summary: Mapping[str, Any]) -> str:
    """Сводка режима в текстовом виде (то, что читает человек)."""
    kappa = summary["kappa"]
    kappa_text = (
        f"{kappa:.3f} {_fmt_ci(summary['kappa_ci95'])}" if kappa is not None else "n/a"
    )
    lines = [
        f"режим {summary['mode']}: пар {summary['paired']} из {summary['records']} "
        f"(без вердикта {summary['unpaired']})",
        f"  κ = {kappa_text}"
        + (f" ({summary['undefined_reason']})" if kappa is None else ""),
        f"  наблюдаемое согласие = {summary['observed_agreement']:.3f}"
        if summary["observed_agreement"] is not None
        else "  наблюдаемое согласие = n/a",
        "  матрица EN → ZH:",
        _fmt_matrix(summary["matrix"]),
        f"  ✅→❓ (не понял перевод): {summary['ok_to_unknown']}",
        f"  ✅→❌ (перевод сказал обратное): {summary['ok_to_absent']}",
    ]
    deduped = summary.get("deduped")
    if isinstance(deduped, Mapping):
        kappa_d = deduped["kappa"]
        kappa_d_text = (
            f"{kappa_d:.3f} {_fmt_ci(deduped['kappa_ci95'])}" if kappa_d is not None else "n/a"
        )
        lines.append(
            f"  контроль корреляции: слайсов {summary.get('distinct_slices')}, "
            f"κ = {kappa_d_text}, ✅→❓ {deduped['ok_to_unknown']}, "
            f"✅→❌ {deduped['ok_to_absent']}"
        )
    return "\n".join(lines)


def render_rows(mode: str, records: Sequence[Mapping[str, Any]], pairs: Sequence[Pair]) -> str:
    """Построчная таблица: параметр, якорь, путь, длины, статусы, цитаты."""
    head = (
        "| param_id | якорь | путь | EN симв. | ZH симв. | ZH/EN | EN | ZH | цитата EN | цитата ZH |"
        if mode == "aligned"
        else "| param_id | якорь | путь | запросов | EN хитов | ZH хитов | EN якорь | ZH якорь | EN | ZH |"
    )
    lines = [head, "|" + "---|" * (head.count("|") - 1)]
    for pair, record in zip(pairs, records, strict=True):
        block = record[mode]
        if mode == "aligned":
            en_quote = (block["en"]["quotes"] or [""])[0].replace("|", "\\|").replace("\n", " ")
            zh_quote = (block["zh"]["quotes"] or [""])[0].replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| {pair.param.param_id} | {pair.anchor} | {pair.path} | "
                f"{len(pair.en.text)} | {len(pair.zh.text)} | "
                f"{_ratio(len(pair.zh.text), len(pair.en.text))} | "
                f"{block['status_en'] or '—'} | {block['status_zh'] or '—'} | "
                f"{en_quote} | {zh_quote} |"
            )
        else:
            lines.append(
                f"| {pair.param.param_id} | {pair.anchor} | {pair.path} | "
                f"{len(block['queries'])} | {block['en_hits']} | {block['zh_hits']} | "
                f"{'да' if block['en_anchor_hit'] else 'нет'} | "
                f"{'да' if block['zh_anchor_hit'] else 'нет'} | "
                f"{block['status_en'] or '—'} | {block['status_zh'] or '—'} |"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Аргументы прибора (все условия замера — объявленные флаги, не env)."""
    parser = argparse.ArgumentParser(
        description=(
            "Замер: извлекает ли оценщик тот же смысл из ZH-документа, что и из EN "
            "(aligned — изолирует оценщика; retrieval — весь лексический путь R0)"
        )
    )
    parser.add_argument("--mode", choices=("aligned", "retrieval", "both"), default="both")
    parser.add_argument("--catalog", default="packages/ru-pg-dbms/catalog.csv")
    parser.add_argument("--deploy", default="config/deploy.yaml")
    parser.add_argument(
        "--cache-dir",
        default="/tmp/opencode/xling/cache",  # noqa: S108 — объявленный каталог замера
        help="кэш страниц на диске",
    )
    parser.add_argument("--pairs", type=int, default=30, help="сколько пар мерить")
    parser.add_argument("--max-fetches", type=int, default=150, help="потолок сетевых страниц")
    parser.add_argument("--pause", type=float, default=1.0, help="пауза между сетевыми запросами, с")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--top-k", type=int, default=10, help="cutoff_K лексического плеча")
    parser.add_argument("--bootstrap", type=int, default=2000, help="итераций бутстрэпа")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--llm-model", default=None, help="модель оценщика (объявленный вход)")
    parser.add_argument("--llm-slot", default=None)
    parser.add_argument("--llm-concurrency", type=int, default=4)
    parser.add_argument(
        "--control-params",
        type=int,
        default=10,
        help="сколько параметров без латинского якоря взять в контроль языка запроса",
    )
    parser.add_argument("--json", default=None, help="куда писать JSON-артефакт")
    return parser


def _evaluator(args: argparse.Namespace) -> CountingEvaluator:
    """Оценщик тем же путём, что ``hound experiment evaluator-benchmark``."""
    deploy = load_deploy_config(args.deploy)
    try:
        evaluator = build_evaluator_llm(
            deploy_llm=deploy.llm,
            slot=args.llm_slot,
            model_id=args.llm_model,
        )
    except CliError as exc:
        print(f"отказ: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    if evaluator is None:
        print(
            "отказ: LLM endpoint оценщика не сконфигурирован (base_url слота пуст) — "
            "мерить нечем",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return CountingEvaluator(evaluator)


def _load_discovery(cache: PageCache) -> tuple[tuple[str, ...], tuple[IndexEntry, ...]]:
    """Перечень страниц и предметный указатель EN-мануала; без них мерить нечего.

    Отказ называется числом и адресом, а не трассировкой: «хост не отдал sitemap» — это
    условие сети, а не дефект прибора, и путать их нельзя.
    """
    try:
        sitemap = parse_sitemap(cache.get(SITEMAP_URL)[0].decode("utf-8", "replace"))
        entries = parse_index_entries(cache.get(EN_INDEX_URL)[0].decode("utf-8", "replace"))
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print(f"отказ: перечень страниц недоступен ({exc})", file=sys.stderr)
        raise SystemExit(2) from exc
    if not sitemap or not entries:
        print(
            f"отказ: перечень пуст (sitemap={len(sitemap)}, указатель={len(entries)})",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return sitemap, entries


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа: сбор пар → два режима → сводка и JSON-артефакт."""
    args = build_parser().parse_args(argv)
    started = time.monotonic()
    cache = PageCache(
        Path(args.cache_dir),
        pause=args.pause,
        timeout=args.timeout,
        user_agent="coHound-xling-measure/0.1 (research; public docs only)",
    )
    catalog = load_catalog(args.catalog)

    sitemap, entries = _load_discovery(cache)
    chunker = MarkdownChunkerPort()
    extractor = HtmlExtractor()

    pairs, drops, dropped = build_pairs(
        catalog,
        entries,
        cache,
        extractor=extractor,
        chunker=chunker,
        limit=args.pairs,
        max_fetches=args.max_fetches,
    )
    if not pairs:
        print("пар не собрано: якорей нет ни у одного параметра", file=sys.stderr)
        return 1

    evaluator = _evaluator(args)
    run_id = "xling-aligned"
    records: list[dict[str, Any]] = [pair.as_dict() for pair in pairs]
    if args.mode in ("aligned", "both"):
        for record, block in zip(
            records,
            run_aligned(pairs, evaluator=evaluator, run_id=run_id, workers=args.llm_concurrency),
            strict=True,
        ):
            record["aligned"] = block
    if args.mode in ("retrieval", "both"):
        for record, block in zip(
            records,
            run_retrieval(
                pairs,
                evaluator=evaluator,
                run_id="xling-retrieval",
                top_k=args.top_k,
                workers=args.llm_concurrency,
            ),
            strict=True,
        ):
            record["retrieval"] = block

    control: dict[str, Any] | None = None
    if args.mode in ("retrieval", "both") and args.control_params > 0:
        control = run_query_language_control(
            pairs, catalog, entries, top_k=args.top_k, limit=args.control_params
        )

    summaries: dict[str, Any] = {}
    for mode in ("aligned", "retrieval"):
        if any(mode in record for record in records):
            summary = summarize(records, mode, bootstrap=args.bootstrap, seed=args.seed)
            # Контроль корреляции: несколько параметров делят одну страницу/якорь
            # (семь — ``plpgsql-porting``), и такие пары не независимы. Публикуем тот же
            # расчёт по одному представителю на группу ``(якорь, путь)``.
            summary["deduped"] = summarize(
                _one_per_slice(records), mode, bootstrap=args.bootstrap, seed=args.seed
            )
            summary["distinct_slices"] = len(_one_per_slice(records))
            summaries[mode] = summary

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "inputs": {
            "catalog": args.catalog,
            "pairs_requested": args.pairs,
            "pairs_built": len(pairs),
            "sitemap_urls_docs18": len(sitemap),
            "index_entries": len(entries),
            "chunk_size": chunker.chunk_size,
            "chunker_version": chunker.version,
            "reader_version": extractor.version,
            "top_k": args.top_k,
            "pause_s": args.pause,
            "bootstrap": args.bootstrap,
            "seed": args.seed,
            "model_id": getattr(evaluator, "model_id", None),
            "model_version": getattr(evaluator, "model_version", None),
            "decoder": getattr(evaluator, "decoder", None),
            "llm_concurrency": args.llm_concurrency,
        },
        "drops": drops,
        "dropped": dropped,
        "query_language_control": control,
        "records": records,
        "summaries": summaries,
        "cost": {
            **cache.stats.as_dict(),
            "llm_calls": evaluator.calls,
            "seconds_total": round(time.monotonic() - started, 1),
        },
    }

    out_path = Path(args.json) if args.json else Path(args.cache_dir).parent / "cross-lingual-eval.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"пар: {len(pairs)}; sitemap /docs/18/: {len(sitemap)}; указатель: {len(entries)} записей")
    print(
        f"чанкер {chunker.version} chunk_size={chunker.chunk_size}; "
        f"читатель {extractor.version}; модель {payload['inputs']['model_id']}"
    )
    print("отбраковка: " + ", ".join(f"{k}={v}" for k, v in drops.items() if v))
    for summary in summaries.values():
        print()
        print(render_summary(summary))
    for mode in summaries:
        print()
        print(render_rows(mode, records, pairs))
    if control is not None:
        print()
        print(
            "контроль языка запроса: "
            f"{len(control['params'])} параметров × {control['pages']} страниц = "
            f"{control['cells']} ячеек. Чисто русский запрос "
            f"({control['pure_ru_query_cells']} ячеек): EN нашёл чанк в "
            f"{control['pure_ru_en_cells_with_hits']}, ZH — в "
            f"{control['pure_ru_zh_cells_with_hits']}. Латинский токен в запросе "
            f"({control['latin_token_cells']} ячеек): EN — "
            f"{control['latin_token_en_cells_with_hits']}, ZH — "
            f"{control['latin_token_zh_cells_with_hits']}"
        )
    cost = payload["cost"]
    print()
    print(
        "цена: страниц из сети {pages_network}, из кэша {pages_from_cache}, "
        "{mb_network} МБ, сеть {seconds} с, LLM-вызовов {llm_calls}, всего {seconds_total} с".format(
            **cost
        )
    )
    print(f"JSON: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
