"""Аномалии ``heading_path`` — числом, а не прилагательным (план 26, O28).

Зачем отдельный артефакт. Дефект извлечения живёт в чанках: страница, где консольный
``pre`` прочитан как setext-заголовок, даёт ``heading_path`` до 3 668 символов, и он
доезжает до промпта оценщика как «контекст». До этого модуля такое число видел только
измерительный харнесс (``scripts/measure_md_chunking.py``), а отчёт прогона чанки не
читал вовсе — то есть дефект был, а публикуемого факта о нём не было.

Что публикуется:

* ``outputs/heading_anomalies.csv`` — по строке на (порог × документ-виновник): сколько
  чанков документа перевалило порог и какой у него самый длинный ``heading_path``;
* блок :func:`heading_anomaly_summary` — те же числа в виде словаря (его кладёт в
  манифест проводка отчёта);
* строка для оператора (:func:`human_line`) — в ``notes`` CLI-отчёта.

Обрезание **не** вводится: длинный ``heading_path`` — дефект извлечения, отчёт обязан
его показать. В публикацию попадает **превью** (``longest_heading_preview``), но длина
(``max_heading_chars``) остаётся полной, поэтому обрезка не скрывает масштаб.

Пороги и формула взяты у харнесса, а не выбраны здесь:

* ``HEADING_CHAR_BUCKETS = (200, 500)`` — публикуемые бакеты хвоста ``heading_path`` в
  символах (строгое ``>``), заведены потому, что p50/p90/p99/max хвост за p99 не
  показывают;
* ``CONTEXT_SHARE_LIMIT = 0.25`` — порог доли контекста (``heading_path`` +
  ``context_prefix``) в тексте чанка; выше него чанк считается контекст-тяжёлым.
  Считается дважды — в символах и (если дан оценщик) в токенах.

Это **публикуемые бакеты, а не гейты**: ничего не блокируют и не отбрасывают.

Токены и символы — **два разных замера, а не прокси один для другого.**

* **Символьный** замер идёт всегда: «контекст длиннее текста» — прямое наблюдение
  дефекта (``context_prefix``/``heading_path`` длиннее самого чанка), токенизатор ему
  не нужен. Публикуется в ``context.chars``.
* **Токенный** замер требует того же оценщика, что у харнесса (``--tokens``: локальный
  ``transformers.AutoTokenizer``, символьный прокси там сознательно **не**
  подставляется), поэтому он — вход ``count_tokens`` с контрактом
  ``TokenGeometry.add`` (``text -> int``). Не дан или упал — ``context.tokens``
  публикуется как **не измеренный** (``measured=False`` и машинная причина), а не как
  ноль и не через выдуманный прокси (A3). Публикуется в ``context.tokens``.

Оба замера делят порог ``CONTEXT_SHARE_LIMIT`` (формула харнесса), но числители у них
разные и не смешиваются: символы против символов, токены против токенов.

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from core.util.ids import parse_source_id

__all__ = [
    "CONTEXT_SHARE_LIMIT",
    "CSV_COLUMNS",
    "HEADING_BUCKET_TOP",
    "HEADING_CHAR_BUCKETS",
    "HEADING_PREVIEW_CHARS",
    "NO_SUBJECT_REASONS",
    "heading_anomaly_summary",
    "human_line",
    "not_computed_note",
    "write_heading_anomalies_artifact",
]

#: Причины «не посчитано», при которых у прогона **нет предмета**: корпусного индекса
#: в нём не было вовсе (``runs.index_build_id`` пуст, store не умеет его читать) либо
#: отчёт строится без хранилища/прогона. Такое молчание — не пропуск измерения: чанков,
#: у которых можно измерить ``heading_path``, в этом прогоне не существует.
#: Остальные причины (``chunks_read_failed``, ``chunks_reader_unavailable``,
#: ``manifest_unavailable``) означают «предмет есть, а числа нет» — о них оператор
#: обязан узнать строкой, иначе отсутствие измерения прочтётся как отсутствие аномалий.
NO_SUBJECT_REASONS: frozenset[str] = frozenset(
    {"store_missing", "run_id_missing", "build_id_unknown"}
)

#: Пороги хвоста ``heading_path`` в символах — публикуемые бакеты харнесса, не гейты.
HEADING_CHAR_BUCKETS: tuple[int, ...] = (200, 500)

#: Порог доли контекста в тексте чанка (формула харнесса). Один порог на два замера:
#: символьный (``len(context) > limit × len(text)``) и токенный
#: (``tokens(context) > limit × tokens(text)``) — числители разные, порог общий.
CONTEXT_SHARE_LIMIT: float = 0.25

#: Сколько документов-виновников показывать в каждом бакете (как у харнесса).
HEADING_BUCKET_TOP: int = 10

#: Длина превью ``heading_path`` для публикации. Полная строка бывает 3 668 символов и
#: в таблицу/манифест не влезает; длина (``max_heading_chars``) публикуется полностью.
HEADING_PREVIEW_CHARS: int = 160

#: Колонки ``outputs/heading_anomalies.csv`` (порядок стабилен: файл читают глазами).
CSV_COLUMNS: tuple[str, ...] = (
    "chunks_total",
    "bucket_chars",
    "bucket_chunks",
    "bucket_documents",
    "bucket_share",
    "doc_rank",
    "doc_id",
    "url",
    "doc_chunks",
    "max_heading_chars",
    "heading_preview",
    "preview_truncated",
    "context_limit",
    "context_chars_heavy_chunks",
    "context_chars_heavy_share",
    "context_tokens_heavy_chunks",
    "context_tokens_heavy_share",
)


def _document_id(chunk: Any) -> str:
    """Адрес документа, который несёт чанк.

    Порт-тип ``Chunk`` (``core/ports/types.py``) не имеет поля ``doc_id``: документ
    восстанавливается из ``chunk_id`` (``product:doc_version:chunk_no``,
    ``core/pipeline/corpus_index.py``). Если у объекта есть явный ``doc_id`` (будущий
    богатый чанк/фикстура), он главнее — разбор ``chunk_id`` лишь запасной путь.

    Возвращается ``product:doc_version`` — устойчивый адрес документа **этого** чанка.
    Это не ``corpus_docs.doc_id`` (``product:content_hash``): связь «версия → документ»
    живёт в ``state.db``, а порт ``iter_chunks`` её не отдаёт. Для отчёта важна
    адресуемость и различение документов, а не имя таблицы-источника.
    """
    explicit = getattr(chunk, "doc_id", None)
    if explicit:
        return str(explicit)
    chunk_id = str(getattr(chunk, "chunk_id", "") or "")
    try:
        ref = parse_source_id(chunk_id)
    except ValueError:
        # Малформед-адрес не теряется: чанк остаётся виден под своим chunk_id, иначе
        # знаменатель и топ разошлись бы молча.
        return chunk_id or "<unknown>"
    return f"{ref.product_id}:{ref.doc_id}"


def _preview(text: str, limit: int) -> tuple[str, bool]:
    """Превью строки для публикации: ``(текст, обрезано?)``; пустая — не обрезана."""
    if limit <= 0 or len(text) <= limit:
        return text, False
    return text[:limit] + "…", True


class _DocTally:
    """Накопитель одного документа в одном бакете: чанки, длина, превью, пример чанка."""

    __slots__ = ("chunks", "max_heading_chars", "preview", "preview_truncated", "sample_chunk_id")

    def __init__(self) -> None:
        self.chunks = 0
        self.max_heading_chars = 0
        self.preview = ""
        self.preview_truncated = False
        self.sample_chunk_id = ""

    def add(self, heading: str, chunk_id: str, preview_chars: int) -> None:
        self.chunks += 1
        if len(heading) > self.max_heading_chars:
            self.max_heading_chars = len(heading)
            self.preview, self.preview_truncated = _preview(heading, preview_chars)
            self.sample_chunk_id = chunk_id


def heading_anomaly_summary(
    chunks: Iterable[Any],
    *,
    buckets: Sequence[int] = HEADING_CHAR_BUCKETS,
    context_share_limit: float = CONTEXT_SHARE_LIMIT,
    count_tokens: Callable[[str], int] | None = None,
    top: int = HEADING_BUCKET_TOP,
    preview_chars: int = HEADING_PREVIEW_CHARS,
) -> dict[str, Any]:
    """Агрегат аномалий ``heading_path`` по потоку чанков.

    :param chunks: объекты с ``chunk_id``/``heading_path``/``context_prefix``/``text``
        (порт-тип ``Chunk``). Поток читается **один** раз: память — по документам и
        превью, а не по всем строкам заголовков.
    :param buckets: пороги в символах; сравнение строгое (``len(heading) > threshold``),
        как у харнесса.
    :param context_share_limit: порог доли контекста в тексте чанка — **общий** для двух
        замеров (формула харнесса). Числители у них разные и не смешиваются:
        символьный замер сравнивает ``len(context)`` с ``limit × len(text)``, токенный —
        ``tokens(context)`` с ``limit × tokens(text)``. Порог один, величины — разные.
    :param count_tokens: счётчик токенов **того же** оценщика, что у харнесса
        (``text -> int``). ``None`` — токенный замер не производится
        (``context.tokens.measured=False``), а символьный от него не зависит.
    :param top: сколько документов-виновников публиковать в каждом бакете.
    :param preview_chars: длина превью самой длинной ``heading_path``.

    :returns: словарь с ключами:

        * ``chunks_total`` — знаменатель (все прочитанные чанки);
        * ``longest_heading_chars`` — максимум по всему потоку;
        * ``buckets`` — ``{str(threshold): {threshold, chunks, documents, share, top}}``,
          где ``top`` — список ``{doc_id, chunks, max_heading_chars,
          longest_heading_preview, preview_truncated, sample_chunk_id}`` (детерминированно:
          по убыванию числа чанков, затем по ``doc_id``);
        * ``context.chars`` — ``{limit, chunks_total, heavy_chunks, heavy_share}``:
          символьный замер, есть **всегда** (токенизатор не нужен);
        * ``context.tokens`` — ``{limit, chunks_total, measured, heavy_chunks,
          heavy_share, reason}``: токенный замер; при ``measured=False`` числа ``None``,
          причина — машинный токен.

    Два замера намеренно разделены: символьный — прямое наблюдение дефекта
    (``context_prefix`` длиннее самого чанка) и не выдаёт себя за токены; токенный
    требует оценщика модели и без него честно «не измерен» (A3).

    Ноль публикуется нулём, а не пропуском: «за порогом ни одного чанка» — измеренный
    факт, и он должен быть отличим от «не считали».
    """
    order = [int(threshold) for threshold in buckets]
    per_doc: dict[int, dict[str, _DocTally]] = {threshold: {} for threshold in order}
    chunks_total = 0
    longest_heading_chars = 0

    heavy_chars = 0
    tokens_measured = count_tokens is not None
    heavy_tokens: int | None = 0 if tokens_measured else None
    tokens_reason: str | None = None if tokens_measured else "tokenizer_unavailable"

    for chunk in chunks:
        chunks_total += 1
        heading = str(getattr(chunk, "heading_path", "") or "")
        heading_chars = len(heading)
        if heading_chars > longest_heading_chars:
            longest_heading_chars = heading_chars
        doc_id = _document_id(chunk)
        chunk_id = str(getattr(chunk, "chunk_id", "") or "")
        text = str(getattr(chunk, "text", "") or "")
        prefix = str(getattr(chunk, "context_prefix", "") or "")
        # Та же сборка контекста, что у харнесса: непустые части через ``\n``.
        context = "\n".join(part for part in (heading, prefix) if part)

        for threshold in order:
            if heading_chars > threshold:
                tally = per_doc[threshold].setdefault(doc_id, _DocTally())
                tally.add(heading, chunk_id, preview_chars)

        # Символьный замер: прямое наблюдение, токенизатор не участвует.
        if len(context) > context_share_limit * len(text):
            heavy_chars += 1

        if tokens_measured and count_tokens is not None:
            try:
                text_tokens = int(count_tokens(text))
                context_tokens = int(count_tokens(context)) if context else 0
            except Exception as exc:  # noqa: BLE001 — сбой счётчика не роняет отчёт
                tokens_measured = False
                heavy_tokens = None
                tokens_reason = f"tokenizer_failed: {type(exc).__name__}: {exc}"
            else:
                if context_tokens > context_share_limit * text_tokens:
                    heavy_tokens = (heavy_tokens or 0) + 1

    buckets_out: dict[str, Any] = {}
    for threshold in order:
        documents = per_doc[threshold]
        ranked = sorted(
            documents.items(), key=lambda pair: (-pair[1].chunks, pair[0])
        )[: max(0, int(top))]
        bucket_chunks = sum(tally.chunks for tally in documents.values())
        buckets_out[str(threshold)] = {
            "threshold": threshold,
            "chunks": bucket_chunks,
            "documents": len(documents),
            "share": (bucket_chunks / chunks_total) if chunks_total else 0.0,
            "top": [
                {
                    "doc_id": doc_id,
                    "chunks": tally.chunks,
                    "max_heading_chars": tally.max_heading_chars,
                    "longest_heading_preview": tally.preview,
                    "preview_truncated": tally.preview_truncated,
                    "sample_chunk_id": tally.sample_chunk_id,
                }
                for doc_id, tally in ranked
            ],
        }

    # ``None`` — «не измерено»; ноль публикуется нулём, чтобы доля была проверяема.
    heavy_share: float | None = None
    if heavy_tokens is not None:
        heavy_share = (heavy_tokens / chunks_total) if chunks_total else 0.0

    return {
        "chunks_total": chunks_total,
        "longest_heading_chars": longest_heading_chars,
        "buckets": buckets_out,
        "context": {
            "chars": {
                "limit": context_share_limit,
                "chunks_total": chunks_total,
                "heavy_chunks": heavy_chars,
                "heavy_share": (heavy_chars / chunks_total) if chunks_total else 0.0,
            },
            "tokens": {
                "limit": context_share_limit,
                "chunks_total": chunks_total,
                "measured": tokens_measured,
                "heavy_chunks": heavy_tokens,
                "heavy_share": heavy_share,
                "reason": tokens_reason,
            },
        },
    }


def _resolve_partitions(store: Any, run_id: str) -> tuple[list[tuple[str, str]], str | None]:
    """Разрешает ``cb_`` → ``pb_`` по ``index_manifest``; ``(партиции, причина)``.

    ``runs.index_build_id`` — композиция когорты (``cb_…``), а ``corpus_chunks.build_id``
    — сборка **продукт-партиции** (``pb_…``). Сравнение ``cb_`` с колонкой ``pb_`` всегда
    даёт «чанков нет» (измерено 06.11.2026, ``evaluate.py``). Поэтому чанки читаются по
    ``partition_build_id`` каждой строки манифеста, с откатом на композицию для legacy-строк
    (``pb := index_build_id``, MVP-4 шаг 1).

    Любая неразрешимость возвращает **причину** и пустой список: «не посчитано» обязано
    быть видимым, а не выглядеть нулём.
    """
    reader = getattr(store, "read_run_index_ref", None)
    if not callable(reader):
        return [], "build_id_unknown: store не умеет читать runs.index_build_id"
    try:
        ref = reader(str(run_id))
    except Exception as exc:  # noqa: BLE001 — провенанс не роняет отчёт, но виден
        return [], f"build_id_read_failed: {type(exc).__name__}: {exc}"
    composition = getattr(ref, "index_build_id", None) if ref is not None else None
    if not composition:
        return [], "build_id_unknown: runs.index_build_id пуст"
    lister = getattr(store, "iter_index_manifest", None)
    if not callable(lister):
        return [], f"manifest_unavailable: у store нет iter_index_manifest (build={composition})"
    try:
        entries = list(lister(str(composition)))
    except Exception as exc:  # noqa: BLE001 — сбой чтения не выдаётся за «нет чанков»
        return [], f"manifest_read_failed: {type(exc).__name__}: {exc}"
    partitions: list[tuple[str, str]] = []
    for entry in entries:
        product_id = getattr(entry, "product_id", None)
        if product_id is None:
            continue
        partition = getattr(entry, "partition_build_id", None) or composition
        partitions.append((str(partition), str(product_id)))
    if not partitions:
        return [], f"manifest_empty: манифест {composition} не называет ни одной партиции"
    return partitions, None


def _iter_partition_chunks(
    store: Any, partitions: Sequence[tuple[str, str]]
) -> Iterable[Any]:
    """Чанки всех партиций: ``iter_chunks(pb, product_id=…)`` по каждой строке манифеста."""
    for partition, product_id in partitions:
        yield from store.iter_chunks(partition, product_id=product_id)


def _fill_top_urls(store: Any, summary: dict[str, Any]) -> None:
    """Добавляет ``url`` к документам-виновникам (по ``sample_chunk_id``), если умеет.

    Разрешение — по одному чанку на документ (не более ``HEADING_BUCKET_TOP`` на бакет),
    поэтому N+1 здесь ограничен публикуемым топом. Сбой резолва не роняет артефакт:
    ``url`` остаётся ``None`` — «не разрешили», а не выдуманный адрес.
    """
    resolver = getattr(store, "resolve_source_url", None)
    if not callable(resolver):
        return
    for bucket in summary.get("buckets", {}).values():
        for entry in bucket.get("top", []):
            chunk_id = entry.get("sample_chunk_id")
            url: str | None = None
            if chunk_id:
                try:
                    resolved = resolver(str(chunk_id))
                except Exception:  # noqa: BLE001 — адрес вторичен к измерению
                    resolved = None
                url = str(resolved) if resolved else None
            entry["url"] = url


def write_heading_anomalies_artifact(
    store: Any,
    run_id: str | None,
    path: str | Path,
    *,
    count_tokens: Callable[[str], int] | None = None,
    buckets: Sequence[int] = HEADING_CHAR_BUCKETS,
    context_share_limit: float = CONTEXT_SHARE_LIMIT,
    top: int = HEADING_BUCKET_TOP,
) -> dict[str, Any]:
    """Пишет ``outputs/heading_anomalies.csv``; агрегат и причину возвращает словарём.

    Разрешение ``cb_``→``pb_`` делается **внутри** (:func:`_resolve_partitions`): без
    него агрегат молча дал бы ноль на реальном прогоне.

    :returns: ``{written, path, reason, summary}``:

        * ``written=True`` — файл записан, ``summary`` — агрегат, ``reason=None``.
          Документы-виновники в ``summary`` получают ещё и ``url``, если store умеет
          ``resolve_source_url`` (разрешается по одному чанку на документ; ``None`` —
          адрес не разрешён, а не выдуман);
        * ``written=False`` — не посчитано, ``summary=None``, ``path=None``, а ``reason``
          называет причину машинным токеном (``build_id_unknown``,
          ``manifest_unavailable``, ``chunks_read_failed: …`` и т. п.). Это видимое
          «не посчитано», а не ноль.

    Сбой чтения чанков не поднимается наружу (отчёт вторичен к фазе), но и не выдаётся
    за пустой корпус: причина возвращается вызывающему.
    """
    not_computed = {
        "written": False,
        "path": None,
        "reason": "store_missing",
        "summary": None,
    }
    if store is None:
        return not_computed
    if not run_id:
        return {**not_computed, "reason": "run_id_missing"}
    partitions, reason = _resolve_partitions(store, str(run_id))
    if reason is not None:
        return {**not_computed, "reason": reason}
    reader = getattr(store, "iter_chunks", None)
    if not callable(reader):
        return {**not_computed, "reason": "chunks_reader_unavailable: у store нет iter_chunks"}
    try:
        summary = heading_anomaly_summary(
            _iter_partition_chunks(store, partitions),
            buckets=buckets,
            context_share_limit=context_share_limit,
            count_tokens=count_tokens,
            top=top,
        )
    except Exception as exc:  # noqa: BLE001 — отчёт вторичен к фазе
        return {
            **not_computed,
            "reason": f"chunks_read_failed: {type(exc).__name__}: {exc}",
        }

    _fill_top_urls(store, summary)

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    context = summary["context"]
    chars = context["chars"]
    tokens = context["tokens"]
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(CSV_COLUMNS), extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        for bucket_key in summary["buckets"]:
            bucket = summary["buckets"][bucket_key]
            rows = bucket["top"] or [None]
            for rank, entry in enumerate(rows, start=1):
                writer.writerow(
                    {
                        "chunks_total": summary["chunks_total"],
                        "bucket_chars": bucket["threshold"],
                        "bucket_chunks": bucket["chunks"],
                        "bucket_documents": bucket["documents"],
                        "bucket_share": bucket["share"],
                        "doc_rank": rank if entry is not None else "",
                        "doc_id": (entry or {}).get("doc_id", ""),
                        "url": (entry or {}).get("url") or "",
                        "doc_chunks": (entry or {}).get("chunks", ""),
                        "max_heading_chars": (entry or {}).get("max_heading_chars", ""),
                        "heading_preview": (entry or {}).get("longest_heading_preview", ""),
                        "preview_truncated": (
                            int(bool(entry.get("preview_truncated"))) if entry else ""
                        ),
                        # Символьный замер есть всегда (токенизатор не нужен).
                        "context_limit": chars["limit"],
                        "context_chars_heavy_chunks": chars["heavy_chunks"],
                        "context_chars_heavy_share": chars["heavy_share"],
                        # Токенный — пустая строка, если оценщика не было: «не измерено».
                        "context_tokens_heavy_chunks": (
                            tokens["heavy_chunks"] if tokens["measured"] else ""
                        ),
                        "context_tokens_heavy_share": (
                            tokens["heavy_share"] if tokens["measured"] else ""
                        ),
                    }
                )
    return {
        "written": True,
        "path": str(target),
        "reason": None,
        "summary": summary,
    }


def not_computed_note(reason: str | None) -> str | None:
    """Строка «не посчитано» для оператора — или ``None``, если предмета не было.

    Различие существенное: прогон без корпусного индекса (``build_id_unknown``) не имеет
    чанков, у которых можно измерить ``heading_path``, — замечание о нём было бы шумом,
    который приучает не читать ``notes``. А прогон, где чанки есть и не прочитались
    (``chunks_read_failed``/``manifest_unavailable``), обязан назвать это: молчание там
    читается как «аномалий нет».

    :param reason: машинный токен из :func:`write_heading_anomalies_artifact`.
    :returns: строка для ``notes`` либо ``None`` (счёт выполнен или считать нечего).
    """
    if not reason:
        return None
    if reason.split(":", 1)[0].strip() in NO_SUBJECT_REASONS:
        return None
    return (
        f"аномалии heading_path не посчитаны: {reason} "
        "(не ноль, а отсутствие измерения)"
    )


def human_line(summary: dict[str, Any] | None) -> str | None:
    """Строка для оператора (``notes`` CLI) или ``None``, если чанков не было.

    Символьный факт «контекст длиннее текста» называется **всегда**, когда он есть:
    он не требует токенизатора и потому не может быть «не измерен». Токенный —
    только при ``measured=True``; иначе строка честно говорит, что он не измерен
    (и почему), вместо того чтобы молчать или выдавать символы за токены.
    """
    if not summary:
        return None
    chunks_total = int(summary.get("chunks_total") or 0)
    if chunks_total <= 0:
        return None
    parts: list[str] = []
    top_doc: str | None = None
    for bucket in summary.get("buckets", {}).values():
        chunks = int(bucket.get("chunks") or 0)
        if chunks <= 0:
            continue
        share = float(bucket.get("share") or 0.0)
        parts.append(
            f"> {bucket['threshold']} симв.: {chunks} чанков "
            f"({share:.3%}) на {bucket['documents']} документах"
        )
        if top_doc is None and bucket.get("top"):
            winner = bucket["top"][0]
            top_doc = (
                f"{winner['doc_id']} ({winner['chunks']} чанков, "
                f"max {winner['max_heading_chars']} симв.)"
            )
    context = summary.get("context") or {}
    chars = context.get("chars") or {}
    if chars:
        heavy_chars = int(chars.get("heavy_chunks") or 0)
        if heavy_chars > 0:
            share = float(chars.get("heavy_share") or 0.0)
            parts.append(
                f"контекст длиннее текста (символы): {heavy_chars} чанков ({share:.3%})"
            )
    tokens = context.get("tokens") or {}
    if tokens.get("measured"):
        heavy_tokens = int(tokens.get("heavy_chunks") or 0)
        if heavy_tokens > 0:
            share = float(tokens.get("heavy_share") or 0.0)
            parts.append(
                f"контекст тяжелее текста (токены): {heavy_tokens} чанков ({share:.3%})"
            )
    elif tokens:
        parts.append(
            f"контекст в токенах не измерен: {tokens.get('reason') or 'нет счётчика'}"
        )
    if not parts:
        return None
    line = "аномалии heading_path: " + "; ".join(parts)
    if top_doc is not None:
        line = f"{line}; виновник: {top_doc}"
    return line
