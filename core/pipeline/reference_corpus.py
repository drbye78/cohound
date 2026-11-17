"""coHound — reference-корпус и разведка фактов (план 27 §4, §7.3, §8).

Reference-корпус — это документация **одного reference-продукта одной версии**,
прочитанная тем же конвейером, что корпус когорты (extractor → chunker → ``content_hash``),
но живущая **вне** ``runs/``: она переживает прогон и переиспользуется между циклами
(документация прошедшей версии не меняется никогда).

```
COHOUND_HOME/reference/{package_id}/{product_id}/{version}/
  reference.db        # corpus_docs + corpus_chunks (та же схема, отдельная БД)
  manifest.json       # build_id, extractor_version, chunker_version, content_digest, docs, chunks
  maps/{map_id}/      # карта: cells.jsonl + score.json + manifest.json (§9)
```

Свойства, которые здесь закреплены кодом:

* **иммутабельность**: новая сборка — новый ``build_id``, правка на месте запрещена
  (как ``index_store/{package}/{build}/{product}``);
* ``build_id = ref_{product}_{version}_{hash(content_digest)[:8]}`` — content-addressed
  идентичность сборки, а не прогона (§7.3: у reference-корпуса нет когорты, сидов и
  покрытия продуктов, поэтому ``run_id`` описывал бы не тот предмет);
* **факты без векторов** (§8.4): задача факта — «найти в документации одного продукта
  одной версии место про параметр P». BM25/детерминированный отбор решает её, а векторы
  добавили бы в провенанс зависимость от сборки.

Разведка здесь **детерминированная** (шаг 7a плана): кандидат — чанк, где встречаются
все термины правила параметра; цитата обязана быть **дословной подстрокой** текста чанка
(I22-дисциплина). LLM-путь (шаг 7b) и карта (§9) — отдельные шаги: первый только выбирает,
что цитировать, вторая оценивает; вердикт о продукте когорты не выносит ни один.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.domain.enums import FactReview, FactStatus
from core.domain.models import BaselineFact, CatalogRow
from core.util.hashing import content_hash, short_hash

__all__ = [
    "REFERENCE_SCHEMA",
    "ReferenceCorpusLayout",
    "ReferenceManifest",
    "derive_facts",
    "reference_build_id",
    "reference_content_digest",
    "select_quote",
]

#: Версия формата ``manifest.json`` reference-корпуса.
REFERENCE_SCHEMA = "reference-corpus/1"

#: Минимальная длина термина запроса (короче — шум: «и», «в», «of»).
MIN_TERM_LEN = 4

#: Предел числа терминов в запросе одного параметра: правило бывает длинной фразой, и
#: требовать все её слова от одного чанка значило бы не найти ничего.
MAX_TERMS = 12

#: Предел длины цитаты (символов) — та же дисциплина, что у прочих цитат корпуса.
MAX_QUOTE_CHARS = 600

_SPLIT_RE = re.compile(r"[^\w\-]+", re.UNICODE)


def reference_build_id(product_id: str, version: str, content_digest: str) -> str:
    """``ref_{product}_{version}_{hash(content_digest)[:8]}`` (§4 плана).

    Идентичность сборки — от **содержимого** прочитанного текста, а не от времени или
    пути: две сборки одной версии с одинаковым текстом обязаны совпасть, иначе
    «пересобрали» и «не пересобирали» стали бы неразличимы.
    """
    digest = short_hash(str(content_digest), 8)
    return f"ref_{product_id}_{version}_{digest}"


@dataclass(frozen=True)
class ReferenceCorpusLayout:
    """Раскладка reference-корпуса одного продукта одной версии (§4).

    ``root`` — ``COHOUND_HOME/reference/{package_id}/{product_id}/{version}``. Путь
    передаётся вызывающим (composition root), а не выводится из окружения: раскладка
    хоста — не измерение, и модуль не должен её угадывать.
    """

    root: Path
    package_id: str
    product_id: str
    version: str

    @property
    def db_path(self) -> Path:
        return self.root / "reference.db"

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.json"

    @property
    def maps_dir(self) -> Path:
        return self.root / "maps"

    def map_dir(self, map_id: str) -> Path:
        return self.maps_dir / map_id

    def ensure(self) -> ReferenceCorpusLayout:
        """Создаёт каталог сборки (идемпотентно)."""
        self.root.mkdir(parents=True, exist_ok=True)
        return self


@dataclass
class ReferenceManifest:
    """``manifest.json`` сборки reference-корпуса (§4).

    Публикуются ровно те величины, которые нужны, чтобы понять **чем** прочитан текст:
    сборка, версии читателя и чанкера, отпечаток содержимого и объёмы. ``observed_at`` —
    провенанс (дата снятия), а не идентичность.
    """

    schema: str
    build_id: str
    package_id: str
    product_id: str
    version: str
    content_digest: str
    docs: int
    chunks: int
    extractor_version: str | None = None
    chunker_version: str | None = None
    observed_at: str | None = None
    roots: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "build_id": self.build_id,
            "package_id": self.package_id,
            "product_id": self.product_id,
            "version": self.version,
            "content_digest": self.content_digest,
            "docs": self.docs,
            "chunks": self.chunks,
            "extractor_version": self.extractor_version,
            "chunker_version": self.chunker_version,
            "observed_at": self.observed_at,
            "roots": list(self.roots),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReferenceManifest:
        return cls(
            schema=str(data.get("schema", REFERENCE_SCHEMA)),
            build_id=str(data["build_id"]),
            package_id=str(data.get("package_id", "")),
            product_id=str(data.get("product_id", "")),
            version=str(data.get("version", "")),
            content_digest=str(data.get("content_digest", "")),
            docs=int(data.get("docs", 0)),
            chunks=int(data.get("chunks", 0)),
            extractor_version=data.get("extractor_version"),
            chunker_version=data.get("chunker_version"),
            observed_at=data.get("observed_at"),
            roots=list(data.get("roots") or ()),
        )


def read_manifest(path: str | Path) -> ReferenceManifest | None:
    """Читает ``manifest.json``; отсутствие — ``None`` (сборки ещё нет)."""
    file = Path(path)
    if not file.is_file():
        return None
    data = json.loads(file.read_text(encoding="utf-8"))
    return ReferenceManifest.from_dict(data)


def write_manifest(path: str | Path, manifest: ReferenceManifest) -> Path:
    """Пишет ``manifest.json`` детерминированно (``sort_keys``, UTF-8, ``\\n``)."""
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest.as_dict(), ensure_ascii=False, sort_keys=True, indent=2)
    file.write_text(text + "\n", encoding="utf-8")
    return file


# ---------------------------------------------------------------------------
# Отбор цитаты (шаг 7a плана: детерминированно, без LLM)
# ---------------------------------------------------------------------------


def _terms(text: str) -> list[str]:
    """Термины запроса из текста правила/заголовка (lowercase, без коротких слов)."""
    out: list[str] = []
    for raw in _SPLIT_RE.split((text or "").lower()):
        term = raw.strip("-")
        if len(term) < MIN_TERM_LEN or term in out:
            continue
        out.append(term)
    return out[:MAX_TERMS]


def parameter_terms(param: CatalogRow) -> list[str]:
    """Термины параметра для поиска места в документации (§8.1 шаг 6).

    Источник — ``title`` + ``rule`` + ``concept_terms``: этого достаточно, чтобы найти
    страницу, где параметр описан, и не требует ни LLM, ни векторов. Порядок и состав
    детерминированы, поэтому повторный recon на тех же данных даёт тот же кандидат (R7).
    """
    parts = [str(param.title or ""), str(param.rule or "")]
    for values in (param.concept_terms or {}).values():
        parts.extend(str(value) for value in values)
    return _terms(" ".join(parts))


def _sentence_around(text: str, term: str) -> str:
    """Предложение, содержащее термин (цитата обязана быть дословной подстрокой)."""
    lowered = text.lower()
    index = lowered.find(term.lower())
    if index < 0:
        return ""
    start = max(text.rfind("\n", 0, index), text.rfind(". ", 0, index)) + 1
    end_candidates = [pos for pos in (text.find("\n", index), text.find(". ", index))]
    ends = [pos for pos in end_candidates if pos > 0]
    end = min(ends) + 1 if ends else len(text)
    quote = text[start:end].strip()
    if len(quote) > MAX_QUOTE_CHARS:
        quote = quote[: MAX_QUOTE_CHARS - 1].rstrip() + "…"
    return quote


def select_quote(text: str, terms: Sequence[str]) -> str | None:
    """Дословная цитата из чанка по терминам правила; ``None`` — места не нашлось.

    Возвращаемое **гарантированно** является подстрокой ``text`` (I22-дисциплина:
    выдуманная цитата отсекается ещё здесь, а не при наследовании). Если терминов нет
    или ни один не встречается — ``None``: «не нашли» отличимо от «нашли пустое».
    """
    if not text:
        return None
    for term in terms:
        quote = _sentence_around(text, term)
        if quote and quote in text:
            return quote
    return None


def reference_content_digest(chunks: Iterable[ChunkText]) -> str:
    """Отпечаток **содержимого** reference-корпуса (вход ``build_id``, §4).

    Материал — отсортированные хеши текстов чанков. Именно содержимое, а не путь и не
    время: «пересобрали» и «не пересобирали» обязаны различаться по тексту, иначе
    ``build_id`` описывал бы раскладку хоста. Порядок канонизируется сортировкой —
    перестановка чанков в БД измерение не меняет.
    """
    material = sorted(
        chunk.content_hash or content_hash(chunk.text) for chunk in chunks
    )
    return short_hash("\n".join(material), 16)


@dataclass
class ChunkText:
    """Минимальный носитель чанка для отбора цитаты (не завязываемся на порт-тип)."""

    chunk_id: str
    doc_id: str
    url: str | None
    text: str
    content_hash: str | None = None


def iter_reference_chunks(db_path: str | Path, *, build_id: str | None = None) -> Iterator[ChunkText]:
    """Читает чанки reference-БД **одним проходом** (порядок ``doc_id, chunk_no``).

    Один проход, а не запрос на документ: индекса по ``corpus_chunks.doc_id`` в схеме нет
    (замер 17.11.2026: ``WHERE doc_id=?`` на каждый документ — полный скан 253k строк), и
    разведка по 1–2 тыс. страниц не имеет права превратиться в квадратичную.
    """
    import sqlite3

    path = Path(db_path)
    if not path.is_file():
        return
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        sql = (
            "SELECT chunk_id, doc_id, url, text, content_hash FROM corpus_chunks"
        )
        params: list[Any] = []
        if build_id is not None:
            sql += " WHERE build_id = ?"
            params.append(build_id)
        sql += " ORDER BY doc_id, chunk_no, chunk_id"
        for chunk_id, doc_id, url, text, digest in conn.execute(sql, params):
            yield ChunkText(
                chunk_id=str(chunk_id),
                doc_id=str(doc_id),
                url=str(url) if url else None,
                text=str(text or ""),
                content_hash=str(digest) if digest else None,
            )
    finally:
        conn.close()


def derive_facts(
    *,
    baseline_id: str,
    version: str,
    chunks: Iterable[ChunkText],
    params: Sequence[CatalogRow],
    corpus_build_id: str,
    observed_at: str,
    coverage_complete: bool = False,
    review: FactReview = FactReview.PENDING,
    reviewed_by: str | None = None,
) -> list[BaselineFact]:
    """Факты по каталогу: детерминированный отбор цитаты по каждому параметру (§8.1).

    Три исхода и ни одного выдуманного:

    * ``present`` — нашлось место, цитата дословна, хеш взят у чанка;
    * ``unclear`` — место не нашлось **или** охват корпуса не полон. Неполнота не даёт
      отсутствия (та же дисциплина, что I2/I2c): «не нашли» при неполном охвате —
      наблюдение о проходе, а не о документации;
    * ``absent`` — только при ``coverage_complete=True``: проход по корпусу версии полон,
      и места нет.

    ``review`` по умолчанию ``pending``: прибор не подписывает за человека, и наследует
    только ``accepted`` (§3.3).
    """
    material: list[ChunkText] = list(chunks)
    facts: list[BaselineFact] = []
    for param in params:
        terms = parameter_terms(param)
        found: tuple[ChunkText, str] | None = None
        for chunk in material:
            quote = select_quote(chunk.text, terms)
            if quote is not None:
                found = (chunk, quote)
                break
        if found is None:
            facts.append(
                BaselineFact(
                    baseline_id=baseline_id,
                    param_id=str(param.param_id),
                    version=version,
                    status=(
                        FactStatus.ABSENT if coverage_complete else FactStatus.UNCLEAR
                    ),
                    observed_at=observed_at,
                    corpus_build_id=corpus_build_id,
                    review=review,
                    reviewed_by=reviewed_by,
                    note=(
                        "no_place_found"
                        if coverage_complete
                        else "no_place_found; coverage_incomplete"
                    ),
                )
            )
            continue
        chunk, quote = found
        if not chunk.url:
            # I15: без адреса цитата не резолвится, значит наследовать нечем. Это не
            # «absent» (место найдено) и не «present» (доказательство не адресуемо) —
            # наблюдение о документе без URL.
            facts.append(
                BaselineFact(
                    baseline_id=baseline_id,
                    param_id=str(param.param_id),
                    version=version,
                    status=FactStatus.UNCLEAR,
                    observed_at=observed_at,
                    corpus_build_id=corpus_build_id,
                    review=review,
                    reviewed_by=reviewed_by,
                    note="quote_found_without_url",
                )
            )
            continue
        digest = chunk.content_hash or content_hash(chunk.text)
        facts.append(
            BaselineFact(
                baseline_id=baseline_id,
                param_id=str(param.param_id),
                version=version,
                status=FactStatus.PRESENT,
                url=chunk.url,
                quote=quote,
                content_hash=digest,
                observed_at=observed_at,
                corpus_build_id=corpus_build_id,
                review=review,
                reviewed_by=reviewed_by,
                note=None,
            )
        )
    return facts


def facts_by_version(
    facts: Iterable[BaselineFact],
) -> dict[str, list[BaselineFact]]:
    """Группирует факты по версии (для ``baseline facts --version`` и отчёта)."""
    out: dict[str, list[BaselineFact]] = {}
    for fact in facts:
        out.setdefault(str(fact.version), []).append(fact)
    return {version: out[version] for version in sorted(out)}
