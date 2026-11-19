"""coHound — reference-корпус и разведка фактов (план 27 §4, §7.3, §8).

Reference-корпус — это документация **одного reference-продукта одной версии**,
прочитанная тем же конвейером, что корпус когорты (extractor → chunker → ``content_hash``),
но живущая **вне** ``runs/``: она переживает прогон и переиспользуется между циклами
(документация прошедшей версии не меняется никогда).

```
COHOUND_HOME/reference/{package_id}/{product_id}/{version}/
  current.json             # указатель на «текущую» сборку (атомарная запись)
  .lock                    # один писатель на (пакет, продукт, версия)
  raw/{sha256}.bin         # retained-байты, общие для всех сборок (I19, иммутабельны)
  builds/{build_id}/
    reference.db           # corpus_docs + corpus_chunks (та же схема, отдельная БД)
    manifest.json          # build_id, версии читателя/чанкера, content_digest, объёмы, провенанс
    maps/{map_id}/         # карта: cells.jsonl + score.json + manifest.json (§9)
```

Раскладка **per-build**, а не per-version: сборка — это содержимое, и две сборки одной
версии обязаны сосуществовать, не перезаписывая друг друга. Пока ``reference.db`` лежал
прямо в каталоге версии, «пересобрали» означало «стёрли прежнее», то есть терялось
единственное доказательство того, что именно читалось в прошлый раз. Байты при этом
**общие** для сборок: retention content-addressed (``raw/{sha256}.bin``), поэтому
сосуществование сборок не удваивает диск — и иммутабельность байтов следует из имени
файла, а не из обещания.

``current.json`` — указатель, а не порядок каталога: выбор «текущей» сборки по mtime
сделал бы измерение функцией порядка записи на диск (та же дисциплина, что у
``report --with-map``). Нет указателя и нет единственной сборки — отказ, а не догадка.

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
import os
import re
import sqlite3
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from core.domain.enums import FactReview, FactStatus
from core.domain.models import (
    BaselineFact,
    CatalogRow,
    CorpusCoverage,
    CorpusRootCoverage,
)
from core.pipeline.evaluate import CorpusArtifactDates
from core.util.hashing import content_hash, short_hash

__all__ = [
    "CHUNK_ID_SCHEME",
    "REFERENCE_SCHEMA",
    "ChunkDigestSource",
    "ReferenceBuildAmbiguous",
    "ReferenceBuildMissing",
    "ReferenceCorpusLayout",
    "ReferenceManifest",
    "coverage_from_manifest",
    "derive_facts",
    "list_builds",
    "parameter_phrases",
    "parameter_terms",
    "read_artifact_dates",
    "read_current_build",
    "reference_build_id",
    "reference_content_digest",
    "reference_layout",
    "resolve_build",
    "select_quote",
    "write_current_build",
]

#: Версия формата ``manifest.json`` reference-корпуса.
#:
#: ``2`` — раскладка стала per-build (``builds/{build_id}/``) и манифест публикует
#: провенанс прохода (``complete``/``incomplete_reasons``/``skipped``/``errors``).
#: Манифест ``/1`` читается по-прежнему (``from_dict`` не требует новых полей), и это
#: честно: поля не было — значит «полнота не заявлена» (``complete=False``), а не
#: «полно». Раскладка ``/1`` (``reference.db`` прямо в каталоге версии) не
#: поддерживается: сборок в ``builds/`` нет, и выбор сборки отказывает
#: (:class:`ReferenceBuildMissing`), а не читает файл «как получится».
REFERENCE_SCHEMA = "reference-corpus/2"

#: Форма адреса чанка в сборке (``product:doc:chunk_no``, ``core.util.ids.parse_source_id``).
#: Входит в ``content_digest``: адрес — часть содержимого сборки, и его смена при
#: байт-идентичном тексте обязана давать новый ``build_id`` (иначе пересборка ответит
#: «уже была» и оставит неразбираемый адрес).
CHUNK_ID_SCHEME = "product:doc:chunk_no"

#: Имя файла-указателя текущей сборки в каталоге версии.
CURRENT_BUILD_FILE = "current.json"

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

    Внутри ``root`` живут **две разные вещи**: ``raw/`` — retained-байты, общие для всех
    сборок версии (имя файла — хеш содержимого, поэтому общий каталог ничего не путает),
    и ``builds/{build_id}/`` — сборки, каждая со своей БД и манифестом. Поэтому
    ``db_path``/``manifest_path``/``maps_dir`` — **методы от ``build_id``**, а не
    свойства: у версии больше нет одного «того самого» корпуса, есть сборки.
    """

    root: Path
    package_id: str
    product_id: str
    version: str

    @property
    def builds_dir(self) -> Path:
        """Каталог сборок версии (``builds/``)."""
        return self.root / "builds"

    def build_dir(self, build_id: str) -> Path:
        """Каталог одной сборки: ``builds/{build_id}/``."""
        return self.builds_dir / build_id

    def db_path(self, build_id: str) -> Path:
        """БД сборки: ``builds/{build_id}/reference.db``."""
        return self.build_dir(build_id) / "reference.db"

    def manifest_path(self, build_id: str) -> Path:
        """Манифест сборки: ``builds/{build_id}/manifest.json``."""
        return self.build_dir(build_id) / "manifest.json"

    def maps_dir(self, build_id: str) -> Path:
        """Каталог карт сборки: ``builds/{build_id}/maps/`` (§7.3: карта — своя сборка)."""
        return self.build_dir(build_id) / "maps"

    def map_dir(self, build_id: str, map_id: str) -> Path:
        """Каталог одной карты внутри сборки."""
        return self.maps_dir(build_id) / map_id

    @property
    def raw_dir(self) -> Path:
        """Retention байтов версии: ``raw/{sha256}.bin`` (общий для сборок, I19)."""
        return self.root / "raw"

    @property
    def current_path(self) -> Path:
        """Указатель на текущую сборку: ``current.json``."""
        return self.root / CURRENT_BUILD_FILE

    @property
    def lock_path(self) -> Path:
        """Файл лока «один писатель на (пакет, продукт, версия)»: ``.lock``."""
        return self.root / ".lock"

    def ensure(self) -> ReferenceCorpusLayout:
        """Создаёт каталог версии (идемпотентно)."""
        self.root.mkdir(parents=True, exist_ok=True)
        return self


def reference_layout(
    base: str | Path, *, package_id: str, product_id: str, version: str
) -> ReferenceCorpusLayout:
    """Раскладка версии из **корня раскладки хоста** (``paths.reference``).

    Имена каталогов внутри корня — правило этого модуля, а не каждой команды: две копии
    «где лежит сборка» разошлись бы при первом же изменении раскладки, и разведка искала
    бы корпус не там, где его собрали. Сам корень задаёт composition root (``paths`` —
    поле ``HARNESS_FIELDS``), а не модуль: раскладка хоста — не измерение.
    """
    return ReferenceCorpusLayout(
        root=Path(base) / package_id / product_id / version,
        package_id=package_id,
        product_id=product_id,
        version=version,
    )


class ReferenceBuildMissing(LookupError):
    """Сборку выбрать нельзя: её нет (явная/указанная/единственная).

    Сообщение называет путь версии и то, что было объявлено: «сборок нет» и «вы назвали
    сборку, которой нет» — разные факты, и оператору нужен именно второй.
    """


class ReferenceBuildAmbiguous(LookupError):
    """Сборок несколько, а ``current.json`` не указывает ни на одну — выбор неоднозначен.

    ``builds`` — перечень имеющихся сборок: отказ обязан быть действенным («укажите
    ``--build``»), а не просто запретом.
    """

    def __init__(self, message: str, builds: Sequence[str]) -> None:
        super().__init__(message)
        self.builds: list[str] = list(builds)


def list_builds(layout: ReferenceCorpusLayout) -> list[str]:
    """Сборки версии, у которых есть манифест (отсортировано лексикографически).

    Каталог без манифеста — **не** сборка: публикация пишет манифест в staging и
    переименовывает каталог целиком, поэтому недописанная сборка не видна. Сортировка —
    строковая, а не по времени: порядок каталога не должен нести смысл «новее»
    (``resolve_build`` его не использует вовсе).
    """
    builds = layout.builds_dir
    if not builds.is_dir():
        return []
    found: list[str] = []
    for entry in builds.iterdir():
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        if not (entry / "manifest.json").is_file():
            continue
        found.append(entry.name)
    return sorted(found)


def read_current_build(layout: ReferenceCorpusLayout) -> str | None:
    """``build_id`` из ``current.json``; файла нет — ``None``.

    Битый или чужой по форме файл — :class:`ReferenceBuildMissing`, а не ``None``:
    молчаливое «указателя нет» при двух сборках превратилось бы в выбор по каталогу,
    то есть в догадку о том, что именно измерено.
    """
    path = layout.current_path
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReferenceBuildMissing(
            f"указатель текущей сборки не читается: {path} ({type(exc).__name__}: {exc})"
        ) from exc
    build_id = payload.get("build_id") if isinstance(payload, dict) else None
    if not isinstance(build_id, str) or not build_id:
        raise ReferenceBuildMissing(
            f"указатель текущей сборки не содержит build_id: {path}"
        )
    return build_id


def write_current_build(layout: ReferenceCorpusLayout, build_id: str) -> None:
    """Пишет ``current.json`` **атомарно** (временный файл в том же каталоге + ``replace``).

    Атомарность здесь не косметика: указатель читают другие команды (``recon``/``map``),
    и половина JSON на диске выглядела бы как «указателя нет» — то есть как разрешение
    выбрать сборку за читателя.
    """
    layout.root.mkdir(parents=True, exist_ok=True)
    path = layout.current_path
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    text = json.dumps({"build_id": build_id}, ensure_ascii=False, sort_keys=True, indent=2)
    tmp.write_text(text + "\n", encoding="utf-8")
    os.replace(tmp, path)


def resolve_build(layout: ReferenceCorpusLayout, *, explicit: str | None = None) -> str:
    """Какую сборку читать: явная > ``current.json`` > единственная > отказ.

    Порядок — это и есть правило, и оно **не** включает время: ``mtime`` каталога не
    является ни объявленным входом, ни наблюдением о содержимом, поэтому выбор по нему
    сделал бы ответ функцией порядка записи на диск (та же дисциплина, что у
    ``report --with-map``, где карта выбирается по ``map_id``).

    Отказы:

    * :class:`ReferenceBuildMissing` — явная сборка не найдена (сообщение перечисляет
      имеющиеся), ``current.json`` указывает на отсутствующую сборку, либо сборок нет;
    * :class:`ReferenceBuildAmbiguous` — сборок несколько и указателя нет; ``builds``
      перечисляет их, чтобы отказ можно было исправить одной командой.
    """
    builds = list_builds(layout)
    if explicit is not None:
        if explicit not in builds:
            available = ", ".join(builds) if builds else "ни одной"
            raise ReferenceBuildMissing(
                f"сборка {explicit!r} не найдена в {layout.builds_dir} "
                f"(есть: {available})"
            )
        return explicit
    current = read_current_build(layout)
    if current is not None:
        if current not in builds:
            available = ", ".join(builds) if builds else "ни одной"
            raise ReferenceBuildMissing(
                f"current.json указывает на отсутствующую сборку {current!r} "
                f"({layout.current_path}; есть: {available})"
            )
        return current
    if not builds:
        # Подсказку «чем собрать» даёт CLI: слой pipeline не знает имён команд и флагов,
        # а прежняя подсказка здесь называла `recon` — команду, которая корпус **читает**.
        raise ReferenceBuildMissing(
            f"сборок reference-корпуса нет: {layout.builds_dir}"
        )
    if len(builds) == 1:
        return builds[0]
    raise ReferenceBuildAmbiguous(
        f"сборок несколько ({len(builds)}), а current.json отсутствует: "
        f"{layout.root} — укажите сборку явно",
        builds,
    )


@dataclass
class ReferenceManifest:
    """``manifest.json`` сборки reference-корпуса (§4).

    Публикуются ровно те величины, которые нужны, чтобы понять **чем** прочитан текст:
    сборка, версии читателя и чанкера, отпечаток содержимого и объёмы. ``observed_at`` —
    провенанс (дата снятия), а не идентичность.

    **Провенанс прохода — не идентичность.** ``complete``/``incomplete_reasons``/
    ``skipped``/``errors``/``roots`` описывают, как прошёл обход, и в ``build_id`` не
    входят (он — функция содержимого, §4): иначе одна и та же документация, собранная
    дважды, давала бы две сборки из-за одного упавшего запроса. Но публиковать их
    обязательно: ``absent`` в :func:`derive_facts` допустим **только** при
    ``complete=True`` (I2/I2c-дисциплина), и читатель манифеста должен видеть, на каком
    основании ему отказано.

    ``complete=False`` по умолчанию у нового манифеста: «не заявлено» — не «полно».
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
    #: Обход прошёл объявленное пространство до исчерпания (см. ``build_reference_corpus``).
    complete: bool = False
    #: Машинные причины неполноты (``truncated:…``/``skipped:…``), отсортированы.
    incomplete_reasons: list[str] = field(default_factory=list)
    #: Счётчики пропусков по машинным ключам (``SkipReason``): что именно не попало и почему.
    skipped: dict[str, int] = field(default_factory=dict)
    #: Ошибки прохода (отказ корня, сбой фетча/retention/экстракции) — с адресом.
    errors: list[str] = field(default_factory=list)

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
            "complete": self.complete,
            "incomplete_reasons": list(self.incomplete_reasons),
            "skipped": dict(sorted(self.skipped.items())),
            "errors": list(self.errors),
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
            complete=bool(data.get("complete", False)),
            incomplete_reasons=[str(item) for item in (data.get("incomplete_reasons") or ())],
            skipped={
                str(key): int(value)
                for key, value in (data.get("skipped") or {}).items()
            },
            errors=[str(item) for item in (data.get("errors") or ())],
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

    Источник — ``title`` + ``concept_terms``: этого достаточно, чтобы найти страницу,
    где параметр описан, и не требует ни LLM, ни векторов. Порядок и состав
    детерминированы, поэтому повторный recon на тех же данных даёт тот же кандидат (R7).

    **Текст правила в термины не входит, и это замер, а не вкус.** ``rule`` — служебная
    формулировка каталога («Засчитывается наличие явно указанного значения в исходной
    comparative table…»), и её слова встречаются в документации предка как обычная
    лексика: на ``ru-pg-dbms``/PG 18 recon дал **16 ложных ``present`` из 49**
    (18.11.2026) — параметр «Разработчик» подтверждался цитатой
    ``| X040 |  | Basic table mapping |  |``, и разные параметры получали одну и ту же
    цитату, потому что совпадали по слову ``table``. ``present`` обязан опираться на
    цитату, которая называет **параметр**, а не слово из процедуры его проверки; иначе
    провенанс наследования описывает каталог, а не предка.

    Покрытие от этого не падает: ``title`` + ``concept_terms`` дают термины у всех 181
    параметра пакета (замер 18.11.2026). Параметр без терминов не «угадывается» по
    правилу — он остаётся ``unclear``, и :func:`run_recon` называет его числом.
    """
    parts = [str(param.title or "")]
    for values in (param.concept_terms or {}).values():
        parts.extend(str(value) for value in values)
    return _terms(" ".join(parts))


def parameter_phrases(param: CatalogRow) -> list[str]:
    """Фразы параметра для **зачёта** цитаты: авторские термины целиком, без разбиения.

    Запрос и зачёт — разные задачи, и это замер, а не вкус. Запрос (:func:`parameter_terms`)
    строит BM25-запрос, поэтому его материал обязан быть словами: индекс всё равно
    токенизирует. Зачёт же отвечает на вопрос «называет ли предложение **параметр**», и
    слово — слишком слабый ответ: ``point-in-time recovery`` превращалось в три слова, и
    любое из них давало повод совпасть случайно.

    Замер 18.11.2026 на reference-корпусе PG 18 (9 763 чанка, ``top_k=20``, каталог
    ``ru-pg-dbms`` с авторскими ``concept_terms``): кандидатов во всех вариантах даёт один
    и тот же запрос :func:`parameter_terms`, различается только условие зачёта —
    любое слово **165/181** ``present`` при 154 уникальных цитатах и **11** цитатах,
    делившихся между параметрами; все слова термина 86/181, 85, 1; термин целиком
    **37/181**, 37, **0**. Доля делящихся цитат — структурный дефект: цитата, обслуживающая
    2–8 параметров, параметр не называет (на старом каталоге одна цитата
    ``'Oracle Compatibility Mode #'`` приходилась на 8 параметров), а именно это и есть
    ложный ``present``.

    Материал — значения ``concept_terms`` всех языков, каждое значение **целиком** (strip,
    пустые отброшены, дубликаты убраны). Порядок детерминирован: языки в отсортированном
    порядке ключей (``en`` < ``ru``), внутри языка — авторский порядок, поэтому повторный
    recon на тех же данных даёт тот же зачёт (R7). Авторских терминов нет — fallback на
    ``title``: заголовок непуст у всех 181 параметра пакета (замер 18.11.2026), поэтому
    fallback не оставляет параметр без зачёта.

    ``rule`` в фразы не входит по той же причине, что и в :func:`parameter_terms` (там
    описан замер 16 ложных ``present`` из 49): процедура проверки описывает каталог, а не
    предка.
    """
    phrases: list[str] = []
    for lang in sorted(param.concept_terms or {}):
        for value in param.concept_terms[lang]:
            phrase = str(value or "").strip()
            if phrase and phrase not in phrases:
                phrases.append(phrase)
    if phrases:
        return phrases
    fallback = str(param.title or "").strip()
    return [fallback] if fallback else []


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
        # Режем ЖЁСТКО и без многоточия: «…» делает цитату не-подстрокой, а тогда
        # `select_quote` ниже отбрасывает найденный термин и отвечает «не нашли» —
        # ложный отрицательный результат из-за косметики. Замер 18.11.2026 на PG 18
        # (9 763 чанка, 181 параметр): таких потерь 0, но цена дефекта — молчаливое
        # «нет информации в корпусе», поэтому обрезка остаётся подстрокой.
        quote = quote[:MAX_QUOTE_CHARS].rstrip()
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


class ChunkDigestSource(Protocol):
    """Что нужно отпечатку содержимого от чанка: ``text`` и (опционально) его хеш.

    Протокол, а не конкретный класс: отпечаток считается и по :class:`ChunkText`
    (разведка), и по ``CorpusChunk``/``Chunk`` порта (сборка) — вторая копия формулы
    «что такое содержимое сборки» разошлась бы с первой молча.
    """

    text: str
    content_hash: str | None


def reference_content_digest(
    chunks: Iterable[ChunkDigestSource], *, address_scheme: str | None = None
) -> str:
    """Отпечаток **содержимого** reference-корпуса (вход ``build_id``, §4).

    Материал — отсортированные хеши текстов чанков. Именно содержимое, а не путь и не
    время: «пересобрали» и «не пересобирали» обязаны различаться по тексту, иначе
    ``build_id`` описывал бы раскладку хоста. Порядок канонизируется сортировкой —
    перестановка чанков в БД измерение не меняет.

    ``address_scheme`` — **форма адреса чанка** (``product:doc:chunk_no``), и она входит в
    отпечаток, потому что адрес — часть содержимого сборки, а не оформление: он попадает в
    ``corpus_chunks.chunk_id`` и оттуда в ``source_id`` evidence. Дефект, ради которого
    параметр введён (19.11.2026): формат адреса сменили, текст остался байт-в-байт, отпечаток
    совпал, ``build_id`` совпал — и пересборка ответила «уже была, ничего не перезаписано»,
    оставив в каталоге БД со старым, неразбираемым адресом. Один и тот же текст, адресованный
    по-разному, — **разные сборки**, ровно как разные тексты.

    ``None`` — вызывающий схему не объявил: отпечаток считается по одним текстам (прежнее
    поведение, нужно только тестам и чтению старых записей).
    """
    material = sorted(
        chunk.content_hash or content_hash(chunk.text) for chunk in chunks
    )
    if address_scheme is not None:
        material.append(f"address:{address_scheme}")
    return short_hash("\n".join(material), 16)


@dataclass
class ChunkText:
    """Минимальный носитель чанка для отбора цитаты (не завязываемся на порт-тип).

    ``heading_path`` — раздел документа: нужен выдаче ``hound search --scope reference``
    (по нему человек понимает, куда смотреть), для отбора цитаты не используется.
    ``None`` — раздел не записан, а не «раздела нет»: колонка есть у всех сборок,
    созданных хранилищем, но у чужого файла её может не быть.
    """

    chunk_id: str
    doc_id: str
    url: str | None
    text: str
    content_hash: str | None = None
    heading_path: str | None = None


def read_artifact_dates(
    db_path: str | Path, product_id: str
) -> CorpusArtifactDates:
    """Даты наблюдения артефактов **reference-сборки** — носитель I2d для карты (§9).

    Зачем отдельный читатель. У прогона когорты даты приходят через ``StorePort``
    (:func:`core.pipeline.evaluate.observe_corpus_artifact_dates`), но у карты стора нет:
    она читает файл сборки. Правило при этом одно и то же, поэтому и форма ответа та же —
    :class:`CorpusArtifactDates`, а не второй вид дат.

    **Две формы адреса — два источника даты.** ``corpus_artifact`` +
    ``corpus_doc_version`` дают основную форму (``dv_...``): так адресует прогон когорты.
    Reference-сборка адресует документ legacy-формой ``{product}:{content_hash}``, поэтому
    вторая половина индекса строится по ``corpus_docs`` (``doc_id`` + ``fetched_at``).

    **Пустой индекс — законный и значимый ответ.** Сборка без датированных документов
    даёт пустой индекс, и ``❌`` класса ``explicit``/``authoritative`` понижается до
    ``❓`` с токеном ``absence_undated`` (I2d). Замер 18.11.2026: карта PG 18
    опубликовала ``❌`` без датированного носителя именно потому, что читатель смотрел
    только в пустую ``corpus_artifact``. Поэтому читатель возвращает индекс, а не
    ``None``: ``None`` означал бы «читать нечем» и оставил бы ``❌`` в силе.
    """
    by_doc_version: dict[str, str | None] = {}
    by_doc: dict[str, str | None] = {}
    conn = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True)
    try:
        for version, observed_at in conn.execute(
            "SELECT doc_version_id, observed_at FROM corpus_artifact WHERE product_id = ?",
            (product_id,),
        ):
            key = str(version or "").strip()
            if not key:
                continue
            by_doc_version[key] = _earliest(by_doc_version.get(key), observed_at)
        try:
            versions = list(
                conn.execute(
                    "SELECT doc_version_id, doc_id FROM corpus_doc_version WHERE product_id = ?",
                    (product_id,),
                )
            )
        except sqlite3.OperationalError:  # pragma: no cover — таблицы нет в старых файлах
            versions = []
        for version, doc_id in versions:
            key = str(version or "").strip()
            if key not in by_doc_version:
                # Версия без строки артефакта даты не имеет: «не найдено» — честный
                # ответ, а ``None`` здесь означал бы наблюдение, которого нет.
                continue
            by_doc[str(doc_id)] = _earliest(by_doc.get(str(doc_id)), by_doc_version[key])
        # Legacy-адрес ссылки — ``{product}:{content_hash}``, и у reference-сборки это
        # **единственная** форма: ``corpus_docs.doc_id`` уже имеет её вид, а
        # ``fetched_at`` — дата наблюдения документа (замер 18.11.2026: 1 136 из 1 136
        # документов PG 18 датированы, ``corpus_artifact`` при этом пуста — она пишется
        # прогоном когорты, а не сборкой reference-корпуса). ``reused`` дату не отменяет:
        # I2d прямо допускает, что дата наблюдения не из этого цикла.
        try:
            docs = list(
                conn.execute(
                    "SELECT doc_id, fetched_at FROM corpus_docs"
                    " WHERE product_id = ? AND fetched_at IS NOT NULL",
                    (product_id,),
                )
            )
        except sqlite3.OperationalError:  # pragma: no cover — таблицы нет в чужом файле
            docs = []
        for doc_id, fetched_at in docs:
            key = str(doc_id or "").strip()
            if not key:
                continue
            by_doc[key] = _earliest(by_doc.get(key), fetched_at)
    finally:
        conn.close()
    return CorpusArtifactDates(
        product_id=product_id, by_doc_version=by_doc_version, by_doc=by_doc
    )


def _earliest(current: str | None, candidate: str | None) -> str | None:
    """Самая ранняя из двух дат ISO (слабая опора: документ наблюдался не позже)."""
    if current is None:
        return candidate
    if candidate is None:
        return current
    return min(current, candidate)


def coverage_from_manifest(
    manifest: ReferenceManifest | None, product_id: str
) -> CorpusCoverage:
    """Покрытие reference-сборки по её манифесту — вход I2c/I2d для карты (§9).

    Сборка объявляет ``complete`` и корни, которые обошла, с датой снятия. Это и есть
    «пространство, исчерпанное снятием C от даты D»: ``bounded`` опирается на корни,
    ``explicit``/``authoritative`` — на даты документов (см. :func:`read_artifact_dates`).

    Манифеста нет (чужая сборка, ``--corpus-db``) — покрытие **не заявлено**:
    ``complete=False`` и пустые корни. Это не «неполно», а «полнота не объявлена», и
    следствие то же, что у I2c: ``bounded`` не публикуется.
    """
    roots: list[CorpusRootCoverage] = []
    complete = bool(manifest.complete) if manifest is not None else False
    if manifest is not None:
        for root in manifest.roots:
            roots.append(
                CorpusRootCoverage(
                    root_url=root,
                    capture_id=manifest.build_id,
                    complete=bool(manifest.complete),
                    observed_how="live",
                    observed_at=manifest.observed_at,
                    policy_digest=None,
                )
            )
    return CorpusCoverage(
        product_id=product_id,
        complete=complete and bool(roots),
        roots=roots,
        expected_sources=0,
        observed_sources=0,
        # Момент вычисления — не дата наблюдения: дата носителя живёт в ``roots`` и в
        # ``artifact_dates``, а это поле только помечает, когда считали покрытие.
        computed_at=(
            manifest.observed_at if manifest is not None and manifest.observed_at else ""
        ),
    )


def iter_reference_chunks(db_path: str | Path, *, build_id: str | None = None) -> Iterator[ChunkText]:
    """Читает чанки reference-БД **одним проходом** (порядок ``doc_id, chunk_no``).

    Один проход, а не запрос на документ: индекса по ``corpus_chunks.doc_id`` в схеме нет
    (замер 17.11.2026: ``WHERE doc_id=?`` на каждый документ — полный скан 253k строк), и
    разведка по 1–2 тыс. страниц не имеет права превратиться в квадратичную.

    Раздел (``heading_path``) читается **тем же** проходом, а не отдельным запросом на
    документ: выдача без раздела бесполезна человеку, но добирать его по ``doc_id``
    значило бы вернуть ту самую квадратичность.
    """
    import sqlite3

    path = Path(db_path)
    if not path.is_file():
        return
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        sql = (
            "SELECT chunk_id, doc_id, url, heading_path, text, content_hash "
            "FROM corpus_chunks"
        )
        params: list[Any] = []
        if build_id is not None:
            sql += " WHERE build_id = ?"
            params.append(build_id)
        sql += " ORDER BY doc_id, chunk_no, chunk_id"
        for chunk_id, doc_id, url, heading, text, digest in conn.execute(sql, params):
            yield ChunkText(
                chunk_id=str(chunk_id),
                doc_id=str(doc_id),
                url=str(url) if url else None,
                text=str(text or ""),
                content_hash=str(digest) if digest else None,
                heading_path=str(heading) if heading else None,
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

    Зачёт идёт по **фразе** (:func:`parameter_phrases`), а не по слову: слово давало
    ложный ``present`` (165 против 37 на 181 параметре, 18.11.2026) и цитату, делившуюся
    между 2–8 параметрами. Цитата обязана называть параметр, а не содержать одно из слов
    его названия.
    """
    material: list[ChunkText] = list(chunks)
    facts: list[BaselineFact] = []
    for param in params:
        phrases = parameter_phrases(param)
        found: tuple[ChunkText, str] | None = None
        for chunk in material:
            quote = select_quote(chunk.text, phrases)
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
