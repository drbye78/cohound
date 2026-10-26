"""``sources/manual_artifacts.yaml`` — объявленный вход байтов, которые не взял краулер.

**Зачем.** Регламент называет обход robots/CAPTCHA/платных барьеров запрещённым, а
**ручной drop-in корпуса** — штатным escape hatch с правилом симметрии
(``cohound-spec-v2.md`` §10). Код этого механизма не имел: список недоступного
печатался (``artifacts_gap.yaml``, 13-manual-artifacts §4), но заготовку ``provide``
было нечем заполнить — ни один писатель не читал её, а локального файла как источника
байтов не существовало.

**Что это.** Загрузчик/валидатор/дайджест объявленного входа §3.1. Байты оператора
попадают в корпус **единственным** путём I19 (``RawPage`` → ``_retain`` → экстракция →
чанкер → индекс, ``core/pipeline/corpus.py``); этот модуль байтов не публикует и
evidence не создаёт — он только читает декларацию и считает её отпечаток.

**Почему файл, а не флаг** (§3.1). Ось, заданная флагом, теряется в следующем вызове:
``run_id`` перестал бы описывать корпус. Пакет — носитель авторских входов, и этот вход
авторский: он остаётся в дереве пакета и повторяется на другой машине.

**``sha256`` — сверка, не основание доверия.** Объявлен и не совпал с вычисленным —
видимая ошибка (``ValueError``, CLI отображает в ``EXIT_USAGE``). Не объявлен — хеш всё
равно считается из байтов: оператор не может «объявить» хеш и тем самым провести байты
без проверки. Именно вычисленный хеш идёт в :func:`~core.pipeline.digests.manual_artifacts_digest`
(материал ``(product_id, url, sha256)``): замена файла без правки декларации обязана
менять ``run_id``, иначе корпус изменится, а идентичность измерения — нет.

**Путь** резолвится относительно **каталога пакета**, а не ``cwd``: прогон из другого
каталога иначе молча не нашёл бы файл. Абсолютные пути и выход за корень пакета
отвергаются — декларация должна переезжать вместе с пакетом (воспроизводимость, не
измерение).

**Отсутствие файла — не ошибка**: пустой набор и стабильный дайджест пустого множества.
Ошибка — только объявленный, но нечитаемый/несовпавший артефакт.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from core.pipeline.digests import manual_artifacts_digest
from core.util.security import validate_url

__all__ = [
    "MANUAL_ARTIFACTS_FILE",
    "MANUAL_ARTIFACTS_SCHEMA",
    "ORIGIN_CRAWL",
    "ORIGIN_MANUAL",
    "ManualArtifact",
    "ManualArtifacts",
    "artifact_record",
    "load_manual_artifacts",
    "manual_artifacts_summary",
]

#: Путь декларации относительно каталога пакета (03 §A.1, ``sources/``).
MANUAL_ARTIFACTS_FILE = "sources/manual_artifacts.yaml"

#: Версия схемы файла (меняется только при несовместимой смене формы).
MANUAL_ARTIFACTS_SCHEMA = "manual_artifacts/1"

#: Признак происхождения **байтов** корпуса: обычный краул (дефолт ``corpus_docs.origin``).
ORIGIN_CRAWL = "crawl"

#: Признак происхождения **байтов** корпуса: байты предоставлены человеком (§3.3).
#:
#: Не путать с ``source_kind=manual`` (провенанс **вердикта** человека, I22-исключение):
#: это разные смыслы в разных полях, и ``MANUAL`` вне ``_CORPUS_SOURCE_KINDS`` — то есть
#: не повод пройти мимо хеширования (I19). Здесь же материал проходит ровно ту же цепочку.
ORIGIN_MANUAL = "manual"

#: Размер порции потокового чтения при сверке ``sha256`` (1 MiB: файл не в RAM целиком).
_READ_CHUNK = 1024 * 1024


def _sha256_file(path: Path) -> tuple[str, int]:
    """``(sha256, size)`` файла потоково — байты не поднимаются в RAM целиком."""
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_READ_CHUNK), b""):
            hasher.update(chunk)
            size += len(chunk)
    return hasher.hexdigest(), size


def _text(value: Any, *, field_name: str, where: str) -> str:
    """Непустая строка поля декларации; ``None``/пусто — ``ValueError`` с адресом записи."""
    text = "" if value is None else str(value).strip()
    if not text:
        raise ValueError(f"{where}: поле {field_name!r} обязательно и не может быть пустым")
    return text


def _optional(value: Any) -> str | None:
    """Необязательное строковое поле (провенанс записи): пусто → ``None``."""
    text = "" if value is None else str(value).strip()
    return text or None


@dataclass(frozen=True)
class ManualArtifact:
    """Одна объявленная подача: URL → локальный файл, чей хеш — часть идентичности.

    ``path`` — **разрешённый** путь внутри пакета (существует и прочитан);
    ``declared_path`` — как объявлено (аудит и переносимость декларации). ``sha256``/
    ``size`` — вычислены из **байтов** (объявленный хеш только сверен, §3.1).
    ``reason``/``provided_by``/``provided_at`` — провенанс записи: в контракт
    (``manual_artifacts_digest``) не входят, в аудит — входят.
    """

    product_id: str
    url: str
    path: Path
    declared_path: str
    sha256: str
    size: int
    reason: str | None = None
    provided_by: str | None = None
    provided_at: str | None = None

    @property
    def digest_row(self) -> tuple[str, str, str]:
        """Строка материала ``manual_artifacts_digest`` — ``(product_id, url, sha256)``."""
        return (self.product_id, self.url, self.sha256)

    @property
    def origin(self) -> str:
        """Хост URL (``netloc``) — для человека и ``by_reason``-сводок."""
        return (urlsplit(self.url).netloc or "").lower()


@dataclass(frozen=True)
class ManualArtifacts:
    """Результат загрузки декларации: отсортированные записи + путь файла.

    Пустой набор — валидный результат (файла нет). ``path is None`` означает «декларации
    нет»; ``path`` существует при пустом ``entries`` — файл есть, подач нет (форма
    стабильна, оператор видит, куда писать).
    """

    entries: tuple[ManualArtifact, ...] = ()
    path: Path | None = None

    def for_product(self, product_id: str) -> tuple[ManualArtifact, ...]:
        """Записи продукта в детерминированном порядке (пусто — подач нет)."""
        return tuple(
            entry for entry in self.entries if entry.product_id == str(product_id)
        )

    @property
    def count(self) -> int:
        """Число объявленных подач (не документов корпуса)."""
        return len(self.entries)

    @property
    def total_bytes(self) -> int:
        """Сумма размеров файлов оператора (наблюдаемость, не потолок)."""
        return sum(entry.size for entry in self.entries)

    def digest(self) -> str:
        """Отпечаток декларации для ``manual_artifacts_digest`` (пустой набор — стабилен)."""
        return manual_artifacts_digest(entry.digest_row for entry in self.entries)


def _known_product_ids(package_root: Path) -> set[str] | None:
    """Продукты пакета из ``cohort.yaml``; ``None`` — когорты нет (проверять не по чему).

    Отсутствие/нечитаемость когорты здесь не ошибка: её разберёт обязательный путь
    загрузки входов. Выдумывать «продукт известен» нельзя, поэтому проверка просто
    не применяется.
    """
    cohort_path = package_root / "cohort.yaml"
    if not cohort_path.is_file():
        return None
    from core.config.run_config import load_cohort

    try:
        cohort = load_cohort(cohort_path)
    except (ValueError, OSError):
        return None
    return {entry.product_id for entry in cohort.entries}


def _resolve_artifact_path(
    root: Path, declared: str, *, where: str
) -> Path:
    """Резолвит путь декларации **относительно пакета** и держит его внутри корня.

    :raises ValueError: абсолютный путь либо выход за каталог пакета (в т.ч. через
        ``..``/симлинк).
    """
    raw = Path(declared)
    if raw.is_absolute():
        raise ValueError(
            f"{where}: path {declared!r} абсолютный — путь резолвится относительно "
            "каталога пакета (абсолютный не переезжает вместе с пакетом)"
        )
    candidate = (root / raw).resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError(
            f"{where}: path {declared!r} выходит за каталог пакета {str(root)!r} — "
            "объявленный артефакт обязан лежать внутри пакета"
        )
    return candidate


def load_manual_artifacts(package_dir: str | Path | None) -> ManualArtifacts:
    """Читает и валидирует ``sources/manual_artifacts.yaml`` пакета (§3.1).

    :param package_dir: каталог пакета категории; ``None`` — пакета нет (прямой вызов
        модуля/DI) → пустой набор, ровно как при отсутствии файла.
    :returns: записи, отсортированные по ``(product_id, url)``, и путь файла.
    :raises ValueError: файл есть, но не разбирается/невалиден; объявленный артефакт
        отсутствует, не читается, выходит за пакет или не совпал по ``sha256``; URL не
        проходит SSRF-валидацию; продукт не входит в когорту; дубль ``(product_id, url)``.
        Пустой/отсутствующий файл — **не** ошибка.
    """
    if package_dir is None:
        return ManualArtifacts()
    root = Path(package_dir).resolve()
    path = root / MANUAL_ARTIFACTS_FILE
    if not path.is_file():
        return ManualArtifacts()

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {path}: {exc}") from exc
    if data is None:
        # Пустой файл: форма объявлена, подач нет (стабильный дайджест пустого множества).
        return ManualArtifacts(path=path)
    if not isinstance(data, dict):
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: ожидался YAML-словарь, получено {type(data).__name__}"
        )
    schema = _optional(data.get("schema"))
    if schema != MANUAL_ARTIFACTS_SCHEMA:
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: schema={schema!r}, ожидается "
            f"{MANUAL_ARTIFACTS_SCHEMA!r}"
        )
    raw_entries = data.get("provide")
    if raw_entries is None:
        raw_entries = []
    if not isinstance(raw_entries, list):
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: provide обязан быть списком, "
            f"получено {type(raw_entries).__name__}"
        )

    known = _known_product_ids(root)
    entries: list[ManualArtifact] = []
    seen: set[tuple[str, str]] = set()
    for index, item in enumerate(raw_entries):
        where = f"{MANUAL_ARTIFACTS_FILE}: provide[{index}]"
        if not isinstance(item, dict):
            raise ValueError(
                f"{where}: обязан быть словарём, получено {type(item).__name__}"
            )
        product_id = _text(item.get("product_id"), field_name="product_id", where=where)
        url = _text(item.get("url"), field_name="url", where=where)
        declared_path = _text(item.get("path"), field_name="path", where=where)
        # SSRF/схема — те же ворота, что у любого URL прогона: файл оператора не повод
        # тянуть байты с недопустимого адреса (хоть краулер и не участвует).
        validate_url(url)
        if known is not None and product_id not in known:
            raise ValueError(
                f"{where}: неизвестный product_id {product_id!r} (нет в cohort.yaml)"
            )
        key = (product_id, url)
        if key in seen:
            raise ValueError(
                f"{where}: дубликат подачи {key!r} — один URL продукта, один файл"
            )
        seen.add(key)

        target = _resolve_artifact_path(root, declared_path, where=where)
        if not target.is_file():
            raise ValueError(
                f"{where}: объявленный артефакт не найден или не файл: {declared_path!r} "
                f"(разрешено в {str(target)!r})"
            )
        try:
            digest_value, size = _sha256_file(target)
        except OSError as exc:  # noqa: BLE001 — объявленный, но нечитаемый — ошибка
            raise ValueError(f"{where}: артефакт не читается ({declared_path!r}): {exc}") from exc
        declared_hash = _optional(item.get("sha256"))
        if declared_hash is not None and declared_hash.lower() != digest_value:
            raise ValueError(
                f"{where}: sha256 не совпал с объявленным: объявлено "
                f"{declared_hash.lower()!r}, вычислено {digest_value!r} "
                f"({declared_path!r})"
            )
        entries.append(
            ManualArtifact(
                product_id=product_id,
                url=url,
                path=target,
                declared_path=declared_path,
                sha256=digest_value,
                size=size,
                reason=_optional(item.get("reason")),
                provided_by=_optional(item.get("provided_by")),
                provided_at=_optional(item.get("provided_at")),
            )
        )

    entries.sort(key=lambda entry: (entry.product_id, entry.url))
    return ManualArtifacts(entries=tuple(entries), path=path)


def artifact_record(
    entry: ManualArtifact,
    *,
    run_id: str,
    doc_id: str | None,
    raw_path: str | None,
    created_at: str,
) -> dict[str, Any]:
    """Аудит-строка предоставления (§3.3): провенанс, который не стирает факт отказа.

    Пишется после того, как байты ретенированы и документ создан (``doc_id``) либо не
    создан (``None`` — артефакт не дошёл до корпуса). ``bytes`` — размер **предъявленных**
    байтов, ``declared_path`` — путь в декларации (переносимость), ``raw_path`` —
    фактические retained-байты (I19).
    """
    return {
        "run_id": str(run_id),
        "product_id": entry.product_id,
        "url": entry.url,
        "sha256": entry.sha256,
        "doc_id": doc_id,
        "raw_path": raw_path,
        "declared_path": entry.declared_path,
        "reason": entry.reason,
        "provided_by": entry.provided_by,
        "provided_at": entry.provided_at,
        "byte_size": int(entry.size),
        "created_at": created_at,
    }


def manual_artifacts_summary(
    store: Any,
    run_id: str | None,
    *,
    products: Iterable[str] = (),
) -> dict[str, Any]:
    """Блок ``manual_artifacts`` манифеста (13-manual-artifacts §3.4) — доля числом.

    Регламент требует от механизма **симметрии** («одинаково доступен всем продуктам
    кейса»), но потолок здесь запрещён принципом «не ограничивать, а измерять». Поэтому
    публикуются числа, по которым подмена и асимметрия видны без запрета:

    * ``count`` — сколько **документов** корпуса пришло не от краула (различные
      ``doc_id``). Не «сколько файлов объявлено»: объявленный, но не дошедший до корпуса
      файл документом не стал, и выдавать его за наблюдение нельзя;
    * ``declared`` — сколько подач объявлено (акты). Разница с ``count`` — видимая работа
      оператора, не давшая материала;
    * ``share_of_corpus`` = ``count / <документов корпуса>`` (0.0 при пустом корпусе) —
      та самая доля, которую нельзя ограничивать, но обязано быть видно;
    * ``bytes`` — сумма размеров **предъявленных** байтов, ставших документами;
    * ``by_reason`` — почему краул не смог, по актам: ``reason`` объявлен, аудит;
    * ``by_product`` — документы по продуктам, **включая нули** для всех продуктов плана:
      правило симметрии должно быть видно, а не подразумеваться.

    Читает **персистентный** аудит (``manual_artifacts``), а не память прогона: на resume
    ``holder`` пуст, а манифест обязан называть тот же факт. Сбой чтения не роняет отчёт
    (он вторичен к фазе) — блок тогда пуст, и это честно: «не прочитали», а не «не было».

    :param store: ``StorePort`` прогона (``iter_manual_artifacts``/``iter_corpus_docs``).
    :param run_id: прогон; ``None`` — идентичности нет, блок пуст.
    :param products: продукты плана — нужны, чтобы нули были видны.
    """
    empty: dict[str, Any] = {
        "count": 0,
        "declared": 0,
        "share_of_corpus": 0.0,
        "bytes": 0,
        "by_reason": {},
        "by_product": {str(product): 0 for product in products},
    }
    if store is None or not run_id:
        return empty
    try:
        rows = [dict(row) for row in store.iter_manual_artifacts(str(run_id))]
    except Exception:  # noqa: BLE001 — манифест вторичен к фазе
        return empty
    #: ``doc_id → строка``: одна подача = один документ, но повторная подача того же
    #: документа (resume/дубль URL) не должна считаться дважды.
    resolved: dict[str, dict[str, Any]] = {}
    for row in rows:
        doc_id = row.get("doc_id")
        if doc_id:
            resolved[str(doc_id)] = row
    by_product: dict[str, int] = {str(product): 0 for product in products}
    by_reason: dict[str, int] = {}
    total_bytes = 0
    for row in resolved.values():
        product_id = str(row.get("product_id") or "")
        by_product[product_id] = by_product.get(product_id, 0) + 1
        reason = str(row.get("reason") or "unspecified")
        by_reason[reason] = by_reason.get(reason, 0) + 1
        try:
            total_bytes += int(row.get("byte_size") or 0)
        except (TypeError, ValueError):
            continue
    total_docs = 0
    try:
        total_docs = sum(1 for _ in store.iter_corpus_docs(str(run_id)))
    except Exception:  # noqa: BLE001 — знаменатель может быть недоступен
        total_docs = 0
    count = len(resolved)
    return {
        "count": count,
        "declared": len(rows),
        "share_of_corpus": (count / total_docs) if total_docs else 0.0,
        "bytes": total_bytes,
        "by_reason": dict(sorted(by_reason.items())),
        "by_product": dict(sorted(by_product.items())),
    }
