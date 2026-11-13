"""``sources/manual_artifacts.yaml`` — объявленный вход байтов, которые не взял краулер.

**Зачем.** Регламент называет обход robots/CAPTCHA/платных барьеров запрещённым, а
**ручной drop-in корпуса** — штатным escape hatch с правилом симметрии
(``cohound-spec-v2.md`` §10). Код этого механизма не имел: список недоступного
печатался (``artifacts_gap.yaml``, 13-manual-artifacts §4), но заготовку ``provide``
было нечем заполнить — ни один писатель не читал её, а локального файла как источника
байтов не существовало.

**Что это.** Загрузчик/валидатор/дайджест объявленного входа §3.1 **и** писатель
декларации (:func:`write_manual_artifacts`). Байты оператора попадают в корпус
**единственным** путём I19 (``RawPage`` → ``_retain`` → экстракция → чанкер → индекс,
``core/pipeline/corpus.py``); этот модуль байтов не публикует и evidence не создаёт — он
читает декларацию, считает её отпечаток и дописывает в неё записи (байты кладёт
:mod:`core.pipeline.artifact_fetch`).

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
from core.util.atomic_write import atomic_write_text
from core.util.security import validate_url

__all__ = [
    "MANUAL_ARTIFACTS_FILE",
    "MANUAL_ARTIFACTS_SCHEMA",
    "ORIGIN_CRAWL",
    "ORIGIN_MANUAL",
    "ManualArtifact",
    "ManualArtifactEntry",
    "ManualArtifacts",
    "artifact_record",
    "load_manual_artifacts",
    "manual_artifacts_summary",
    "write_manual_artifacts",
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
    #: Подача сделана человеком **вместо** отказа инструмента по политике (§10).
    #: Флаг — провенанс записи, а не измерение: в ``manual_artifacts_digest`` не входит
    #: (материал — ``(product_id, url, sha256)``), поэтому его наличие/отсутствие в старых
    #: файлах ``run_id`` не меняет.
    robots_override: bool = False

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
        robots_override = item.get("robots_override", False)
        if not isinstance(robots_override, bool):
            # Строка ``"false"`` истинна в Python: тихо принять её значило бы записать
            # противоположный факт. Схема файла та же — меняется только проверка типа.
            raise ValueError(
                f"{where}: robots_override обязан быть bool, получено "
                f"{type(robots_override).__name__} ({robots_override!r})"
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
                robots_override=robots_override,
            )
        )

    entries.sort(key=lambda entry: (entry.product_id, entry.url))
    return ManualArtifacts(entries=tuple(entries), path=path)


@dataclass(frozen=True)
class ManualArtifactEntry:
    """Желаемое состояние одной записи декларации — вход :func:`write_manual_artifacts`.

    Отличается от :class:`ManualArtifact` тем, что здесь нет байтов: ``path`` — как
    объявлено (относительно пакета), ``sha256`` — необязательная сверка. Писатель не
    читает файл и не считает хеш: он пишет объявление, а сверку делает загрузчик —
    иначе объявленный хеш стал бы основанием доверия (§3.1).

    ``None`` в необязательном поле означает «не объявлять» (поле остаётся таким, каким
    было в файле), а не «стереть»: писатель дополняет декларацию, и потеря
    провенанса при правке одного поля была бы тихой подменой аудита.
    """

    product_id: str
    url: str
    path: str
    sha256: str | None = None
    reason: str | None = None
    provided_by: str | None = None
    provided_at: str | None = None
    robots_override: bool = False


def _entry_payload(entry: ManualArtifactEntry) -> dict[str, Any]:
    """Поля записи, которыми владеет писатель (``None`` — поле не объявляется)."""
    payload: dict[str, Any] = {
        "product_id": str(entry.product_id),
        "url": str(entry.url),
        "path": str(entry.path),
    }
    for name in ("sha256", "reason", "provided_by", "provided_at"):
        value = getattr(entry, name)
        if value is not None:
            payload[name] = str(value)
    if entry.robots_override:
        payload["robots_override"] = True
    return payload


def _read_declaration(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Текущий документ и его ``provide`` (неизвестные ключи сохраняются как есть)."""
    if not path.is_file():
        return {"schema": MANUAL_ARTIFACTS_SCHEMA}, []
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {path}: {exc}") from exc
    if loaded is None:
        return {"schema": MANUAL_ARTIFACTS_SCHEMA}, []
    if not isinstance(loaded, dict):
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: ожидался YAML-словарь, получено "
            f"{type(loaded).__name__}"
        )
    schema = _optional(loaded.get("schema"))
    if schema != MANUAL_ARTIFACTS_SCHEMA:
        # Файл чужой схемы не переписывается: «дописать» в него значило бы объявить
        # его текущим, не будучи им.
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: schema={schema!r}, ожидается "
            f"{MANUAL_ARTIFACTS_SCHEMA!r} — файл другой схемы не переписывается"
        )
    raw = loaded.get("provide")
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: provide обязан быть списком, получено "
            f"{type(raw).__name__}"
        )
    records: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(
                f"{MANUAL_ARTIFACTS_FILE}: provide[{index}]: обязан быть словарём, "
                f"получено {type(item).__name__}"
            )
        records.append(item)
    return loaded, records


def write_manual_artifacts(
    package_dir: str | Path,
    entries: Iterable[ManualArtifactEntry],
    *,
    dry_run: bool = False,
) -> tuple[Path, int]:
    """Дописывает/обновляет записи в ``sources/manual_artifacts.yaml`` пакета.

    **Слияние, а не перезапись.** Записи сопоставляются по ``(product_id, url)``;
    существующие записи и **неизвестные ключи** — верхнего уровня и внутри записи —
    сохраняются: файл может нести поля будущих схем, и писатель не имеет права их
    выбрасывать. Обновляются только поля :class:`ManualArtifactEntry`; ``None`` в
    необязательном поле означает «не трогать».

    **Комментарии не сохраняются.** Файл пишется как данные
    (``safe_dump(..., allow_unicode=True, sort_keys=True)``): заголовок шаблона — авторский
    текст, а не данные, и восстанавливать его значило бы притворяться, что файл остался
    шаблоном. Порядок записей — ``(product_id, url)``, порядок ключей — алфавитный.

    **Атомарность и самопроверка.** Запись идёт через temp + ``os.replace``; после неё
    :func:`load_manual_artifacts` обязан пройти. Не прошёл — прежнее содержимое
    возвращается, а наружу летит ``ValueError``: оставить невалидную декларацию писатель
    не вправе, иначе следующий прогон упадёт на входе без следа того, кто её испортил.

    :param package_dir: каталог пакета; файл создаётся при отсутствии.
    :param entries: желаемые записи (порядок не важен).
    :param dry_run: ничего не писать; ``changed`` при этом считается.
    :returns: ``(путь файла, число записей с изменённым хотя бы одним полем)``.
        Новая запись считается изменённой.
    :raises ValueError: файл чужой схемы/не разбирается, либо запись сделала бы декларацию
        невалидной (тогда файл восстановлен).
    """
    root = Path(package_dir).resolve()
    path = root / MANUAL_ARTIFACTS_FILE
    wanted = list(entries)
    existed = path.is_file()
    previous = path.read_text(encoding="utf-8") if existed else None

    document, records = _read_declaration(path)
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = (str(record.get("product_id") or ""), str(record.get("url") or ""))
        if key in index:
            raise ValueError(
                f"{MANUAL_ARTIFACTS_FILE}: дубликат подачи {key!r} — один URL продукта, "
                "один файл (писатель не выбирает между двумя строками)"
            )
        index[key] = record

    changed = 0
    for entry in wanted:
        key = (str(entry.product_id), str(entry.url))
        old = index.get(key)
        merged = dict(old or {})
        merged.update(_entry_payload(entry))
        if old is None or merged != old:
            changed += 1
        index[key] = merged

    if not index and not existed:
        # Пустая подача в отсутствующий файл — не повод создавать файл: «нет декларации»
        # и «декларация без подач» дают один отпечаток, но второе — уже след операции.
        return path, 0

    document["provide"] = [index[key] for key in sorted(index)]
    text = yaml.safe_dump(
        document, allow_unicode=True, sort_keys=True, default_flow_style=False
    )
    if dry_run:
        return path, changed

    atomic_write_text(path, text)
    try:
        load_manual_artifacts(root)
    except ValueError as exc:
        if previous is None:
            path.unlink(missing_ok=True)
        else:
            atomic_write_text(path, previous)
        raise ValueError(
            f"{MANUAL_ARTIFACTS_FILE}: запись сделала бы декларацию невалидной ({exc}); "
            "прежнее содержимое восстановлено"
        ) from exc
    return path, changed


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
        # Подача вместо отказа инструмента по политике (§10). Ключ не входит в схему
        # таблицы аудита (``manual_artifacts``) — он публикуется там, где строку читают
        # как словарь, и не теряет факт «человек сделал это сам».
        "robots_override": bool(entry.robots_override),
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
      правило симметрии должно быть видно, а не подразумеваться;
    * ``robots_override`` — сколько документов подано человеком **вместо** отказа
      инструмента по политике (§10). Число читается из строк аудита; колонки в таблице
      ``manual_artifacts`` пока нет (схема БД не мигрирует), поэтому поле публикуется
      нулём до аддитивного расширения схемы — это видимое ограничение, а не «флаг не
      ставился».

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
        "robots_override": 0,
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
    overrides = 0
    for row in resolved.values():
        product_id = str(row.get("product_id") or "")
        by_product[product_id] = by_product.get(product_id, 0) + 1
        reason = str(row.get("reason") or "unspecified")
        by_reason[reason] = by_reason.get(reason, 0) + 1
        # Отсутствие ключа в старых строках — ``False`` (поле additive: строка аудита,
        # записанная до появления флага, не утверждает «человек обошёл политику»).
        if row.get("robots_override"):
            overrides += 1
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
        "robots_override": overrides,
    }
