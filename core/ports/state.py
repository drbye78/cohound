"""coHound — порт durable per-product состояния ``ProductStatePort`` (09 §4.1/§5/§14.3).

Контракт между ``pipeline`` (резолв режима продукта, планирование инкрементального
корпуса, wiring прогона) и хранилищем ``state/{package_id}/state.db`` (реализация —
``core/store/state_store.py``). Порт описывает поверхность, нужную ``resolve_product_modes``
и durable-субстрату документов (MVP-3a: ``url_state``/``document_version``/``chunk_store``/
``chunk_meta``), а также реестру индексных партиций/композиций (MVP-4 шаг 2:
``index_partition``/``index_composition``, 09 §8.1–§8.2); cell measurement добавляется
следующими срезами по мере появления потребителей.

Строки — простые dataclass'ы (домен остаётся Pydantic; порт может использовать dataclass).
Временные метки — ISO-8601 строки (как они лежат в SQLite TEXT). Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from core.domain.enums import Gate, OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import ProfileAttestationRow, SeedEntry

__all__ = [
    "CellMeasurementRow",
    "ChunkRow",
    "DocumentVersionRow",
    "ForgetProductResult",
    "IndexCompositionRow",
    "IndexPartitionRow",
    "ProductStatePort",
    "ProductStateRow",
    "RunLineageRow",
    "SeedRow",
    "SnapshotRow",
    "UrlStateRow",
]


@dataclass(frozen=True)
class ProductStateRow:
    """Строка ``product_state``: готовность продукта и рубежи последних измерений.

    ``onboarding_state`` — :class:`OnboardingState`; ``last_*_run_id`` — рубежи §5.1
    (``last_corpus_run_id`` / ``last_evaluated_run_id`` / ``last_completed_run_id``).

    ``retired_at``/``re_added_at`` — метки человеческого решения «выведен из работы» и
    «возвращён в работу». ``re_added_at`` не декоративен: при выставленной метке
    :func:`core.pipeline.onboarding._mode_for` даёт ``new`` (полный краул — за время
    отсутствия состояние устарело, §5.1). Пишет их сверка seed-набора
    (:meth:`ProductStatePort.reconcile_seed_set`), а не прогон: retired — решение
    человека в данных (``sources/seeds.yaml``).
    """

    product_id: str
    canonical_name: str
    onboarding_state: OnboardingState
    added_at: str
    added_by: str
    vendor: str | None = None
    onboarding_reason: str | None = None
    #: Момент возврата продукта после ``retired``; ``None`` — не возвращался/метка снята.
    re_added_at: str | None = None
    retired_at: str | None = None
    last_corpus_run_id: str | None = None
    last_evaluated_run_id: str | None = None
    last_completed_run_id: str | None = None
    last_completed_cycle_id: str | None = None
    last_attempt_run_id: str | None = None
    last_attempt_status: str | None = None
    snapshot_digest: str | None = None


@dataclass(frozen=True)
class SnapshotRow:
    """Строка ``product_snapshot``: снимок продукта за прогон (после eval/coverage)."""

    snapshot_id: str
    product_id: str
    cycle_id: str
    run_id: str
    status: str
    scope: str
    coverage_complete: bool
    created_at: str
    coverage_digest: str | None = None
    doc_set_digest: str | None = None
    partition_build_id: str | None = None
    docs_total: int = 0
    docs_reused: int = 0
    docs_new: int = 0
    cells_total: int = 0
    cells_carried: int = 0
    cells_measured: int = 0
    cells_pending: int = 0
    baseline_complete: bool = False


@dataclass(frozen=True)
class SeedRow:
    """Строка ``seed_set``: операционное состояние seed'а (authoring — ``seeds.yaml``)."""

    seed_id: str
    product_id: str
    url: str
    normalized_url: str
    role: SeedRole
    source: SeedSource
    state: SeedState
    added_at: str
    added_by: str
    source_ref: str | None = None
    profile_key: str | None = None
    last_verified_at: str | None = None
    retired_at: str | None = None


@dataclass(frozen=True)
class UrlStateRow:
    """Строка ``url_state``: последняя версия документа URL + условная валидация.

    Поля зеркалят DDL §4.2 (порядок колонок сохранён, кроме обязательных раньше
    опциональных). ``reuse_kind`` — :class:`ReuseKind` в виде строки (порт не навязывает
    доменный enum хранилищу). ``seed_id`` — из какого seed'а обнаружен URL.
    ``next_check_at`` хранится, но в этом срезе планировщик его **не читает** (§6.3).
    """

    product_id: str
    url: str
    normalized_url: str
    doc_version_id: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_http_status: int | None = None
    last_checked_at: str | None = None
    last_changed_at: str | None = None
    next_check_at: str | None = None
    consecutive_failures: int = 0
    last_error_kind: str | None = None
    reuse_kind: str | None = None
    seed_id: str | None = None


@dataclass(frozen=True, kw_only=True)
class DocumentVersionRow:
    """Строка ``document_version``: immutable-версия документа (§4.2/§9).

    Поля идут в порядке DDL §4.2 (``kw_only`` снимает ограничение dataclass на
    «обязательные раньше опциональных»). Уникальность —
    ``UNIQUE (product_id, content_hash, extractor_version, level)``; ``doc_version_id``
    детерминированно выводится из этой же четвёрки
    (:func:`core.pipeline.incremental.doc_version_id`), поэтому PK и UNIQUE не
    расходятся. ``raw_path`` может быть ``NULL`` после retention.
    """

    doc_version_id: str
    doc_id: str
    product_id: str
    url: str
    content_hash: str
    extractor_version: str
    level: str
    raw_path: str | None = None
    chunker_version: str | None = None
    chunk_ids_digest: str | None = None
    first_seen_run_id: str
    last_seen_run_id: str
    first_seen_at: str
    last_seen_at: str
    status: str
    #: Частичность извлечения — **измеренное** свойство версии (не вывод из текста).
    #: ``True`` — прочитана часть (docling ``PARTIAL_SUCCESS``, pdfium: часть страниц не
    #: прочитана), ``False`` — прочитано всё, ``None`` — **не измерено** (строка записана
    #: версией, которая признак не сохраняла). ``None`` нельзя читать как «полное»:
    #: переиспользование такой версии не вправе объявлять покрытие полным молча.
    extract_partial: bool | None = None
    #: Почему частично — как сообщил прибор (''/None при ``extract_partial is not True``).
    extract_partial_reason: str | None = None
    #: Сколько страниц реально попало в текст (``None`` — прибор числа не сообщил).
    extract_pages_converted: int | None = None
    #: Сколько страниц заявлено документом (``None`` — не сообщено).
    extract_page_count: int | None = None


@dataclass(frozen=True)
class CellMeasurementRow:
    """Строка ``cell_measurement``: индекс измерения ячейки для carry-forward (§4.2).

    Ключ — ``(run_id, product_id, param_id)``. Строка называет **чем** измерено
    (``prompt_hash``/``model_id``/``llm_scenario``/``index_build_id``/
    ``partition_build_id``) и **против чего** (``doc_set_digest``/``coverage_digest``), то
    есть служит ключом переноса вердикта: совпал ключ — вердикт переносим.

    Поля ``catalog_row_digest``/``cell_reuse_key``/``evidence_digest`` заполняются
    измерителем, а не импортом: у adoption их нет (``NULL`` — «не вычислено», не «пусто»).
    """

    run_id: str
    product_id: str
    param_id: str
    state: str
    measured_at: str
    status: str | None = None
    prompt_hash: str | None = None
    model_id: str | None = None
    llm_scenario: str | None = None
    index_build_id: str | None = None
    partition_build_id: str | None = None
    doc_set_digest: str | None = None
    coverage_digest: str | None = None
    catalog_row_digest: str | None = None
    cell_reuse_key: str | None = None
    evidence_digest: str | None = None


@dataclass(frozen=True)
class ChunkRow:
    """Строка ``chunk_meta``: метаданные чанка документа (§4.2).

    Текст лежит отдельно в ``chunk_store`` по ``content_hash`` (I19): один и тот же
    текст может разделяться несколькими документами. ``chunk_id`` — идентификатор
    durable-субстрата (:func:`core.pipeline.incremental.chunk_row_id`), это **не**
    run-level ``chunk_id`` вида ``product:doc_hash:no``.
    """

    chunk_id: str
    doc_version_id: str
    product_id: str
    chunk_no: int
    content_hash: str
    chunker_version: str
    heading_path: str | None = None
    context_prefix: str | None = None
    lang: str | None = None


@dataclass(frozen=True)
class IndexPartitionRow:
    """Строка ``index_partition``: иммутабельная продукт-партиция индекса (09 §8.1).

    Ключ — ``(package_id, product_id, partition_build_id)``. ``content_digest`` —
    отпечаток содержимого партиции (sorted ``(chunk_id, content_hash)``), версии —
    входы формулы ``pb``. ``last_used_run_id`` — последний прогон, который партицию
    использовал (наблюдаемость; не влияет на иммутабельность).
    """

    package_id: str
    product_id: str
    partition_build_id: str
    content_digest: str
    chunker_version: str
    embed_model_id: str
    extractor_version: str
    lexical_engine_version: str
    vector_store: str
    store_path: str
    chunks: int
    created_at: str
    dim: int | None = None
    last_used_run_id: str | None = None


@dataclass(frozen=True)
class IndexCompositionRow:
    """Строка ``index_composition``: композиция партиций когорты (09 §8.1/§8.2).

    ``members`` — ``[(product_id, partition_build_id, content_digest)]``; в БД лежит
    JSON-массив (``members_json``). ``composition_id`` — это ``runs.index_build_id``.
    """

    composition_id: str
    package_id: str
    members: tuple[tuple[str, str, str], ...]
    created_at: str


@dataclass(frozen=True)
class ForgetProductResult:
    """Итог забывания продукта: что удалено и куда положен снимок ``state.db``.

    ``counts`` — число удалённых строк по таблицам; ``chunk_store`` считает только
    **осиротевшие** тексты продукта (текст, разделяемый другим продуктом, остаётся).
    ``found=False``/``backup_path=None`` — продукта в состоянии не было: «нечего забывать»
    не ошибка, и снимок тогда не нужен (удалять нечего).
    """

    product_id: str
    found: bool
    counts: dict[str, int]
    backup_path: str | None = None

    @property
    def total(self) -> int:
        """Всего удалённых строк (сумма по таблицам)."""
        return sum(self.counts.values())

    def as_dict(self) -> dict[str, Any]:
        """JSON-представление результата."""
        return {
            "product": self.product_id,
            "found": self.found,
            "counts": dict(self.counts),
            "total": self.total,
            "backup": self.backup_path,
        }


@dataclass(frozen=True)
class RunLineageRow:
    """Строка ``run_lineage``: провенанс прогона в цепочке циклов (§12.3).

    ``products`` — состав когорты прогона (``products_json``). ``None`` — состав
    неизвестен (legacy-строка без колонки), пустой список — прогон без продуктов.
    ``refresh_policy`` — политика наблюдения, с которой запускался прогон.
    """

    run_id: str
    package_id: str
    cycle_id: str
    cohort_digest: str
    seeds_digest: str
    execution_contract_digest: str
    change_class: str
    status: str
    started_at: str
    previous_run_id: str | None = None
    completed_at: str | None = None
    error: str | None = None
    products: list[str] | None = None
    refresh_policy: str | None = None


@runtime_checkable
class ProductStatePort(Protocol):
    """Durable per-product состояние (``state/{package_id}/state.db``, 09 §4.2).

    Идемпотентность: ``upsert_*``/``record_*`` — ``INSERT ... ON CONFLICT DO UPDATE``;
    повторный вызов с теми же данными не создаёт дублей. ``reconcile_seed_set`` не удаляет
    строки: seed, отсутствующий в авторском файле, становится ``disabled`` + ``retired_at``.
    """

    # -- продукт --------------------------------------------------------- #
    def get_product_state(self, product_id: str) -> ProductStateRow | None:
        """Строка продукта или ``None`` (отсутствие — нормально для нового продукта)."""
        ...

    def upsert_product_state(self, row: ProductStateRow) -> None:
        """Создаёт/обновляет строку продукта (идемпотентно)."""
        ...

    # -- снимки ---------------------------------------------------------- #
    def last_snapshot(
        self, product_id: str, *, scope: str = "full"
    ) -> SnapshotRow | None:
        """Последний снимок продукта в заданном ``scope`` (``full``|``product_only``)."""
        ...

    def record_snapshot(self, row: SnapshotRow) -> None:
        """Записывает снимок продукта (идемпотентно по ``snapshot_id``)."""
        ...

    # -- seeds ----------------------------------------------------------- #
    def reconcile_seed_set(
        self, product_id: str, seeds: Sequence[SeedEntry]
    ) -> None:
        """Синхронизирует авторский seed-набор → ``seed_set`` (upsert; без удаления).

        Помимо строк ``seed_set`` метод — **единственный** писатель меток
        ``product_state.retired_at``/``re_added_at`` (09 §5.1–§5.3): ``retired`` — решение
        человека в данных (``sources/seeds.yaml``), поэтому переход ``retired → активен``
        наблюдается здесь, при сверке набора с состоянием, а не командой.

        * пустой набор активных seed'ов ⇒ продукт помечается ``retired``
          (``onboarding_state='retired'`` + ``retired_at``), а строки ``seed_set`` —
          ``disabled`` + ``retired_at``;
        * возврат активного seed'а при прежнем признаке retirement ⇒ ``re_added_at`` =
          момент перехода (и ``retired_at`` снимается): ``_mode_for`` даёт ``new``;
        * ``re_added_at`` снимается, когда после перехода есть **завершённый** прогон
          (``last_completed_run_id`` завершён позже метки) — иначе продукт остался бы
          ``new`` навсегда.
        """
        ...

    def seed_rows(self, product_id: str) -> list[SeedRow]:
        """Все строки seed'ов продукта, включая ``disabled``/``invalid``."""
        ...

    # -- забывание продукта (человеческое решение) ----------------------- #
    def forget_product(
        self, product_id: str, *, at: str | None = None
    ) -> ForgetProductResult:
        """Удаляет строки **состояния** продукта, предварительно сняв копию ``state.db``.

        Условие, а не удобство: снимок делается консистентно (``sqlite3`` backup API) рядом
        с файлом, и при невозможности снимка удаление не выполняется. Удаление — одна
        транзакция (ошибка ⇒ откат). История прогонов (``run_lineage``/``cycle``) и
        композиции индекса не трогаются: это провенанс, а не состояние продукта.
        Продукта в состоянии нет — ``found=False`` (не ошибка), снимок не создаётся.
        """
        ...

    # -- URL/document/chunk (MVP-3a: durable субстрат, 09 §4.2/§6.3) ---- #
    def get_url_state(
        self, product_id: str, normalized_url: str
    ) -> UrlStateRow | None:
        """Состояние URL продукта или ``None`` (URL ещё не наблюдался)."""
        ...

    def iter_url_state(self, product_id: str) -> Iterable[UrlStateRow]:
        """Все строки ``url_state`` продукта в порядке ``normalized_url``."""
        ...

    def upsert_url_state(self, row: UrlStateRow) -> None:
        """Создаёт/обновляет состояние URL (идемпотентно по ``(product_id, normalized_url)``)."""
        ...

    def get_document_version(self, doc_version_id: str) -> DocumentVersionRow | None:
        """Версия документа по ``doc_version_id`` (``None`` — нет)."""
        ...

    def find_document_version(
        self,
        product_id: str,
        *,
        content_hash: str,
        extractor_version: str,
        level: str,
    ) -> DocumentVersionRow | None:
        """Версия по ключу §4.2 ``UNIQUE (product_id, content_hash, extractor_version, level)``."""
        ...

    def upsert_document_version(self, row: DocumentVersionRow) -> None:
        """Создаёт/обновляет версию документа (идемпотентно по ``doc_version_id``)."""
        ...

    def iter_document_versions(
        self, product_id: str, *, status: str = "active"
    ) -> Iterable[DocumentVersionRow]:
        """Версии продукта с заданным ``status`` (по умолчанию только ``active``)."""
        ...

    def put_chunk_text(
        self, content_hash: str, text: str, lang: str | None = None
    ) -> None:
        """Кладёт текст чанка в ``chunk_store`` (идемпотентно по ``content_hash``).

        Текст иммутабелен относительно ``content_hash``: повторная запись обновляет
        только ``lang``, но не текст.
        """
        ...

    def get_chunk_text(self, content_hash: str) -> str | None:
        """Текст чанка по ``content_hash`` (``None`` — текст не сохранён/вытеснен)."""
        ...

    def replace_doc_chunks(
        self, doc_version_id: str, chunks: Sequence[ChunkRow]
    ) -> None:
        """Атомарно заменяет набор ``chunk_meta`` документа (delete + insert).

        Текст должен быть заранее положен в ``chunk_store`` (:meth:`put_chunk_text`).
        Тексты, на которые ссылаются другие документы, не удаляются.
        """
        ...

    def iter_doc_chunks(self, doc_version_id: str) -> Iterable[ChunkRow]:
        """Чанки документа в порядке ``chunk_no`` (детерминированно)."""
        ...

    # -- измерения ячеек (carry-forward, 09 §4.2/§10.1) ------------------ #
    def record_cell_measurements(self, rows: Sequence[CellMeasurementRow]) -> None:
        """Создаёт/обновляет строки ``cell_measurement`` (идемпотентно по составному ключу).

        Ключ — ``(run_id, product_id, param_id)``: повторная запись того же измерения
        обновляет строку, а не плодит дубль. Строки с пустым ``param_id`` не
        записываются (ключ измерения не выдумывается).
        """
        ...

    def iter_cell_measurements(
        self, product_id: str, *, run_id: str | None = None
    ) -> Iterable[CellMeasurementRow]:
        """Измерения продукта (все или только указанного прогона), по ``param_id``."""
        ...

    def get_cell_measurement(
        self, run_id: str, product_id: str, param_id: str
    ) -> CellMeasurementRow | None:
        """Измерение одной ячейки (``None`` — строка не записана).

        Читатель переноса (09 §10.2): по строке прогона-предшественника берётся
        ``cell_reuse_key`` и ``state``; сама ячейка и её evidence живут в БД прогона и
        читаются только после совпадения ключа. ``None`` означает «измерения нет» —
        переносить нечего, и это не «ключ пуст».
        """
        ...

    def save_cell_measurement(self, row: CellMeasurementRow) -> None:
        """Создаёт/обновляет **одно** измерение (идемпотентно по составному ключу).

        Одиночная запись — то, что нужно прогону: измерение пишется сразу после ячейки
        (``observed`` и ``carried`` одинаково), чтобы следующий цикл мог на него опереться.
        Строка с пустым ``param_id`` не записывается: ключ измерения не выдумывается.
        """
        ...

    # -- индексные партиции/композиции (09 §8.1–§8.2) -------------------- #
    def register_index_partition(self, row: IndexPartitionRow) -> None:
        """Создаёт/обновляет строку продукт-партиции (идемпотентно по составному ключу).

        Метаданные иммутабельны (``content_digest``/версии/``created_at`` не меняются при
        повторной регистрации того же ``partition_build_id``); обновляется только
        ``last_used_run_id``.
        """
        ...

    def iter_index_partitions(self, package_id: str) -> Iterable[IndexPartitionRow]:
        """Все известные продукт-партиции пакета (порядок ``product_id, partition_build_id``)."""
        ...

    def save_index_composition(self, row: IndexCompositionRow) -> None:
        """Создаёт/обновляет композицию (идемпотентно по ``composition_id``)."""
        ...

    def get_index_composition(self, composition_id: str) -> IndexCompositionRow | None:
        """Композиция по id или ``None`` (неизвестная/устаревшая строка)."""
        ...

    # -- lineage --------------------------------------------------------- #
    def record_run_lineage(self, row: RunLineageRow) -> None:
        """Записывает/обновляет строку lineage прогона (идемпотентно по ``run_id``)."""
        ...

    def previous_run(self, package_id: str) -> str | None:
        """``run_id`` последнего известного прогона пакета (``None`` — истории нет)."""
        ...

    def get_run_lineage(self, run_id: str) -> RunLineageRow | None:
        """Строка lineage по ``run_id`` или ``None`` (например, ``--since-run``)."""
        ...

    def previous_lineage(self, package_id: str) -> RunLineageRow | None:
        """Последняя строка lineage пакета (``None`` — истории нет)."""
        ...

    # -- аттестация профиля пакета-якоря (G1/G2) ------------------------- #
    def save_profile_attestation(self, row: ProfileAttestationRow) -> None:
        """Создаёт/обновляет аттестацию профиля (идемпотентно по ``attestation_id``).

        Идемпотентность — по детерминированному ``attestation_id`` (см.
        :func:`core.pipeline.profile_attestation.profile_attestation_id`): повторная
        подача того же решения перезаписывает ту же строку, а не плодит дубли.
        **Отзыв необратим повторной записью**: уже проставленный ``superseded_at``
        сохраняется (аттестация не «воскресает» молча).
        """
        ...

    def find_profile_attestation(
        self,
        case_id: str,
        package_id: str,
        anchor: str,
        gate: Gate,
        profile_digest: str,
    ) -> ProfileAttestationRow | None:
        """Аттестация точного дайджеста профиля (``None`` — не аттестован).

        Строка возвращается **независимо** от ``superseded_at``: решение о том, считать
        ли отозванную аттестацию действующей, принимает вызывающий (гейт-резолвер).
        При нескольких строках одного дайджеста возвращается последняя по ``attested_at``.
        """
        ...

    def iter_profile_attestations(
        self,
        *,
        case_id: str,
        package_id: str,
        anchor: str,
        gate: Gate,
        include_superseded: bool = False,
    ) -> Iterable[ProfileAttestationRow]:
        """Аттестации профиля для ``(case_id, package_id, anchor, gate)``.

        Порядок — ``attested_at``, ``attestation_id`` (детерминированно). По умолчанию
        отозванные (``superseded_at IS NOT NULL``) не возвращаются: действующая
        аттестация — только неотозванная.
        """
        ...

    def supersede_profile_attestation(
        self, attestation_id: str, *, superseded_at: str
    ) -> None:
        """Отзывает аттестацию (проставляет ``superseded_at``); идемпотентно.

        Строка не удаляется: аудит-след решений человека сохраняется. Повторный отзыв
        не переписывает уже проставленную метку.
        """
        ...
