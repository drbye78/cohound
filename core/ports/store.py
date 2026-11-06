"""Порт операционного хранилища — ``StorePort`` / ``GateStore`` (§4.4, 02 §B, S6, S9).

Единая SQLite-БД прогона (``runs/{run_id}/cohound.db``) — вариант «отдельная корпусная БД»
отвергнут (S6): FK между ``cell_evidence.source_id`` и ``corpus_chunks.chunk_id`` сохраняются.

Контракт описывает полную поверхность доступа из 03 §B.1 (T3.1). Реализация —
``core.store.sqlite_store.SqliteStore``. Оба протокола структурные
(``typing.Protocol``) и ``runtime_checkable``: проверка — по наличию методов, без
наследования. Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from core.domain import (
    ArchiveObservation,
    Cell,
    CellEvidence,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CorpusMember,
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
    ExtractQueueItem,
    ExtractQueueState,
    GateDecision,
    IndexManifestEntry,
    ProtocolExhaustion,
    RegistryFact,
    RunConfig,
    RunManifest,
    ScenarioBinding,
    SearchLogEntry,
)
from core.ports.types import Chunk


@dataclass(frozen=True)
class RunIndexRef:
    """Ссылка на индексную сборку прогона из строки ``runs`` (P4b, нейтрально, §13/I17).

    Несёт ровно то, что нужно пост-хок команде эксперимента, чтобы найти **зафиксированную**
    индексную сборку **без** сканирования каталогов ``index_store/``: объявленный пакет
    (``runs.package_id``) и идентификатор сборки (``runs.index_build_id``; заполняется
    персистенцией манифеста на фазе отчёта). ``None``-поля — значения не выдумываются:
    отсутствие — явное.
    """

    run_id: str
    package_id: str | None = None
    index_build_id: str | None = None


@dataclass(frozen=True)
class CorpusArtifactRow:
    """Строка ``corpus_artifact``: проекция наблюдения артефакта в БД прогона (план 20 §F2).

    Зачем проекция, если наблюдение уже durable. Наблюдение живёт в ``state.db``
    (``capture``/``url_state``), а Ш9 и пост-хок ``hound report`` открывают только
    ``runs/{run_id}/cohound.db``. Без этой строки дата артефакта в отчёте либо
    выдумывалась бы, либо требовала второго источника, которого у отчёта нет.

    Поля-даты ``None`` — **наблюдения нет** (ровно ``None``, никогда ``''``):
    артефакт мог войти в корпус без сетевого наблюдения (adoption, субстрат без
    ``observed_at``). Документ в корпусе при этом — факт, поэтому строка существует и без
    даты. ``doc_version_id == ''`` — «версия не записана» (legacy-путь), а не пропуск
    строки. ``root_url`` — корень снятия, на которое ссылается ``capture_id`` (``None``,
    если наблюдения/снятия нет): связь артефакта с корнем нужна ``stale_roots`` (§6).
    """

    run_id: str
    product_id: str
    #: Ключ адреса :func:`core.pipeline.digests.snapshot_url_key` — тот же, что в снимке.
    url_key: str
    doc_version_id: str | None = None
    observed_at: str | None = None
    observed_how: str | None = None
    capture_id: str | None = None
    root_url: str | None = None


class PlanInputsConflict(ValueError):
    """Снимок ``run_inputs`` прогона уже записан с **другим** ``payload_sha256`` (O11).

    ``run_inputs`` — идентичность прогона, а не кэш входов: снимок пишется один раз
    (09 §3.3/§14.2). Повторная запись идентичного снимка — no-op (resume не сдвигает
    время снимка), а расхождение означает, что предъявлены входы **другого** прогона.
    Молчаливая перезапись сделала бы завершённый прогон неотличимым от неисполнявшегося,
    поэтому расхождение — отказ, а не ``REPLACE``.

    Объявлено в порте (контракт), а не в ``core.store``: реализация импортирует его
    отсюда (матрица зависимостей ``ports ↛ store``, 01 §1). Поля — атрибуты, чтобы
    вызывающий не разбирал текст сообщения.
    """

    def __init__(
        self, *, run_id: str, stored_sha256: str | None, incoming_sha256: str
    ) -> None:
        self.run_id = run_id
        #: Дайджест уже сохранённого снимка (``None`` — записи не оказалось).
        self.stored_sha256 = stored_sha256
        #: Дайджест предъявленного снимка.
        self.incoming_sha256 = incoming_sha256
        super().__init__(
            f"run_inputs для run_id={run_id} уже записан с payload_sha256="
            f"{stored_sha256 or '<нет записи>'}, предъявлен {incoming_sha256}; "
            "снимок входов — идентичность прогона (O11, 09 §3.3): расхождение означает "
            "другой прогон, ему нужен новый run_id, а не перезапись"
        )


@runtime_checkable
class StorePort(Protocol):
    """Операционное хранилище прогона. Транзакционность; WAL; батчевые записи (§B.2)."""

    # -- запись ---------------------------------------------------------- #
    def upsert_products(self, products: Iterable[Any]) -> None: ...

    def create_run(self, run_id: str, cfg: RunConfig) -> None: ...

    def set_phase_state(self, run_id: str, phase: str, state: str) -> None:
        """Фиксирует состояние фазы прогона (durable-носитель resume, 02 §A.1).

        Состояния фаз аккумулируются, а не перетирают друг друга: реализация обязана
        сохранять карту ``{phase: state}`` целиком.
        """
        ...

    def get_phase_state(self, run_id: str) -> dict[str, str]:
        """Возвращает карту ``{phase: "ok"|"blocked"|"failed"}`` прогона для resume.

        Пустой словарь — если прогон неизвестен, состояние не записано или повреждено
        (legacy-формат/невалидный JSON). Метод не бросает исключений на неизвестный ``run_id``.
        """
        ...

    def save_cell(self, cell: Cell) -> None:
        """Upsert ячейки по PK ``(run_id, product_id, param_id)``.

        Контракт реализации (``SqliteStore``):

        * ``Cell.absence_rule`` пишется в ``cells.absence_rule``; ``None`` — «правило не
          записано», и при чтении возвращается ``None``, а не ``explicit`` (выдуманное
          правило делало бы ``I2`` проверяемым на значении, которого никто не объявлял —
          ``review.promote`` работает с прочитанными ячейками);
        * ``Cell.validation_notes`` **накапливаются**: реализация сливает их с уже
          записанными (без дублей), потому что ``INSERT OR REPLACE`` иначе стёр бы
          причину прошлой оценки при resume;
        * ``iter_cells`` возвращает обе колонки обратно (``Cell.absence_rule`` /
          ``Cell.validation_notes``).
        """

    def save_cells_batch(self, cells: list[Cell], chunk_size: int = 200) -> None: ...

    def get_cell(self, run_id: str, product_id: str, param_id: str) -> Cell | None:
        """Одна ячейка прогона (``None`` — строки нет).

        Нужна переносу ячеек (09 §10.2): сама ячейка и её evidence читаются из БД
        прогона-предшественника, и только после того, как ключ ``cell_reuse_key`` совпал.
        Читать весь прогон ради одной ячейки значило бы декодировать сотни строк на
        каждую пару «продукт × параметр».
        """

    def iter_cell_evidence(
        self, run_id: str, product_id: str, param_id: str
    ) -> list[CellEvidence]:
        """Типизированный evidence одной ячейки, в порядке вставки (без ``absence``).

        Протокол отсутствия не отдаётся: у него нет ``source_id``, и типизированная
        модель его отвергает (I10/I15) — он переносится носителем
        ``Cell.absence_evidence`` (см. :meth:`add_absence_evidence`).
        """

    def add_evidence(self, ev: CellEvidence) -> None: ...
    def add_evidence_batch(self, evs: list[CellEvidence]) -> None: ...

    def add_absence_evidence(self, entries: list[Mapping[str, Any]]) -> None:
        """Пишет absence-evidence (§4.2) в ``cell_evidence`` как ``evidence_type='absence'``.

        Записи — словари носителя ``Cell.absence_evidence`` (у протокола отсутствия нет
        ``source_id``, поэтому типизированный ``CellEvidence`` для него не строится).
        ``evidence_id`` детерминирован вызывающим, upsert идемпотентен (S8).
        """

    def iter_absence_evidence(self, run_id: str) -> Iterator[dict[str, Any]]:
        """Читает absence-записи прогона (по словарю на запись).

        Симметричен :meth:`add_absence_evidence`; ``iter_cells`` приклеивает их к ячейке,
        поэтому ``❌`` не теряет протокол отсутствия при чтении из store (отчёт/ревью).
        """

    def iter_cell_quotes(self, run_id: str) -> Iterator[dict[str, Any]]:
        """Читает цитаты доказательств прогона — **любого** вида, включая производные.

        Зачем отдельный читатель: ``iter_cells`` не восстанавливает ``Cell.evidence``
        (цитаты живут в ``cell_evidence``), а замер «о предмете ли доказательство» обязан
        видеть и ``direct``, и ``derived``. Наблюдавшийся дефект — визитка продукта как
        единственная «поддержка» 19 разных параметров — живёт именно в производных
        цитатах наследования; читатель, видящий только ``direct``, показал бы ноль.

        Запись — словарь ``product_id``/``param_id``/``citation_quote``/
        ``evidence_type``/``source_kind``. Порядок — порядок вставки (детерминизм).
        """

    def iter_cell_evidence_refs(self, run_id: str) -> Iterator[dict[str, Any]]:
        """Ссылки evidence **на артефакты корпуса** — одним запросом (план 20 §F2).

        Запись — словарь ``product_id``/``param_id``/``source_url``/``doc_version_id``.
        Один запрос на прогон: дата ячейки — это дата цитируемых артефактов, и вопрос
        задаётся ко всем ячейкам сразу (построчный читатель дал бы запрос на ячейку).
        Пустое значение поля не подменяется: ``None`` — «не записано», а резолв
        (версия → адрес) делает вызывающий, потому что второй формулы адреса быть не
        должно (:func:`core.pipeline.digests.snapshot_url_key`).
        """

    def log_search(self, entry: SearchLogEntry) -> None: ...

    def save_exhaustion(self, pe: ProtocolExhaustion) -> None: ...

    def save_gate_decision(self, d: GateDecision) -> None: ...

    def save_registry_fact(self, rf: RegistryFact) -> None: ...

    def save_llm_scenarios(self, bindings: list[ScenarioBinding]) -> None: ...

    def persist_manifest(self, run_id: str, manifest: RunManifest) -> None:
        """Персистит финальный манифест прогона в операционную БД (R7/§13, A3).

        Обновляет версии/индексные/ретривал-поля строки ``runs``,
        ``settings_provenance_json``/``ranking_epsilon`` (провенанс настроек) и
        деплой-провенанс (``deploy_profile``/``edge_schema_version``/``edge_capabilities``,
        07 §7).
        """
        ...

    def set_run_index_build_id(self, run_id: str, index_build_id: str) -> None:
        """Записывает ``runs.index_build_id`` сразу после фазы корпуса (идемпотентно).

        Composition id известен уже на выходе Ш5, а ``persist_manifest`` пишет весь
        манифест только в Ш9 — слишком поздно для resume и для пост-корпусной привязки
        ретривала. Повторная запись того же значения — no-op; неизвестный ``run_id`` —
        no-op (строку прогона создаёт ``create_run``, а не этот метод).
        """
        ...

    def set_run_corpus_snapshot(
        self, run_id: str, snapshot: Mapping[str, Any]
    ) -> None:
        """Пишет снимок корпуса прогона в ``runs.corpus_snapshot_json`` (план 20 §F1).

        Снимок известен уже на выходе фазы ``corpus``, а манифест (Ш9) собирается позже
        и обязан прочитать его **отсюда**, а не пересчитывать: снимок — провенанс, и
        второй расчёт содержания дал бы вторую величину. Идемпотентно: повторная запись
        того же значения — no-op; неизвестный ``run_id`` — no-op (строку создаёт
        :meth:`create_run`). В ``run_id`` снимок не входит.
        """
        ...

    def read_corpus_snapshot(self, run_id: str) -> dict[str, Any] | None:
        """Читает снимок корпуса прогона; ``None`` — не записан/повреждён.

        Носитель — ``runs.corpus_snapshot_json`` (план 20 §F1). Манифест строит Ш9, но
        снимок обязан пережить resume и пост-хок ``hound report``, поэтому читается
        отдельно от ``read_manifest`` (там он — проекция той же колонки).
        """
        ...

    def save_publishability(self, run_id: str, profile: Mapping[str, Any]) -> None:
        """Персистит блок профиля публикуемости в ``runs.publishability_json`` (A1/§6).

        Нужен **до** Ш9: профиль несёт объявленное исключение стенда
        (``rnd_endpoint`` + ``rnd_endpoint_declared``), а это декларация о месте
        исполнения, которую нельзя вывести из ячеек. Без ранней записи пост-хок
        ``hound report`` пересобирал бы профиль из ячеек и терял признак стенда
        (перезаписывая уже опубликованную карту как производственную). Колонка уже
        существует; ``persist_manifest`` позже перезапишет её тем же значением.
        """
        ...

    def save_plan_inputs(
        self, run_id: str, *, payload: Mapping[str, Any], payload_sha256: str
    ) -> bool:
        """Персистит снимок плановых входов прогона (каталог/веса/когорта) в ``run_inputs``.

        Источник истины самодостаточности ``runs/{run_id}`` (P1): пост-хок команды
        восстанавливают каталог без исходного пакета и без side-файла ``plan_inputs.json``.
        ``payload`` — канонический снимок (:func:`core.cli.context._plan_inputs_payload`);
        ``payload_sha256`` — детерминированный дайджест его канонической формы
        (``json.dumps(sort_keys=True)``).

        Снимок **иммутабелен** (O11, 09 §3.3): три исхода, и только первый пишет строку.

        * записи нет → ``INSERT``, возвращается ``True``;
        * запись есть и ``payload_sha256`` совпал → no-op (``created_at``/``payload_json``
          не трогаются: resume не сдвигает время снимка), возвращается ``False``;
        * запись есть и ``payload_sha256`` другой → :class:`PlanInputsConflict`
          (сохранённая строка остаётся нетронутой; вызывающему нужен новый ``run_id``).
        """
        ...

    def read_plan_inputs(self, run_id: str) -> dict[str, Any] | None:
        """Читает снимок плановых входов прогона; ``None``, если записи нет.

        Возвращает уже разобранный ``payload`` (каноническая форма хранится в
        ``run_inputs.payload_json``); валидацию ``snapshot_version``/``run_id`` выполняет
        вызывающий. Метод не бросает исключений на неизвестный ``run_id``.
        """
        ...

    # -- корпус (Ш5, W7) -------------------------------------------------- #
    def upsert_corpus_doc(self, doc: CorpusDoc) -> None: ...

    def replace_corpus_artifacts(
        self,
        run_id: str,
        product_id: str,
        rows: Iterable[CorpusArtifactRow],
    ) -> None:
        """Заменяет строки ``corpus_artifact`` продукта прогона (план 20 §F2).

        **Замена, а не upsert:** набор артефактов продукта — функция его корпуса, и
        повторный проход (resume/``--reset``) обязан дать тот же набор, а не накопить
        прежний. Пустой ``rows`` — законный исход («в корпусе продукта нет документов»):
        строки продукта удаляются, отказом это не является.

        Сбой записи изолирует вызывающий (снимок и провенанс вторичны к корпусу): здесь
        исключения не глотаются, а пробрасываются.
        """
        ...

    def iter_corpus_artifacts(
        self, run_id: str, product_id: str | None = None
    ) -> Iterator[CorpusArtifactRow]:
        """Итерирует строки ``corpus_artifact`` прогона (порядок ``product_id, url_key``).

        Единственный источник дат артефактов для Ш9/``hound report``: ``state.db`` отчёт
        не открывает. ``None``-даты читаются как «наблюдения нет» и не заменяются ничем;
        пустой итератор — у прогона/продукта артефактов нет (в том числе у прогона,
        записанного до 3.19.0: тогда таблица отсутствует и строк нет).
        """
        ...

    def set_corpus_doc_url(self, doc_id: str, url: str) -> bool:
        """Переписывает канонический адрес документа (17 §6, F3).

        Меняется **только** поле ``url`` строки ``corpus_docs``: идентичность документа
        (``doc_id = product_id:content_hash``), версия, retained-байты и признак
        частичности не трогаются. Возвращает ``True`` только когда строка действительно
        изменилась — повторный вызов с тем же адресом идемпотентен (``False``), и
        вызывающий отличает «адрес уже канонический» от «адрес переставлен».
        """
        ...

    def add_chunk(self, chunk: CorpusChunk) -> None: ...

    def add_chunks_batch(self, chunks: list[CorpusChunk]) -> None: ...

    def save_index_manifest(self, entry: IndexManifestEntry) -> None: ...

    # -- сеанс краула (Lane B: durable намерение + прогресс) --------------- #
    def upsert_crawl_session(self, session: CrawlSession) -> None:
        """Upsert строки ``crawl_sessions`` по PK ``session_id`` (полная запись)."""
        ...

    def get_crawl_session(self, session_id: str) -> CrawlSession | None:
        """Возвращает сеанс краула или ``None``, если он неизвестен."""
        ...

    def iter_crawl_sessions(
        self, *, status: CrawlSessionStatus | None = None
    ) -> Iterator[CrawlSession]:
        """Итерирует сеансы краула (порядок ``session_id``); фильтр по статусу опционален.

        Нужен recovery-on-open: протухшие ``running``-сеансы ищутся по всей БД прогона,
        а не только среди открываемого (:mod:`core.pipeline.liveness`).
        """
        ...

    def touch_crawl_session_heartbeat(
        self, session_id: str, *, at: str, owner: str | None = None
    ) -> None:
        """Отмечает свидетельство живости сеанса (``heartbeat_at``, опц. ``owner``).

        Targeted ``UPDATE``: статус/счётчики/провенанс не меняются. ``owner=None``
        сохраняет прежнего владельца — смена владельца объявляется явно.
        """
        ...

    def update_crawl_session_status(
        self,
        session_id: str,
        status: CrawlSessionStatus,
        *,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """Частичный upsert жизненного цикла сеанса (идемпотентный).

        Переданные (не ``None``) поля перекрывают текущие; прочие сохраняются.
        """
        ...

    def upsert_crawl_session_item(self, item: CrawlSessionItem) -> None:
        """Upsert элемента сеанса по PK ``(session_id, product_id, url)``.

        Атомарно (в одной транзакции) пишет строку элемента **и** пересчитывает/персистит
        агрегированные счётчики сеанса — прогресс не имеет разрывов.
        """
        ...

    def iter_crawl_session_items(
        self,
        session_id: str,
        *,
        product_id: str | None = None,
        status: CrawlItemStatus | None = None,
    ) -> Iterator[CrawlSessionItem]:
        """Итерирует элементы сеанса; порядок ``product_id, url``; фильтры опциональны."""
        ...

    def crawl_session_counts(self, session_id: str) -> dict[str, int]:
        """Число элементов сеанса по каждому статусу ``CrawlItemStatus`` (отсутствующие — 0)."""
        ...

    def recover_crawl_session(self, session_id: str) -> int:
        """Recovery-on-open: ``running`` → ``pending`` и пересчёт счётчиков (одна транзакция).

        Возвращает число восстановленных элементов.
        """
        ...

    def clear_crawl_session(self, session_id: str) -> None:
        """Удаляет элементы сеанса, затем сам сеанс (одна транзакция)."""
        ...

    def set_crawl_session_provenance(self, session_id: str, provenance_json: str) -> None:
        """Записывает ``provenance_json`` сеанса (targeted ``UPDATE``; Phase 2).

        Не задевает счётчики/метаданные сеанса — безопасно для провенанса, вычисленного
        по элементам после финализации.
        """
        ...

    # -- durable передача в конверсию (17 §4.1) ---------------------------- #
    def enqueue_extract_item(self, item: ExtractQueueItem) -> None:
        """Пишет строку ``extract_queue`` **идемпотентно по факту** (ключ — PK).

        Ключ — ``(session_id, product_id, url)``. Повторная запись того же факта
        (resume; повторный фетч, давший те же байты) не создаёт вторую строку и **не**
        перезаписывает состояние, выставленное потребителем: иначе краул, продолживший
        сеанс, сбрасывал бы ``done`` обратно в ``pending`` и заставлял конверсию
        перечитывать уже собранные документы.

        **Другой ``content_hash`` — другой факт.** Пере-фетч того же URL может дать новые
        байты (страница изменилась, retained-файл подменён — краул ре-фетчит, ``09``
        §6.3). Тогда адрес и провенанс получения переписываются, а строка возвращается в
        ``pending``: потребитель F5 строит запрос конверсии **только из строки**, и
        прежний адрес дал бы «корпус без нового документа» при ``state=done``.
        """
        ...

    def set_extract_state(
        self,
        session_id: str,
        product_id: str,
        url: str,
        state: ExtractQueueState,
        *,
        expect: ExtractQueueState | None = None,
        expect_claimed_by: str | None = None,
        claimed_by: str | None = None,
        error_kind: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Переводит строку очереди в ``state``; ``False`` — строка не перешла.

        **Почему не upsert.** Перезапись всей строки затирала бы поля краула
        (``content_hash``/``raw_path``/провенанс получения) и позволяла бы вернуть
        ``done`` в ``pending``: то есть потребитель мог бы переписать факт, которым не
        владеет. Здесь меняются **только** поля потребителя (состояние, метка времени,
        ``claimed_by``, класс и текст отказа).

        **Почему ``expect``.** Условный переход — это и есть claim, а не «взял и надеюсь»:
        строка переходит, только если она всё ещё в ожидаемом состоянии. Иначе два
        потребителя объявили бы одну работу своей, а «переход из `done` в `claimed`»
        перечитал бы собранное. Возврат ``rowcount > 0`` даёт вызывающему честный ответ:
        работа не сделана, если строку уже закрыли или забрал другой.

        ``state_changed_at`` ставит хранилище (единственный источник времени); переход в
        терминальное состояние обязан нести ``error_kind`` для ``failed`` — иначе отказ
        невидим в ``hound crawl status``.

        ``claimed_by`` пишется через ``COALESCE``: вызов без него не снимает прежний claim
        (F6: один и тот же метод даёт claim ``expect=PENDING, claimed_by=<worker>`` и
        последующее закрытие ``expect=CLAIMED`` без повторного имени воркера).

        ``expect_claimed_by`` (F6) — закрывать строку вправе только владелец живой аренды:
        просроченная аренда возвращается в ``pending`` другому воркеру, и «закрытие задним
        числом» объявило бы ``done`` о работе, которую делает кто-то другой.

        Переход **в** ``claimed`` ставит ``claimed_at``/``heartbeat_at`` — это и есть
        аренда; долгий документ обязан продлевать её
        (:meth:`touch_extract_claim`), иначе живой потребитель был бы признан мёртвым.
        """
        ...

    def touch_extract_claim(
        self,
        session_id: str,
        product_id: str,
        url: str,
        *,
        claimed_by: str,
        at: str | None = None,
    ) -> bool:
        """Продлевает живую аренду строки: ``heartbeat_at = now`` (F6, §4.2 п.3).

        Узкая запись heartbeat, а не смена состояния: ``state_changed_at`` не трогается,
        потому что строка никуда не перешла. Продлевается только **своя** аренда
        (``state='claimed' AND claimed_by=?``); ``False`` — аренда потеряна (истекла и
        возвращена другому либо строка закрыта), и воркер не вправе считать документ своим.
        """
        ...

    def reclaim_stale_extract_claims(
        self,
        session_id: str,
        *,
        older_than: str,
        now: str,
        max_attempts: int,
        error_kind: str,
    ) -> list[dict[str, Any]]:
        """Возвращает просроченные аренды в ``pending``, называя каждый факт (§4.2 п.3).

        Просрочка — ``heartbeat_at`` (``claimed_at``, если heartbeat не было) старше
        ``older_than``. Каждая восстановленная строка получает ``attempts += 1``; при
        исчерпании ``max_attempts`` — терминальный ``failed`` с ``error_kind`` (§4.2 п.5),
        и строка больше не воскресает.

        Возвращает список фактов (``product_id``/``url``/``attempts``/``claimed_by``/
        ``exhausted``): молчаливое воскрешение запрещено, поэтому вызывающий обязан
        назвать число восстановленных и кто держал аренду.
        """
        ...

    def count_extract_queue(self, session_id: str) -> dict[str, int]:
        """Число строк очереди сеанса по состояниям; **только непустые** состояния.

        Пустой словарь = очередь пуста. Ключи — значения :class:`ExtractQueueState`.
        """
        ...

    def iter_extract_queue(self, session_id: str) -> Iterable[ExtractQueueItem]:
        """Итерирует строки очереди сеанса в детерминированном порядке ``product_id, url``."""
        ...

    # -- покрытие корпуса (Ф0: гейт ❌) ----------------------------------- #
    def save_corpus_coverage(self, run_id: str, coverage: CorpusCoverage) -> None:
        """Идемпотентный upsert покрытия корпуса по PK ``(run_id, product_id)``.

        Покрытие — производный факт от персистентных элементов краула и записей об
        усечении; повторная запись того же продукта перекрывает прежнюю.
        """
        ...

    def read_corpus_coverage(
        self, run_id: str, product_id: str
    ) -> CorpusCoverage | None:
        """Читает покрытие продукта; ``None``, если записи нет (legacy/без краула).

        ``None`` означает «покрытие неизвестно», а не «полное»: вызывающий обязан
        сохранить прежнее поведение, а не выдумывать ``complete=True``.
        """
        ...

    def iter_corpus_coverage(self, run_id: str) -> Iterator[CorpusCoverage]:
        """Итерирует покрытие прогона; детерминированный порядок ``product_id``."""
        ...

    # -- статус продукта в прогоне (16 §3.2) -------------------------------- #
    def save_run_product(
        self,
        run_id: str,
        product_id: str,
        *,
        mode: str,
        status: str,
        scope: str = "full",
        phase: str | None = None,
        coverage_complete: bool | None = None,
        docs_total: int = 0,
        docs_reused: int = 0,
        docs_new: int = 0,
        cells_total: int = 0,
        cells_pending: int = 0,
        baseline_complete: int = 0,
        error_kind: str | None = None,
        error: str | None = None,
    ) -> None:
        """Идемпотентный upsert статуса продукта **в прогоне** по PK ``(run_id, product_id)``.

        Носитель per-product состояния прогона: ``state.db`` хранит durable-субстрат между
        прогонами, а прогонная БД обязана отвечать на «что случилось с продуктом **в этом**
        прогоне» без обращения к ``state.db``. ``coverage_complete=None`` — покрытие не
        считалось (не «полное»). ``updated_at`` ставит хранилище.
        """
        ...

    def iter_run_products(self, run_id: str) -> Iterator[dict[str, Any]]:
        """Итерирует строки ``run_product`` прогона (порядок ``product_id``).

        Ключи — явный набор колонок таблицы; ``coverage_complete`` — ``bool | None``
        (``None`` = покрытие не считалось), счётчики — ``int``.
        """
        ...

    # -- спуск в контейнеры (B3: прогноз/исход + члены) -------------------- #
    def save_archive_observation(self, observation: ArchiveObservation) -> None:
        """Идемпотентный upsert наблюдения спуска по PK ``observation_id``.

        Запись существует для **каждого** контейнера (включая пустой и нечитаемый):
        повторный спуск того же архива перекрывает прежнюю строку, а не плодит новую.
        """
        ...

    def iter_archive_observations(self, run_id: str) -> Iterator[ArchiveObservation]:
        """Итерирует наблюдения спуска прогона (детерминированно: ``product_id, depth, archive_url``)."""
        ...

    def save_corpus_members(self, members: Iterable[CorpusMember]) -> None:
        """Upsert членов контейнеров по PK ``(archive_content_hash, member_path, product_id)``.

        Идентичность — пара плюс продукт (§6.7): повторный спуск того же архива в том же
        продукте **не** дублирует строки, а один и тот же архив в двух продуктах даёт две
        строки (иначе провенанс одного продукта молча перезаписывался бы чужим).
        Батчевая запись в одной транзакции.
        """
        ...

    def iter_corpus_members(
        self,
        *,
        archive_content_hash: str | None = None,
        product_id: str | None = None,
    ) -> Iterator[CorpusMember]:
        """Итерирует членов контейнеров; фильтры опциональны, порядок детерминирован."""
        ...

    def iter_corpus_docs(self, run_id: str) -> Iterator[CorpusDoc]:
        """Итерирует документы корпуса прогона (детерминированно: ``product_id, doc_id``).

        Read-only источник per-product doc-delta для ``hound diff`` (09 §12.5): наборы
        ``content_hash`` по продуктам обоих циклов. Схема не меняется. В штатной
        раскладке БД принадлежит одному прогону; в общей ``--db`` таблица
        ``corpus_docs`` — объединение корпусов (ограничение раскладки, не подмена).

        :raises ValueError: прогона ``run_id`` нет в этой БД.
        """
        ...

    def save_manual_artifact(self, record: Mapping[str, Any]) -> None:
        """Upsert аудит-строки предоставления (13-manual-artifacts §3.3).

        Ключи записи — :func:`core.pipeline.manual_artifacts.artifact_record`:
        ``run_id``/``product_id``/``url``/``sha256``/``doc_id``/``raw_path``/
        ``declared_path``/``reason``/``provided_by``/``provided_at``/``byte_size``/
        ``created_at``. Идентичность — ``(run_id, product_id, url)``: повторная подача
        перекрывает строку (идемпотентность resume). Факт отказа краула живёт в
        ``crawl_session_items`` и этим методом **не** трогается.
        """
        ...

    def iter_manual_artifacts(self, run_id: str) -> Iterator[dict[str, Any]]:
        """Аудит предоставлений прогона (порядок ``product_id, url``).

        Источник блока ``manual_artifacts`` манифеста (13-manual-artifacts §3.4): доля
        ручного публикуется числом, потолка нет. Пустой итератор — подач не было.
        """
        ...

    # -- чтение ---------------------------------------------------------- #
    def iter_cells(self, run_id: str, state: str | None = None) -> Iterator[Cell]: ...

    def get_chunk(self, chunk_id: str) -> Chunk | None: ...

    def get_chunks(self, chunk_ids: Sequence[str]) -> dict[str, Chunk]:
        """Читает **пачку** чанков по идентификаторам (гидртация ретривала, A1/Д-3).

        Зачем пачкой, а не ``get_chunk`` в цикле: гидртатор лексического индекса
        получает сразу все ``chunk_id`` одного поискового окна, и point-lookup на
        каждый хит — это N+1 обращений к БД на запрос (тот же класс дефекта, что
        закрыт в dense-плече батчем ``search_many``).

        Контракт реализации: ровно один SQL с ``IN (...)`` на пачку, разбитую по
        пределу параметров запроса (SQLite: ``SQLITE_MAX_VARIABLE_NUMBER``; берётся
        заведомо меньший шаг); колонки — тот же набор, что у :meth:`get_chunk`.
        Отсутствующие в хранилище идентификаторы в результат **не** попадают
        (вызывающий отличает «нет записи» от «пустой текст»); пустой вход — пустой
        словарь. Порядок словаря не является частью контракта.
        """
        ...

    def iter_chunks(
        self, build_id: str, *, product_id: str | None = None
    ) -> Iterator[Chunk]:
        """Итерирует чанки индексной сборки ``build_id`` как порт-типы ``Chunk``.

        Нужен для seed/rebuild индекса в возобновлённом процессе (resume): чанки
        восстанавливаются из ``corpus_chunks`` по ``build_id``. Порядок
        детерминирован: ``product_id, chunk_no, chunk_id``. ``product_id=None`` —
        все продукты сборки; иначе — только указанная продукт-партиция. Поля ``url``
        нет (как и в ``get_chunk``): URL резолвится отдельно
        (``resolve_source_url``/``resolve``).
        """
        ...

    def has_corpus_chunks(
        self, build_id: str, *, product_id: str | None = None
    ) -> bool:
        """Есть ли в сборке ``build_id`` хотя бы один чанк (продукт-партиция опциональна).

        ``build_id`` — **сборка продукт-партиции** (``partition_build_id`` из
        ``index_manifest``, ``pb_…``), то есть ровно то, что лежит в
        ``corpus_chunks.build_id``. Композиция когорты (``runs.index_build_id``,
        ``cb_…``) сюда не подходит: в колонке её нет, и проба вернула бы ``False`` на
        полном корпусе (измерено 06.11.2026). Разрешение ``cb_ → pb_`` делает
        вызывающий по манифесту.

        Проба **существования**, а не счётчик: спрашивают её о материале («можно ли
        мерить продукт»), а не о числе чанков, и ответ нужен один бит. ``COUNT(*)``
        прочитал бы весь срез партиции, а :meth:`iter_chunks` — отсортировал бы его
        (``ORDER BY product_id, chunk_no, chunk_id``) ради первого элемента; проба
        ``LIMIT 1`` прекращает чтение на первой же строке. Значение не кэшируется:
        партиция иммутабельна, но вопрос задаётся только на ветке, где материала может
        не быть, — кэш здесь был бы вторым источником правды без выигрыша.
        """
        ...

    def resolve_source_url(self, source_id: str) -> str | None: ...

    def read_manifest(self, run_id: str) -> RunManifest: ...

    def read_run_index_ref(self, run_id: str) -> RunIndexRef | None:
        """Читает ``runs.package_id``/``runs.index_build_id`` прогона (P4b).

        Возвращает :class:`RunIndexRef` либо ``None``, если прогон неизвестен. Значения
        (пакет/сборка) могут быть ``None``, пока их не записал манифест — это честное
        отсутствие, а не подстановка. Метод не бросает исключений на неизвестный ``run_id``.
        """
        ...

    def iter_index_manifest(self, index_build_id: str) -> Iterator[IndexManifestEntry]:
        """Итерирует продукт-партиции индексной сборки ``index_build_id`` (P4b).

        Зеркало :meth:`save_index_manifest`: пост-хок эксперимент разрешает пакет сборки и её
        ``embed_model_id``/``embed_dim`` из БД, не сканируя ``index_store/``. Порядок
        детерминирован — ``product_id``. Пустой итератор — сборки в БД нет.
        """
        ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...

    def iter_exhaustions(self, run_id: str) -> Iterator[ProtocolExhaustion]:
        """Итерирует машиночитаемые протоколы исчерпания прогона (§5/S13).

        Вход для :func:`core.domain.invariants.check_unknown_exhaustion` (I3): каждая
        запись несёт перечень пройденных контуров и ``termination_reason``.
        """
        ...


@runtime_checkable
class GateStore(Protocol):
    """Персистенция и чтение решений гейтов G1–G4 (аудит-след, I6/I7, S9).

    Совпадает по методам с ``StorePort`` (хранилище реализует оба протокола), но
    выделен отдельно: гейт не считается пройденным без строки в ``gate_decisions``,
    привязанной к текущему предмету (``subject_digest``) и с вердиктом ``approved``.
    G1/G2 — внутренняя аттестация, G3/G4 — точки интеграции (решение внешней системы).
    """

    def save_gate_decision(self, d: GateDecision) -> None: ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...
