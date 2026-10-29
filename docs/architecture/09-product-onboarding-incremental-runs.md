# 09. Онбординг продукта и инкрементальные прогоны обновления

> **Статус реализации на 2026-10-29** (HEAD `59068f7`; предыдущая сверка — `f6100c7`):
> PARTIAL — по реестру секций **19 пунктов ✅ из 36**, 16 открыто, 1 снято решением
> (из открытых 3 секции целиком только спроектированы: §10, §14, §19); полностью закрыты
> §1, §5, §7, §8, §20. Ниже у каждого пункта маркер ✅/⬜/➖/❔.
>
> **Что изменилось в этой редакции.** Сверка по коду, а не по памяти: 14 коммитов и +405
> строк в `core/pipeline/corpus.py` сделали неверными несколько утверждений (гейты G1–G3
> перестали блокировать; спуск в контейнеры подключён; провенанс спуска стал
> write-through; `scope_attested_by` переименован в `scope_acceptance`;
> `execution_contract_digest` — 28 полей, а не 19). Одновременно закрыты два дефекта,
> которые этот документ называл design-only: `run_inputs` больше не перезаписывается
> (§14.2), ключ межпрогонного кэша вердиктов несёт методологические условия (§10.5).
> Раздел «чего в документе не было» — §4.7 (память и стадийные бакеты), §8.1 (устройство
> в `extractor_version`), §8.4 (потоковый засев лексики), §11.1 (`resolved_manually`).

> **Как читать этот документ.** Это **гибрид**: часть механизмов работает в прогоне
> сегодня, часть описана как целевой дизайн и не имеет производителя в коде. Каждая
> секция несёт маркер статуса в заголовке, чтобы не ходить за этим в код:
>
> - **as-built** — работает в прогоне; утверждения проверены по коду;
> - **реализовано частично** — работает, но не в том объёме, как описано ниже; в тексте
>   секции явно сказано, чего нет;
> - **design-only** — описано намерение; **производителя в прогоне нет**. Писать против
>   этого SQL/код нельзя, пока не появится реализация.
>
> Отдельно помечено: **нет писателя** (таблица/колонка в DDL есть, но ни одна фаза в неё
> не пишет) и **нет оси** (поле упоминается, но в конфиге/коде отсутствует).
>
> Маркер в заголовке — не косметика: он отделяет то, на что можно опираться в отчёте и
> runbook'е, от roadmap'а. Если маркер расходится с текстом секции — прав текст секции.

## Оглавление

- 1. Контекст и проблема
  - 1.1. Исходное состояние до MVP-1/2 (исторический контекст)
  - 1.2. Почему это не лечится точечно
  - 1.3. Целевой контракт
- 2. UX-контракт
  - 2.1. `hound package add-product`
  - 2.2. Продукт без seed'ов
  - 2.3. Управление seed'ами (as-built)
  - 2.4. `hound run --update`
- 3. Доменная модель и инварианты
  - 3.1. Перечисления (`core/domain/enums.py`, `StrEnum`)
  - 3.2. Доменные модели: где они живут
  - 3.3. Инварианты O-серии
- 4. Durable per-product state
  - 4.1. Где хранить
  - 4.2. Схема `state.db`
  - 4.3. Что остаётся в `runs/{run_id}/cohound.db`
  - 4.4. Транзакционность и восстановление
  - 4.5. Retention
  - 4.6. Bootstrap: принятие уже существующих прогонов
  - 4.7. Durability чанков: запись на границе продукта
- 5. Определение «новый продукт»
  - 5.1. Определение
  - 5.2. Матрица случаев
  - 5.3. Псевдокод
- 6. Алгоритм run
  - 6.1. Порядок фаз и onboarding-префлайт
  - 6.2. `derive_run_id` (замена, не надстройка)
  - 6.3. Corpus: new/full vs incremental
  - 6.4. Evaluate (design-only)
  - 6.5. Статус прогона и продукта
  - 6.6. Оси допуска терминальных артефактов (P6/P6b)
  - 6.7. Контракт спуска в контейнеры (B2, as-built)
  - 6.7.1. Исполнение спуска (B3, as-built)
  - 6.8. Текстовые форматы и EPUB: читатель, маршрут, чанкер (as-built)
  - 6.9. Гейты G1–G4: механизм (as-built)
- 7. Seeds
  - 7.1. Канонический артефакт
  - 7.2. Нормализация и дедуп
  - 7.3. Валидация
  - 7.4. Привязка к `domain_profiles.yaml`
  - 7.5. Приоритет источников seed'ов в `run`
  - 7.6. `--url` как hint
  - 7.7. Нет seed'ов
- 8. Index architecture
  - 8.1. Два идентификатора вместо одного
  - 8.2. Раскладка
  - 8.3. Сборка при добавлении продукта
  - 8.4. Retrieval
  - 8.5. Эмбеддинги
  - 8.6. Иммутабельность и GC
  - 8.7. Операторский потолок ревалидации (`crawl.revalidate_max_documents`)
- 9. Reuse matrix
  - 9.1. Что **нельзя** переиспользовать без пере-измерения
- 10. Evaluation / carry-forward (design-only)
  - 10.1. Ключ переноса ячейки
  - 10.2. Перенос
  - 10.3. Когда обязательна переоценка
  - 10.4. I17/I18/I19/I20/I21
  - 10.5. Eval cache: ключ, который знает правила (as-built)
- 11. Coverage / честность
  - 11.1. Поля `corpus_coverage`
  - 11.2. `❌` и неполный корпус (as-built; `freshness` — design-only)
  - 11.3. Ноль seed'ов / ноль источников
  - 11.4. `reused` vs `observed` в отчёте (design-only)
  - 11.5. `scope_acceptance`
  - 11.6. `pending`/`N/A`
- 12. Versioning §13
  - 12.1. Классы изменений
  - 12.2. `cohort_digest` и `seeds_digest`
  - 12.3. Lineage
  - 12.4. Когда bump'ить
  - 12.5. Межцикловый diff
  - 12.6. Открытые решения человека
- 13. Failure / partial semantics
  - 13.1. Статусы продукта в прогоне
  - 13.2. Новый продукт недоступен
  - 13.3. Старый продукт не изменился (design-only)
  - 13.4. Старый продукт изменился частично
  - 13.5. Повтор и resume
  - 13.6. `--only-product`
- 14. Schema / ports / migrations (design-only)
  - 14.1. Run DB: новые поля/таблицы
  - 14.2. `run_inputs` — без `REPLACE` (as-built)
  - 14.3. Порты
  - 14.4. Артефакты и раскладка
  - 14.5. Layer matrix / категорийная нейтральность
  - 14.6. Что придётся заменить, а не надстроить
- 15. Tests / acceptance
  - 15.1. Функциональные сценарии
  - 15.2. Инвариантные тесты
  - 15.3. Регрессии
- 16. Rollout
  - 16.1. Этапы
  - 16.2. MVP (3–5 шагов)
  - 16.3. Риски и компромиссы
  - 16.4. Альтернативы (отклонены)
- 17. Сводка ключевых решений
- 18. Открытые вопросы / риски для решения человека
- 19. Приложение: псевдокод `run --update` (design-only)
- 20. Приложение: benchmark моделей оценщика (`experiment evaluator-benchmark`, A3)

### Реестр статусов по секциям

| Секция | Статус | Чего нет / что важно |
|---|---|---|
| §1 | ✅ сделано | исторический контекст: все четыре дефекта закрыты MVP-1/2; секция читается в прошедшем времени |
| §2.1, §2.2 | ✅ сделано | `add-product` пишет `sources/seeds.yaml` + `pending_seeds` (`core/pipeline/package_add_product.py:1466`) и **ревизию когорты**: фактическая смена состава двигает `cohort.yaml: frozen_at` и `run.yaml: cohort_ref` (`update_cohort`, `core/pipeline/package_admin.py:1586`; формула — `next_cohort_ref:1566`). Повтор с тем же составом — no-op. Остаётся колонка `runs.cohort_ref`, которая хранит другое поле (§12.2) |
| §2.3 | ✅ сделано | подкоманды `hound package seeds list/add/disable/enable/export` (`core/cli/package/parser.py:482`, слой — `core/pipeline/package_seeds.py`); `sources/seeds.yaml` в `ARTIFACT_FILES`; ложные подсказки в `context.py`/`corpus.py` стали правдой |
| §2.4 | ⬜ не сделано | `--only-failed` остаётся **сеансовым**; `--update`+`--only-failed` запрещены; `offline` не реализован |
| §3.1 | ⬜ не сделано | 7 из 10 перечислений; `CoverageFreshness`/`MeasurementOrigin`/`RunProductStatus` **нет** (`core/domain/enums.py`) |
| §3.2 | ⬜ не сделано | модели живут как `*Row` в `core/ports/state.py`; `CellMeasurement`/`RunProduct`/`CompositionMember` **нет** |
| §3.3 | ⬜ не сделано | O1/O2/O3/O12 as-built; O5/O6/O13 опираются на carry-forward (design) |
| §4.1, §4.2 | ✅ сделано | `state.db` — `STATE_SCHEMA_VERSION = "5"` (`core/store/state_sqlite.py:41`); `cell_measurement`/`attestation`/`cycle` — **нет писателя** |
| §4.3 | ⬜ не сделано | в `runs` только `execution_contract_digest`; `run_product` нет |
| §4.4 | ✅ сделано | файловый lock пакета: `core/util/package_lock.py` (`flock`, держатель в JSON, перехват только мёртвого локального pid), проводка — `run`, `crawl run\|resume\|restart`, `package state adopt` |
| §4.5 | ➖ снято решением | оси `crawl.retention_days` и статуса `evicted` **не будет**: бессрочное хранение — субстрат evidence, размер публикуется замером |
| §4.6 | ✅ сделано | `hound package state adopt\|show` (`core/pipeline/state_adopt.py`), запись только через `ProductStatePort`; `state_not_adopted` в `run --update`. **Ограничение:** adoption наполняет субстрат, но не включает `incremental` — нужен complete-baseline, которого у пакета нет |
| §4.7 | ✅ сделано | чанки durable в run DB на границе **продукта** (`_persist_chunks`, `core/pipeline/corpus.py:9242`), провенанс спуска — write-through (§4.7) |
| §5 | ✅ сделано | `resolve_product_modes`/`_mode_for` (`core/pipeline/onboarding.py:277,304`), вызов — `preflight` из `core/cli/commands/run.py:676,774` |
| §6.1 | ✅ сделано | onboarding — CLI-префлайт, **не** фаза |
| §6.2 | ✅ сделано | `derive_run_id` без состава продуктов (`core/pipeline/plan.py:265`) |
| §6.3 | ✅ сделано | `plan_incremental` вызывается корпусом (`core/pipeline/corpus.py:6620`); reuse партиции — по `pb`+диск, `snapshot.partition_build_id` источником не служит |
| §6.4 | ⬜ не сделано | `carry_forward_cells` не существует |
| §6.5 | ⬜ не сделано | статус продукта — снимок в `state.db` (`product_snapshot`), не `run_product` |
| §6.6, §6.7, §6.7.1, §6.8 | ✅ сделано | `archive_descent`/`artifact_probe` в корпусе (`ArchiveDescentWalker` — `core/pipeline/corpus.py:7676`; зонд — `:3256,5390,7787`), семейство чанкеров — `chunker_family` |
| §6.9 | ✅ сделано | `make_profile_gate` подключён (`core/cli/context.py:1214`), waiver-след (`core/pipeline/runner.py:467`) |
| §7 | ✅ сделано | `seeds.yaml`/нормализация/приоритет источников (`load_effective_seeds`) + CLI-набор (`seeds add/disable/enable/export`); экспорт legacy — `render_corpus_urls`, round-trip проверяется тестом |
| §8 | ✅ сделано | `pb_`/`cb_`, композиции (`core/pipeline/composition.py`), GC (`gc_index_compositions`, `core/cli/package/commands.py:2193`) |
| §9 | ⬜ не сделано | строки «Cell/evidence» описывают design-only перенос |
| §10 | ⬜ не сделано **в части переноса** | нет `carry_forward_cell`, `cell_reuse_key` (колонка есть в DDL без писателя), `measurement_origin`; **§10.5 (ключ eval cache) — ✅ сделано**. Дизайн — **per-cell** (решение 29.10.2026, схема `cell-reuse-2`): ключ после retrieval, `retrieved_doc_set_digest` вместо doc-set продукта |
| §11 | ⬜ не сделано | реальные колонки покрытия (включая `resolved_manually`, §11.1); `scope_acceptance` — канал проложен (§11.5); `freshness` и всё на нём — **design-only**; признак reuse существует только в `state.db.url_state`, в прогон не доезжает. **Правило `❌` → только `live\|revalidated` принято 29.10.2026** (§11.2): класс изменения — `methodology_version`, едет в одной границе с `corpus_docs.reuse_kind` |
| §12.1–§12.2, §12.5 | ✅ сделано | `execution_contract_digest`, `content_change` (`core/pipeline/report.py:2095`) |
| §12.3 | ✅ сделано (частично) | lineage пишется в `state.db.run_lineage` (`core/pipeline/state_sync.py:333`); в манифесте прогона его нет |
| §13 | ⬜ не сделано | §13.3 (carry-forward) — design-only |
| §14 | ⬜ не сделано **частично** | колонок дизайна (§14.1) нет; при этом пп. 1–7, 9–10 §14.6 **реализованы** (см. секцию), остаются пп. 8, 11–12; §14.2 (иммутабельность `run_inputs`) — ✅ сделано |
| §15 | ⬜ не сделано | план приёмки: T1/T15–T20 — design-only, acceptance-тестов нет; T10/T7 переведены в as-built по коду (маркеры в §15.1) |
| §16 | ⬜ не сделано | этапы 0–4 реализованы, этап 5 (carry-forward) не начат (маркеры в §16.1) |
| §17 | ⬜ не сделано | сводка: п. 9 и 13 (межпрогонный) — design-only; п. 11 (ключ eval cache) закрыт |
| §18 | ✅ сделано | все 10 вопросов закрыты: п. 2 (`❌` только при `live\|revalidated`), п. 6 (**per-cell**), п. 10 (`run_scope` персистится, политика `--only-product` не меняется) — решениями 29.10.2026; п. 5, 7, 9 — механизмом |
| §19 | ⬜ не сделано (design-only) | псевдокод с `state.lock()`/`run_product` |
| §20 | ✅ сделано | benchmark оценщика (`core/cli/commands/experiment.py:529`) |

> **Как читать маркеры** (проверка 2026-10-29, HEAD `59068f7`; предыдущая — `f6100c7`): ✅ —
> исполнение найдено в `core/` (вызов из `core/pipeline/`, `core/cli/` или `core/store/`), а не
> только объявление; ⬜ — механизма в прогоне нет; ➖ — снято решением (`11` §3.1); ❔ —
> установить не удалось. Разница редакций — 14 коммитов; самые «протухшие» места были про
> корпус (`corpus.py` +405 строк), гейты и имя `scope_acceptance`, и они исправлены по коду,
> а не по памяти.
> Итог по 36 строкам: **19 ✅ / 16 ⬜ / 1 ➖ / 0 ❔** (перемер 29.10.2026 после полос
> seeds/state-adopt/golden-exempt и файлового lock; прежние 14/22 считали §2.3, §4.6 и
> §4.5 по старому состоянию). Символ, существующий и покрытый тестом,
> но не вызываемый конвейером, здесь считается ⬜ (пример из проекта — `run_serp_round`).

> **Что осталось и почему это не «доделать по мелочи».** Восемнадцать открытых строк
> сводятся к **двум границам**, и обе меняют идентичность измерения, поэтому не могут
> делаться побочно:
>
> 1. **Версионная граница `runs` (§14.1) — она же граница A
>    (`16-runs-schema-boundary.md`).** `run_product` и статус продукта в прогоне
>    (§4.3/§6.5/§13.1), `run_scope` и `product_plan` в манифесте (§13.6), счётчики
>    свежести в прогонной БД (§11.2/§11.4), исправление `runs.cohort_ref` (§12.2),
>    признак reuse у `corpus_docs` (§11.2), `chunk_id` с `doc_version_id` (§14.6 п. 11).
>    Плюс — по решению владельца 29.10.2026 — ось `embed_device` (оговорка 5 `14`):
>    она входит в `EXECUTION_CONTRACT_FIELDS`, то есть это тот же `run_id`-сдвиг, и
>    делать его дважды значило бы дважды обнулить прогресс. **`SCHEMA_VERSION` граница
>    НЕ поднимает** — всё аддитивно (`_ensure_columns` + `CREATE TABLE IF NOT EXISTS`),
>    а `_reject_foreign_schema` существует ради несовместимого чтения, а не счётчика
>    ревизий (решение и обоснование — `16-runs-schema-boundary.md` §3). Значит
>    завершающиеся прогоны остаются читаемыми, и откладывать границу заставляет только
>    одно: правка `corpus.py`/`phases.py`, которые импортированы живым процессом.
>    **Класс границы — `methodology_version`, а не `execution_change`:** в неё входит
>    правило «`❌` только при `live|revalidated`» (§11.2, решение владельца 29.10.2026),
>    а оно меняет условие публикации `❌`, то есть absence/evidence-модель. Цена названа
>    честно: `golden-full` **и** пере-аннотация golden (§13). Разносить это на две
>    границы значило бы платить за golden-full дважды — обе правки ждут одного прогона.
> 2. **Волна 2 — carry-forward (§10, §13.3, §19).** `cell_reuse_key`,
>    `measurement_origin`, `carried_from_run_id`, `cell_measurement` с писателем,
>    enum'ы `CoverageFreshness`/`MeasurementOrigin`/`RunProductStatus` (§3.1), модели
>    `CellMeasurement`/`RunProduct` (§3.2), O5/O6/O13 (§3.3), acceptance T15–T20 (§15),
>    этап 5 (§16.1). Без неё §9 (строка Cell/evidence), §11.2 (`freshness`) и §17
>    (пп. 9, 14) остаются design-only по построению: переносить нечего.
>    **Дизайн зафиксирован per-cell** (решение владельца 29.10.2026): ключ считается после
>    retrieval и несёт `retrieved_doc_set_digest` этой ячейки, а не doc-set продукта;
>    `carry_forward` — часть ячеечного цикла, не батч-проход (§10.1/§10.2).
>
> Вне этих двух границ остаются два пункта: межпрогонный `--only-failed` (§2.4/§13.5) и
> `offline` (§2.4 — требует `freshness`, то есть волны 2, и раньше неё честно не
> делается). Переименование `scope_attested_by` → `scope_acceptance` (§11.5) и файловый
> lock на пакет (§4.4) **сделаны** 29.10.2026.
>
> **Плюс два дефекта краула, найденные живым прогоном 29.10.2026** и не входившие ни в одну
> границу. Оба — `execution_change` (меняют корпус), то есть новый `run_id`. **Решение
> владельца 29.10.2026: они входят в ту же полосу, что граница A** — `golden_required_for`
> возвращает `full` и для `execution_change`, поэтому отдельная полоса стоила бы второго
> `golden-full` и второго нового `run_id` на том же изменении корпуса; класс полосы от
> этого не меняется (`methodology_version` сильнее). Состав и порядок —
> `16-runs-schema-boundary.md` §1/§2/§8:
>
> 1. **Реконструкция бюджета на resume заряжает `max_documents` не тому корню**, из-за чего
>    потолок умножается на число перезапусков (§6.3, «Дефект 29.10.2026»). Замер: 1610
>    `fetched` при потолке 800 — ровно `2 × 800`.
> 2. **`Frontier._seen` дедуплицирует по точной строке URL** (`crawl_frontier.py:1050`),
>    поэтому `/page` и `/page/` — два элемента очереди. Замер
>    (`scripts/measure_crawl_efficiency.py`, снимок 29.10 15:05): у `digital_q_database`
>    **907 из 2095** `fetched` (43.3 %) — те же страницы под другой формой URL; у
>    `pangolin` 54 из 629 (8.6 %); у `jatoba` 0. Доказательство не испорчено (извлечённый
>    текст совпадает, `corpus_docs` = 1121, дублей чанков нет), но **бюджет, HTTP-запросы и
>    извлечение потрачены дважды** — это и есть вторая причина, по которой потолок 800
>    «кончился».
>
> **Замер дерева (исправляет цифру 1919 из прежней редакции).** 1919 было суммой элементов
> по глубинам и считало дубликаты дважды. Снимок 15:05 MSK: уникальных страниц под
> `/versiya-180/` — **1238**, глубины 0–6 (элементы: 14 / 739 / 893 / 546 / 232 / 37 / 2;
> уникальные URL: 14 / 716 / 881 / 420 / 208 / 12 / —). `max_depth: 8` ещё не исчерпан,
> дерево продолжало расти, поэтому «потолок по замеру» **нельзя** объявлять до конца
> прогона: промежуточные цифры (1188 на 15:11, глубина 3) устарели в тот же час, когда
> были получены. Твёрдый вывод один и он не зависит от итоговой цифры: потолок 800 стоит
> ниже уже измеренного, то есть объявляет продукт `truncated` и снимает его `❌`.
> Решение владельца: дождаться конца прогона, **измерить деревья по факту**, затем поднять
> потолки по замеру и починить оба дефекта одной полосой.
> Замер и сырьё — `docs/reports/measurements/2026-10-29-crawl-url-duplication.md`.

> **Статус:** **MVP-1, MVP-2a, MVP-2b и MVP-3a реализованы.** MVP-1 (§16.1, этап 1):
> `sources/seeds.yaml`, onboarding-префлайт, zero-source coverage. MVP-2a (часть этапа 2):
> durable `state/{package_id}/state.db` (§4.1/§4.2), `ProductStatePort`/`SqliteStateStore`,
> `resolve_product_modes` (new/full/incremental), синхронизация seed-набора и проводка в
> `run`. MVP-2b (run identity): `derive_run_id` без состава продуктов
> (§6.2: `cycle_id` + `cohort_digest` + `seeds_digest` + `execution_contract_digest` +
> `run_scope` + `refresh_policy` + focus), `--update`/`--cycle-id`/`--since-run`,
> `change_class`/`golden_required` по предыдущему lineage. MVP-3a (durable-субстрат
> инкрементального корпуса, схема `state.db` **3** на тот момент; сейчас **5** — §4.2): `url_state`/`document_version`/
> `chunk_store`/`chunk_meta` доступны через порт (§4.2), `replace_doc_chunks` атомарен и
> удаляет только осиротевшие тексты, миграция лечит дрейф аддитивных колонок;
> `core/pipeline/incremental.py` — `plan_incremental` (§6.3), `classify_reuse`,
> `doc_version_id`/`chunk_row_id`/`doc_set_digest`. MVP-3b (интеграция в корпус):
> `build_corpus` принимает `state`/`product_modes`/`refresh_policy`; условный GET
> (`If-None-Match`/`If-Modified-Since` из `url_state`) на локальном и executor-путях
> (валидаторы `etag`/`last_modified`/`not_modified` больше не теряются на шве);
> `incremental`-продукт получает known-URL из `plan_incremental`; `corpus` группирует их
> по авторским корням и дренирует как depth-0 **листья** внутри frontier владельца (после
> seed, до BFS), **вне** `max_documents`; при `revalidated` (304 или тот же `content_hash`)
> doc-версия и chunk-set переиспользуются **с пересчётом `content_hash(text)`** (I19) и
> без вызова чанкера;
> пишутся `url_state`/`document_version`/`chunk_store`/`chunk_meta`; снимок получает
> `docs_total`/`docs_new`/`docs_reused` из doc-set. `state is None` — байт-идентично
> прежнему. MVP-4 шаг 1 (композиции, поведение сохранено): `core/pipeline/composition.py`
> (`CompositionView`, `derive_composition_id`, `legacy_view`, immutable
> `_compositions/{id}.json`), аддитивный `index_manifest.partition_build_id` (в этом шаге
> `pb := index_build_id` — shim), ретривал и lexical seeding идут через вид композиции
> (`RetrievedChunk.build_id` = партиция, `index.build_id` = композиция), `_compositions/`
> исключён из `list_partitions`/`list_builds`/GC. Попутно исправлен провенанс R0:
> `build_id` ячейки больше не `engine_version`. **MVP-4 шаг 2 реализован** (решение
> владельца: `execution_change`, включая §13/`catalog_version`): формулы разделены —
> `partition_build_id` (`pb_`, per-product, без `params_token`/`snapshot_date`) и
> `composition_id` (`cb_` = `runs.index_build_id`); партиция переиспользуется с диска
> **без вызова эмбеддера** (второй, физический контроль по `manifest.json`), композиция
> пишется после фиксации всех членов; `corpus_chunks.build_id` = `pb`,
> `index_manifest.index_build_id` = `cid`, `cell_evidence.partition_build_id` —
> аддитивно; GC стал reference-aware (живая партиция композиции не удаляется по счётчику
> `keep`); `_single_persisted_build` предпочитает явный id → единственную композицию →
> единственную legacy-сборку; `catalog_version` добавлен в `EXECUTION_FIELDS` (§13:670):
> после удаления `params_token` из id это единственный diff-видимый сигнал смены каталога.
> Прунинг неиспользуемых композиций реализован: `gc_index_compositions` + `hound package
> index-gc` (живое множество — `runs.index_build_id` по run DB; §8.6). Не реализовано:
> `offline`-reuse (**не реализован**: планировщик деградирует до `revalidate` с видимой
> ошибкой на продукт — молчаливая смена политики запрещена, §2.4), расписание
> `next_check_at`, carry-forward (этап 5) — design-only. Операторский потолок
> ревалидации `crawl.revalidate_max_documents` **реализован** как default-off ось (§8.7).
> Документ описывает целевую архитектуру сценария «`add-product` → следующий прогон
> обновления». Он не меняет регламент `cohound-spec-v2.md`; там, где дизайн затрагивает
> семантику §13/§4.4, это явно помечено как **решение человека** (см. §12.6 и §17).
>
> **Отклонения реализованного среза от целевого дизайна** (осознанные, до этапов 3–5):
> `execution_contract_digest` собирается из **декларированных** осей — methodology/package/
> cohort/seeds/retrieval, оси краула, `chunker_version`, `extractor_stack`, реестры,
> deploy-профиль и seam-схема (methodology 2.4.0, §13 регламента). Наблюдаемые версии
> экстрактора/эмбеддера/лексики/векторного хранилища в контракт **не** входят: они
> остаются провенансом манифеста/`pb` и не блокируют diff (§13, принятое ограничение);
> review-артефакт именуется
> `sources/add_product/{date}-{product_id}.json` и ведёт append-only историю `apply`
> (два продукта в один день не затирают друг друга, повторный apply не теряет аудит);
> `package_version` бампается только при фактическом изменении пакета (noop re-add версию
> не двигает); порт durable-состояния лежит в `core/ports/state.py` (не `product_state.py`);
> `previous_run` возвращает последний прогон пакета без параметра `before`.
>
> **Закрыто методологией 2.3.0** (было отклонением, стало правилом): `hound diff`
> сравнивает `execution_contract_digest`, а не `index_build_id` — смена контента при
> неизменном контракте разрешена как `content_change` с баннером и per-product
> doc-delta (§12.5); `CorpusCoverage.complete` требует `observed_sources > 0`, а не
> `expected_sources > 0` (§3.3/O2). Оба — `methodology_change`: на момент решения это был
> `METHODOLOGY_VERSION` 2.2.0 → 2.3.0, `golden-full` + одна пере-аннотация golden.
> **Текущий пин — `0.1.0`** (сброс нумерации 26.10.2026, §12.5); номер версии здесь —
> датировка решения, а не действующее значение.
>
> **Рамки:** `core/` остаётся категорийно-нейтральным; вся предметная специфика — данные
> пакета (`packages/{case_id}/`). Слои: `pipeline ↛ adapters/store/cli`; `index_store/`
> иммутабелен; только публичные сайты, robots, без обхода CAPTCHA/авторизации.

---

## 1. Контекст и проблема

### 1.1. Исходное состояние до MVP-1/2 (исторический контекст)

> **Статус: ✅ сделано (исторический контекст).** Все четыре дефекта ниже **закрыты** (MVP-1/MVP-2, §16.1).
> Секция сохранена как обоснование дизайна и читается в **прошедшем** времени: это не
> описание текущего поведения. Текущее поведение — §2.1/§2.2/§5/§7.

Сценарий «добавили продукт в пакет» разрывался в четырёх местах.

1. **`add-product` не писал seed'ы.** `apply_add_product`
   (`core/pipeline/package_add_product.py`) писал `cohort.yaml`, `run.yaml`,
   `domain_profiles.yaml`, review-артефакт `sources/add_product/{date}-{product_id}.json` и
   опционально паспорт. Ни `sources/corpus_urls.yaml`, ни `crawl_sources.yaml` не
   обновлялись. *Сейчас:* `sources/seeds.yaml` пишется (§2.1, `ensure_seeds_file` +
   merge без затирания).
2. **`run`/`crawl` брали URL только у оператора.** `--urls`
   (`core/cli/commands/run.py`, `core/cli/commands/crawl.py`) был единственным
   источником `url_map`. *Сейчас:* префлайт `preflight` (`core/pipeline/onboarding.py`)
   читает `seeds.yaml` с fallback на `corpus_urls.yaml`; приоритет — §7.5.
3. **Новый продукт давал пустой ingest без аномалии.** `build_corpus` итерировал
   `plan.products`, но URL брал как `url_map.get(product_id, [])`.
   `_persist_corpus_coverage` писал покрытие только по продуктам, у которых есть элементы
   сеанса, поэтому для продукта без URL строки покрытия не было; `_coverage_for`
   (`core/pipeline/evaluate.py`) возвращал `None`, и Ф0-гейт неполного корпуса не
   срабатывал. Продукт оценивался на пустом корпусе — «тихий пустой продукт».
   *Сейчас:* `_persist_corpus_coverage` пишет строку **каждому** продукту плана, включая
   zero-source (с истинной причиной в `reason`); `_coverage_for` для продукта плана
   возвращает synthetic-строку `coverage_missing`, а не `None` (O3, §11.3).
4. **Добавление продукта обнуляло переиспользование.** `derive_run_id`
   (`core/pipeline/plan.py`) включал `sorted(run.products)`: новый состав когорты →
   новый `run_id` → новый `runs/{run_id}/cohound.db`, новый сеанс `crawl:{run_id}`;
   старые `phase_state`, `crawl_session_items`, `corpus_docs`, `corpus_coverage` не
   переиспользовались. Вдобавок `index_build_id` был отпечатком **всех** продуктов
   батча, поэтому добавление продукта пересобирало все партиции. *Сейчас:* состава
   продуктов в материале `run_id` нет (§6.2), а индекс разбит на
   `partition_build_id` (per-product) + `composition_id` (§8.1) — старые партиции не
   пересобираются.

### 1.2. Почему это не лечится точечно

- Недостаточно «дописать seed'ы в `corpus_urls.yaml`»: нет durable-состояния продукта между
  прогонами, нет определения «новый vs существующий», нет переиспользования документов,
  чанков, эмбеддингов, партиций и ячеек.
- Недостаточно «переиспользовать старый `run_id`»: `run_inputs` — `INSERT OR REPLACE`
  (`SqliteStore.save_plan_inputs`, `core/store/sqlite_store.py`), а снимок `run.yaml` при
  отличии не перезаписывается → расхождение провенанса.
- Недостаточно «сделать `index_build_id` по продукту»: retrieval сейчас конструируется на
  **один** `build_id` (`NpyVectorIndex`, `core/adapters/vector_npy.py`;
  `seed_lexical_index`, `core/cli/wiring.py`), поэтому нужна композиция партиций.

### 1.3. Целевой контракт

1. `hound package add-product "<name>" [--url <hint>] --apply --actor ...` добавляет продукт
   в пакет/когорту, **фиксирует seed'ы** и провенанс онбординга.
2. Следующий `hound run --update` обрабатывает **все новые продукты в полном объёме**
   (полный краул, экстракция, чанкинг, индекс, оценка, покрытие), а старые — инкрементально,
   с переиспользованием артефактов.
3. Продукт без валидных seed'ов помечается `pending_seeds`; `run` блокируется с понятной
   ошибкой, а не оценивает пустой корпус.
4. Новый прогон — **новый** `run_id`; старый прогон не мутируется. Переиспользование —
   через durable per-product state, а не через перезапись старого прогона.

---

## 2. UX-контракт

### 2.1. `hound package add-product`

```
hound package add-product "<name>" [--url <hint>] [--category ...] [--reason ...]
    [--search-adapter ...] [--lang ru|en] [--limit N] [--max-pages N]
    [--apply --actor <human>] [--passport] [--passports-dir DIR]
    [--dry-run] [--json]
```

**Фазы команды.**

| Фаза | Что делает | Пишет? |
|---|---|---|
| `resolve_identity` | SERP/scout: `product_id`, canonical name, `resolved_url`, кандидаты | нет |
| `discover_sites` | discovery сайтов, ключи `domain_profiles.yaml`, зондирование осей | нет |
| `plan_seeds` | **новое:** формирует seed-набор из `resolved_url` + всех обнаруженных сайтов, дедуп/валидация | нет |
| `apply` | cohort → seeds → profiles → passport → review-артефакт | да, атомарно по артефактам |

**`--url` — только hint/identity.** Он:
- участвует в выборе `resolved_url` при неоднозначности (`package_add_product.py`);
- становится **одним** seed'ом с `role: primary`, но не отменяет discovery других сайтов;
- не считается «единственным и приоритетным сайтом»: все валидные сайты из `plan.sites`
  попадают в seed-набор.

**Что пишет `--apply` (список — не порядок; порядок записи — в «Атомарность» ниже).**

1. `sources/seeds.yaml` — новый канонический артефакт (см. §7). Merge + дедуп по
   нормализованному URL; существующие seed'ы не затираются. Если файла ещё нет —
   `ensure_seeds_file` импортирует **все** записи `corpus_urls.yaml` (миграция, §7.5),
   чтобы старые продукты не потеряли seed'ы.
2. `domain_profiles.yaml` — как сегодня (`write_domain_profiles_with_new_keys`,
   `package_add_product.py`). Каждый seed обязан резолвиться в существующий или
   только что добавленный profile-key.
3. `package.yaml` — `package_version` bump (minor) через существующий bump-механизм;
   иначе diff не увидит авторское изменение пакета. Bump выполняется **до** записи
   `cohort.yaml`/`run.yaml`: если bump упал, когорта ещё не изменилась.
4. `cohort.yaml` — через `apply_candidate` (`package_add_product.py:1546`), **плюс**
   новая ревизия когорты: `frozen_at = snapshot` (дата онбординга) и новый `cohort_ref`
   (`cohort-{case}-{snapshot}-r{n}`) в `run.yaml`.

   > **Реализовано 29.10.2026** (`update_cohort`, `core/pipeline/package_admin.py:1586`;
   > формула — `next_cohort_ref:1566`). Ревизия двигается **только** при фактической смене
   > состава (`entries` сравниваются после валидации): повторный `--apply` того же продукта
   > и смена одного `frozen_at` — no-op, иначе каждый `--apply` давал бы новый `run_id`.
   > `run.yaml` патчится построчно (`_patch_run_cohort_ref:284`) — комментарии живы.
   >
   > **Уточнение к прежней редакции: прогон от этого не отказывал и не отказывает.**
   > `classify_change` (`core/pipeline/run_identity.py:324`) при росте состава даёт
   > `cohort_expansion` + `golden_required=full` + новый `run_id`; отказ живёт только в
   > `hound diff` (`core/pipeline/report.py:2059`). Настоящий дефект был в **провенансе**: по
   > `run_id`/манифесту нельзя было отличить «когорту расширили, объявив ревизию» от
   > «в `cohort.yaml` дописали строку».
   >
   > Остаётся отдельный дефект — колонка `runs.cohort_ref` хранит `cfg.cohort` (имя файла
   > когорты), а `cohort_digest` считает по `cfg.cohort_ref` (§12.2): манифест показывает
   > ревизию, которой в `run_id` не было. Правка — версионная граница `runs` (§14.1).
5. `sources/add_product/{date}-{product_id}.json` — review-артефакт; в него добавляется `seed_plan`
   (URL, role, source, profile_key, warnings) и `onboarding` (state, missing).
6. `.authoring/apply_log.jsonl` — записи авторских мутаций (существующий контур).

**Атомарность.** Сначала **preflight**: seed-план, profile-binding и cohort-entry
валидируются целиком (profile-binding — против **планируемых** профилей в памяти, ещё не
записанных), ничего не пишется. Затем порядок записи: `domain_profiles.yaml` → `seeds.yaml`
→ `package.yaml` (bump) → `cohort.yaml`/`run.yaml` → passport → review-артефакт. Профили
пишутся первыми, потому что seed обязан ссылаться на существующий `profile_key`; когорта —
**последней** из влияющих на `run` артефактов: если запись профилей/seed'ов/пакета упала,
продукт ещё не в когорте, и следующий `run` его не увидит (безопасный отказ). Если `seeds`
невалидны (SSRF/схема/дубликат/нет profile-key), `--apply` **не пишет ничего** и возвращает
`EXIT_USAGE` с перечнем нарушений. Если валидных seed'ов **ноль** — это не ошибка ввода,
а честный `pending_seeds`: см. §2.2.

**`add-product` не пишет `state.db`.** Это операционное состояние прогона, а не авторский
артефакт; продукт материализуется в state при первом `run`, который его увидел (§4.6).
**Проекция `run.yaml`.** `patch_run_projection` (`core/pipeline/package_io.py:397`) патчит
блок `products` и `anchor`; `cohort_ref` патчится отдельно —
`_patch_run_cohort_ref` (`core/pipeline/package_admin.py:284`), потому что это не поле
проекции из `products`, а ревизия состава. Патч построчный: значение заменяется на месте
с сохранением отбивки и хвостового комментария, при отсутствии ключа строка вставляется
сразу после `cohort:`. Полная перезапись (`exclude_none=True`) в этом пути не включается —
она стирала бы авторские комментарии `run.yaml`, а они несут объяснение версионных границ.

**Ревизия состава — реализовано (29.10.2026).** Фактическая смена `entries` в
`update_cohort` (`core/pipeline/package_admin.py:1586`) двигает ревизию: `frozen_at` ← дата
онбординга, `run.yaml: cohort_ref` ← `next_cohort_ref` (`:1566`, чистая функция:
`cohort-{case_id}-{frozen_at}-r{n}`, суффикс `-r{k}` → `k+1`, его отсутствие → `r2`).
Повтор с тем же составом — no-op, смена только `frozen_at` ревизию не двигает. Тот же путь
использует ручной `hound package cohort update|set` — ручная смена состава тоже новая
когорта. Новая ревизия попадает в review-артефакт (`apply[].cohort_ref`).

**JSON (`--dry-run --json` / `--json`).**

```json
{
  "ok": true,
  "product_id": "acme-db",
  "product_name": "Acme DB",
  "onboarding_state": "ready",
  "resolved_url": "https://acme.example/products/db/",
  "url_source": "serp",
  "seeds": [
    {"url": "https://acme.example/products/db/", "role": "primary",
     "source": "add_product", "profile_key": "acme.example/products/db"},
    {"url": "https://docs.acme.example/", "role": "docs",
     "source": "discovery", "profile_key": "docs.acme.example"}
  ],
  "profiles": {"acme.example/products/db": {"fetch": "http", "extract": "auto"}},
  "cohort": {"changed": true, "noop": false},
  "needs_human": [],
  "warnings": [],
  "written_files": ["packages/.../cohort.yaml", "packages/.../sources/seeds.yaml", "..."]
}
```

### 2.2. Продукт без seed'ов

`add-product --apply` с нулём валидных seed'ов:

- **всё равно** добавляет продукт в `cohort.yaml` (иначе `run` о нём не узнает);
- пишет в `sources/seeds.yaml` запись продукта со `state: pending_seeds`, `seeds: []`;
- пишет review-артефакт с `onboarding.state = "pending_seeds"`,
  `needs_human: ["no_seeds"]`;
- возвращает `ok: false`, `exit_code = EXIT_BLOCKED` и сообщение:
  `продукт 'acme-db' добавлен, но не готов: нет валидных seed'ов; следующий run будет заблокирован`.

Следующий `hound run --update` блокируется в **onboarding-префлайте** (CLI, между
`plan` и `g1` — не фаза, см. §6.1) и возвращает `blocked` с
`reason = "product_not_ready"`, перечисляя продукт и что именно отсутствует
(`no_seeds`, `profile_unbound`, `seed_invalid`). Отчёта/ячеек для такого прогона нет.

> Почему не «отказать и не добавлять»: требование — «следующий run должен блокироваться».
> Если продукта нет в когорте, `run` о нём не знает и не заблокируется. `pending_seeds`
> делает неготовность **явной и durable**.

### 2.3. Управление seed'ами (**as-built**) — ✅ сделано

> **Статус: реализовано 29.10.2026.** Подкоманды `hound package seeds …` есть; `sources/seeds.yaml`
> входит в `ARTIFACT_FILES` (`core/pipeline/package_io.py:127`), поэтому читается и через
> `hound package artifact get --name seeds`. Сценарный слой — `core/pipeline/package_seeds.py`,
> чистые операции над `SeedsConfig` — `core/config/seeds.py` (`add_seed:449`,
> `set_seed_enabled:525`, `render_corpus_urls:257`).

| Команда | Что делает |
|---|---|
| `seeds list --package P [--product ID] [--json]` | read-only: URL, role, source, enabled, state, `profile_key`, `last_verified_at` |
| `seeds add --package P --product ID --url URL [--role primary\|docs\|satellite\|host_root] [--profile-key K] --actor … [--dry-run]` | валидация SSRF + нормализация, дедуп по `normalized_url`; новый URL — **в конец** (позиция = `order` в `seeds_digest`) |
| `seeds disable --package P --product ID --url URL --actor …` | soft-disable (`state: disabled`); `ProductSeeds.state` пересчитывается (последний выключенный → `pending_seeds`) |
| `seeds enable --package P --product ID --url URL --actor …` | возврат в набор; `invalid` снимается |
| `seeds export --package P [--format corpus-urls] [--out PATH]` | legacy-экспорт только активных seed'ов; `render_corpus_urls` — точная обратная сторона `load_corpus_urls` |

Физического удаления нет намеренно: `remove` заменён на `disable`/`enable` — seed — это
авторское утверждение «этот корень относится к продукту», и его отмена должна быть видимой
(`state: disabled`), а не исчезновением строки. Мутации требуют `--actor` и пишут строку в
`.authoring/apply_log.jsonl`; `--dry-run` не пишет ни файл, ни журнал; запись происходит
только при изменении байтов.

`seeds add` без `--profile-key` привязывает seed к самому длинному подходящему ключу
`host[+path]` (`resolve_profile_key`). Если ключа нет — seed **не** получает синтезированный
профиль: он остаётся с `profile_key: null` (краул пойдёт с дефолтными осями `fetch`/`extract`),
и это **видимое** предупреждение `profile_unbound`. Синтез осей (`wait_for`, `rate_limit`,
`extract`) не делается: они измеряются (A3), а не угадываются; ключ объявляет человек или
`add-product`/`crawl-probe`, которые владеют этим путём.

> **Дефект ложного runbook закрыт.** Подсказки `core/cli/context.py` (печать блокировки
> прогона) и `core/pipeline/corpus.py` (zero-source заметка) предлагали
> `hound package seeds add --package … --product … --url …`, а команды не было. Теперь
> подсказка совпадает с реальной сигнатурой (`--actor` в ней назван), и runbook правдив.

**Вытеснение legacy — видимое.** `seeds.yaml[product]` приоритетнее
`corpus_urls.yaml[product]` (§7.5), поэтому первая запись продукта в новый носитель
отменяет весь его прежний набор: один `seeds add` превратил бы корпус продукта в один URL.
Это не деталь формата, а потеря, и она названа предупреждением
`legacy_seeds_shadowed: <products>` (`_legacy_shadowed_products`,
`core/pipeline/package_seeds.py`). Предупреждение выдаётся **только** для продуктов, которые
появляются в наборе впервые (уже вытеснённый legacy — не новость), и в `--dry-run` тоже:
это следствие плана, а не факта записи. Продукты, которых в `seeds.yaml` нет, legacy
продолжает питать — они в предупреждение не попадают.

### 2.4. `hound run --update`

```
hound run --run-yaml packages/{case}/run.yaml --package packages/{case}
    --update [--cycle-id 2026-W43 | --cycle-id 2026-10-19]
    [--since-run <run_id>] [--refresh auto|revalidate|refresh|offline]
    [--only-failed | --only-product ID]
    [--dry-run] [--json] [--reset]
```

- **`--update` — явный старт нового цикла.** Без него `run` остаётся «воспроизвести/продолжить
  текущий цикл» и переиспользует `run_id` из `run.yaml`/`cycle_id`. Автоопределение
  «это новый цикл» опасно: повтор после падения обязан resume'ить, а не создавать новый цикл.
- **`--cycle-id`** — идентификатор цикла (ISO-дата или ISO-неделя). Без `--update`
  эффективный `cycle_id` = `run.yaml:cycle_id`, иначе `run.yaml:snapshot_date` — стабильный
  идентификатор, поэтому повтор после падения resume'ит тот же `run_id`. С `--update`
  эффективный `cycle_id` = `--cycle-id` > `run.yaml:cycle_id` > `date.today()`
  (`resolve_cycle_id`, `core/pipeline/run_identity.py`); скрытого `today()` **без**
  `--update` нет. Совпадение эффективного `cycle_id` с предыдущим — `EXIT_USAGE`
  («`--update` требует нового cycle_id»), а не молчаливый resume. Эффективный `cycle_id`
  фиксируется в снимке прогона (`runs/{run_id}/run.yaml` + `run_inputs`), но **не**
  переписывает `packages/{case}/run.yaml` (авторский артефакт не мутируется прогоном).
- **`--since-run`** — явный predecessor для lineage/`change_class`. По умолчанию —
  **последняя строка** `run_lineage` пакета (`previous_lineage`,
  `core/store/state_store.py`: `ORDER BY started_at DESC, run_id DESC LIMIT 1`) —
  **без фильтра статуса**: `failed`/`aborted`-прогон тоже становится predecessor'ом
  (фильтра «только завершённые» в коде нет). Исключение текущего `run_id` делает CLI
  (`core/cli/commands/run.py`), а не правило выбора. Для resume текущего прогона
  predecessor — он сам, перенос ячеек не выполняется.
- **`--refresh`**:
  - `auto` (дефолт): `new|full` для новых/неполных продуктов, `revalidate` для существующих;
  - `revalidate`: условный GET по сохранённым `ETag`/`Last-Modified`; 304 или тот же
    `content_hash` → переиспользование документа;
  - `refresh`: безусловный GET, content-hash дедуп сохраняется;
  - `offline` — **целевое поведение, в текущей реализации не реализовано**: без сети,
    только retained raw; `freshness = stale`, `❌` запрещён (см. §11.2). As-built:
    планировщик (`_plan_incremental_items`, `core/pipeline/corpus.py`) при `offline`
    честно деградирует до `revalidate` — то есть **ходит в сеть** — и оставляет одну
    видимую ошибку на продукт (`offline reuse не реализован в corpus: {product_id}`);
    молчаливая смена политики запрещена.
- **`--only-failed`** — **сеансовый фильтр, не межпрогонный** (as-built).
  `_filter_corpus_urls` (`core/cli/context.py`) оставляет только `(product_id, url)` со
  статусом `FAILED` **текущего** сеанса краула `crawl:{run_id}`; если сеанса нет, фильтр
  **не применяется** и в `notes` пишется пояснение. Чтения `run_product`/`url_state`
  последнего прогона здесь нет. `--update` + `--only-failed` прямо запрещены
  (`EXIT_USAGE`, `core/cli/commands/run.py`): новый цикл и ремонт прошлого несовместимы.
  Межпрогонный вариант (по `url_state` предыдущего прогона) — **design-only**, §13.5.
- **`--only-product`** — фильтр объёма; прогон помечается `scope = product_only` и **не**
  обновляет `last_completed_run_id` продукта (частичное измерение не становится baseline).
  `run_id` включает `run_scope=product:<id>`; без этого scoped-прогон совпал бы с полным
  `run_id` и resume пропустил бы фазы.
- **`--reset`** — сбрасывает **прогон** (сессию/фазы), но **не** durable state. Для
  разрушительного сброса — отдельные команды `hound package state forget-product` /
  `state rebuild`: **design-only**, таких подкоманд нет (в CLI есть только `index-gc` и
  `builds-gc`); сейчас это ручное удаление `state/{package}/state.db` с бэкапом.

> **`--update` vs идентичность прогона.** `run_id` меняется при любом изменении входов
> (`cohort_digest`/`seeds_digest`/`focus`/`cycle_id`), даже без `--update`: иначе нельзя
> гарантировать провенанс. `--update` — это семантическое объявление нового **цикла**
> (новый `cycle_id`, `previous_run_id` = предыдущая строка lineage, `change_class`).
> Без `--update` изменённые входы создают новый `run_id` в рамках прежнего `cycle_id`
> (например, добавление продукта в середине цикла); это допустимо, но `report` пометит
> `change_class=cohort_expansion` и потребует `golden-full`.

**Автоопределение режима продукта.** `run` не спрашивает «чего не хватает в когорте»;
он смотрит на durable-состояние продукта (§5). Новый продукт = нет завершённого
corpus-снимка, а не «отсутствует в прошлом `run.yaml`».

**Что видит пользователь в `--dry-run --json`.**

`--dry-run` **обязан** прогнать onboarding-preflight (без записи в state.db): иначе
пользователь после `add-product` увидит «ok», а следующий run заблокируется. Dry-run
читает `seeds.yaml` + `state.db` и показывает `mode`/`blocked`/`will` для каждого продукта.
Ключ `carry_cells` в примере ниже — **design-only** (переноса ячеек нет, §10).

```json
{
  "run_id": "ru-pg-dbms-...",
  "cycle_id": "2026-10-19",
  "previous_run_id": "ru-pg-dbms-...",
  "change_class": "cohort_expansion",
  "golden_required": "full",
  "products": [
    {"product_id": "acme-db", "mode": "new", "seeds": 2,
     "last_completed_run_id": null, "will": "full_crawl"},
    {"product_id": "pangolin", "mode": "incremental", "seeds": 4,
     "last_completed_run_id": "ru-pg-dbms-...",
     "will": {"revalidate": 187, "reuse_documents": 180, "carry_cells": 42}}
  ],
  "blocked": []
}
```

---

## 3. Доменная модель и инварианты

> **Статус раздела: ⬜ не сделано в части перечислений и моделей.** ✅ работают `ProductMode`,
> `RefreshPolicy`, `IncrementalKind`, `ReuseKind`, `ChangeClass`, `RunScope`, `SeedState`
> (7 перечислений), O1/O2/O3/O12; ⬜ `CoverageFreshness`/`MeasurementOrigin`/`RunProductStatus`
> и модели `CellMeasurement`/`RunProduct`/`CompositionMember`; O5/O6/O13 — design-only.

### 3.1. Перечисления (`core/domain/enums.py`, `StrEnum`) — ⬜ реализовано частично

| Enum | Значения | Смысл | Есть? |
|---|---|---|---|
| `ProductMode` | `new`, `full`, `incremental` | режим обработки продукта в прогоне | да |
| `OnboardingState` | `pending_seeds`, `ready`, `blocked`, `retired` | готовность продукта к измерению | да |
| `SeedRole` | `primary`, `docs`, `satellite`, `host_root` | роль seed'а в корпусе продукта | да |
| `SeedState` | `enabled`, `disabled`, `invalid` | операционное состояние seed'а | да |
| `ReuseKind` | `live`, `revalidated`, `reused`, `stale` | как получен документ в этом цикле | да |
| `ChangeClass` | `none`, `content_change`, `cohort_expansion`, `cohort_reduction`, `execution_change`, `methodology_change` | класс изменения между прогонами | да |
| `RefreshPolicy` | `auto`, `revalidate`, `refresh`, `offline` | политика обновления | да |
| `CoverageFreshness` | `live`, `revalidated`, `stale` | свежесть наблюдения корпуса | **нет** — design-only |
| `MeasurementOrigin` | `observed`, `carried` | ячейка измерена или перенесена | **нет** — design-only |
| `RunProductStatus` | `pending`, `running`, `complete`, `partial`, `failed`, `blocked` | статус продукта в прогоне | **нет** — design-only |

Три последних перечисления отсутствуют и в `core/domain/enums.py`, и в
`docs/status-dictionary.yaml`: у них нет ни производителя, ни потребителя, потому что нет
carry-forward (§10) и нет таблицы `run_product` (§4.3/§14.1). Добавлять их в словарь раньше
реализации — значит объявить семантику, которую некому исполнять.

Источник истины — `docs/status-dictionary.yaml`; перечисления добавляются туда и в схему
зеркала. Все — `StrEnum`, без строковых литералов в коде.

### 3.2. Доменные модели: где они живут — реализовано частично

**Авторские модели — `core/domain/models.py`** (`extra="forbid"`, `BaseModel`):

- `SeedEntry(url, normalized_url, role, source, source_ref, profile_key, added_at, added_by,
  enabled, last_verified_at, state)`;
- `SeedsConfig(schema, products: dict[str, ProductSeeds])`;
- `ProductPlan(product_id, mode, onboarding_state, seed_count, last_completed_run_id,
  last_corpus_run_id, last_evaluated_run_id, refresh_policy, scope, reasons: list[str])`;
- `ProductPlanSet(cycle_id, previous_run_id, change_class, products, blocked)`.

**Durable-строки состояния — `core/ports/state.py`**, не `models.py`: это `frozen=True`
dataclass'ы порта, а не Pydantic-модели. Соответствие имён:

| В дизайне (ниже, историческое имя) | В коде |
|---|---|
| `ProductState` | `ProductStateRow` |
| `ProductSnapshot` | `SnapshotRow` |
| `UrlState` | `UrlStateRow` |
| `DocumentVersion` | `DocumentVersionRow` |
| `IndexComposition` | `IndexCompositionRow` |
| `SeedEntry` (durable-строка) | `SeedRow` |
| `RunLineage` | `RunLineageRow` |
| партиция | `IndexPartitionRow` |
| чанк | `ChunkRow` |

Состав полей `*Row` совпадает с DDL `state.db` (§4.2) — это её прямая проекция.
**Отсутствуют вовсе:** `CellMeasurement`, `RunProduct`, `CompositionMember` (член
композиции в коде — элемент `members_json`/`CompositionView.members`, отдельной модели нет;
`CellMeasurement` в `core/pipeline/evaluator_benchmark.py` — другое, метрика бенчмарка).
Имена ниже (в этой таблице) — **design-only** до появления таблиц `cell_measurement`/
`run_product` с писателем.

### 3.3. Новые инварианты (O-серия)

| ID | Инвариант | Где обеспечивается | Статус |
|---|---|---|---|
| **O1** | Каждый продукт когорты имеет ≥ 1 `enabled` seed **или** `onboarding_state=pending_seeds`; `run` с `pending_seeds` блокируется | onboarding-префлайт (`preflight`) + `lint` | as-built |
| **O2** | Продукт с `mode in (new, full)` не может завершить corpus с `expected_sources = 0`; это `failed`, не пустой `ok`. Сильнее: `complete` требует `observed_sources > 0` — корпус без единого наблюдения не доказывает отсутствия (2.3.0) | `build_corpus` (`derive_corpus_coverage`) | as-built |
| **O3** | Для каждого продукта плана в прогоне существует строка `corpus_coverage`; отсутствие — аномалия, не «неизвестно» | `corpus` (`_persist_corpus_coverage`) + `evaluate` (`_coverage_for`) | as-built |
| **O4** | `doc_version_id` детерминирован от `(product_id, content_hash, extractor_version, level)`; durable `chunk_id` — от `(doc_version_id, chunker_version, chunk_no)`; run-level `chunk_id` — от `(product_id, doc_version_id, chunk_no)` | `corpus` | частично: `doc_version_id` — да (`incremental.doc_version_id:270`); durable-строка чанка — да (`chunk_row_id`); run-level `chunk_id` (`corpus._chunk_id:2005`) — **нет**: `f"{product_id}:{doc_hash}:{chunk_no}"` по content-hash. Пространств идентификаторов **три**, и run-level намеренно не несёт `chunker_version`: грамматика `source_id` отдаёт 4-й сегмент под `build_id` (`ids.py:57`), а в одной прогонной БД `chunker_version` — константа контракта (§14.6 п. 11) |
| **O5** | Любая evidence-строка несёт `doc_version_id`, и этот документ входит в doc-set продукта текущего прогона (I19/I17) | `evaluate` / `carry_forward` | частично: `cell_evidence` колонки `doc_version_id` **нет** (только `content_hash`); переноса нет |
| **O6** | `carried`-ячейка допустима только при совпадении `cell_reuse_key`, полном покрытии и неизменном **retrieved-наборе этой ячейки** (per-cell, 29.10.2026; не doc-set продукта) | `carry_forward_cell` (§10.2) | **design-only** — механизма нет |
| **O7** | `❌` публикуется только при `coverage.complete` (или человеческой аттестации); `freshness != stale` — design-часть | `evaluate` (`_enforce_absence_coverage` → `check_absence_coverage`); класс отрицания решает, а не одна полнота (§11.2) | частично: `coverage.complete` + классы — as-built; `freshness` **не существует** (§11.1) |
| **O8** | `index_build_id` прогона = `composition_id`; каждая партиция иммутабельна и адресуется `partition_build_id` | `composition` / `index_npy` | as-built |
| **O9** | Добавление/удаление продукта меняет `cohort_digest` и `change_class ∈ {cohort_expansion, cohort_reduction}` | `plan` / `report` | as-built |
| **O10** | `last_completed_run_id` обновляется только при `baseline_complete=True` (coverage complete, eval прошёл, `cells_pending == 0`) и `scope=full`; `last_corpus_run_id`/`last_evaluated_run_id` обновляются и для scoped-прогона (корпус/оценка продукта действительно выполнены) | `state_sync.record_run_outcome` | as-built |
| **O11** | `run_inputs` прогона иммутабельны: повторная запись с другим `payload_sha256` — ошибка, не `REPLACE` | `save_plan_inputs` (`PlanInputsConflict`) | **as-built** (§14.2): вставка один раз, идентичный повтор — no-op, расхождение — типизированный отказ; остаток — состав payload |
| **O12** | Продукт без `last_corpus_run_id` — `new`; с `last_corpus_run_id`, но без `last_evaluated_run_id` — `full`; с `last_evaluated_run_id` и полным snapshot `coverage_complete=True` — `incremental` (pending-ячейки не мешают) | `onboarding._mode_for` | as-built |
| **O13** | Человеческие решения (`reviewed`, `N/A`, `scope_acceptance`) не переносятся неявно: carry-forward разрешён только при совпадении `attestation_digest`, а `reviewed`/`N/A` без действующей аттестации переоцениваются | `evaluate` | **design-only** — переноса нет; таблица `attestation` без писателя (§4.2) |

Связь с регламентом: O1–O3 закрывают «тихий пустой продукт» (I5/I11-честность); O4–O6
усиливают I17/I19; O7 сохраняет I2c; O8 — §13/иммутабельность; O9–O10 — §13; O11 —
провенанс R7; O12 — A3 (не выдавать частичное за полное).

---

## 4. Durable per-product state

> **Статус раздела: ✅ сделано в §4.1/§4.2/§4.5/§4.6/§4.7, ⬜ открыто §4.3–§4.4.** `state.db`
> v5 с 15 таблицами; писателя **нет** у двух таблиц — `cell_measurement`, `attestation`
> (обе ждут carry-forward, §10) — плюс `cycle`, который пишется только из adoption.
> `state adopt` **есть** (§4.6); `run_product` и файловый lock — нет (§4.3/§4.4);
> retention-ось снята решением (§4.5).

### 4.1. Где хранить — ✅ реализовано

**`state/{package_id}/state.db`** — отдельная SQLite-БД на пакет, вне `packages/` (авторские
данные не смешаются с операционным состоянием) и вне `runs/{run_id}/` (переживает прогоны).

Раскладка (`core/config/paths.py`): поле `state: Path = Path("state")`, env
`COHOUND_STATE_DIR`, приоритет как у остальных полей. Файл:
`state/{package_id}/state.db` (+ `state.db-wal`, `state.db-shm`).
Версия схемы — `STATE_SCHEMA_VERSION = "5"` (`core/store/state_sqlite.py:41`), хранится в
`schema_meta`.

**Почему per-package, а не глобальный `history.db`:**

- blast radius: повреждение/миграция одного пакета не трогает другие;
- нет cross-package блокировок и «общего» `product_id`-пространства (I17: продукт-скоупед);
- бэкап/restore — вместе с пакетом (`backup.py` расширяется);
- категорийная нейтральность: `core/` не знает имён пакетов, путь — данные.

**Глобальными остаются:** `corpus_raw/{product_id}/` (content-addressed байты),
`cache/http/`, `extract_cache/`, `eval_cache/`, `index_store/{package}/`.

**Что действительно лежит в `state.db` (v5).** 15 таблиц, включая `schema_meta`
(14 предметных): `product_state`, `seed_set`, `url_state`, `document_version`,
`chunk_store`, `chunk_meta`, `product_snapshot`, `cell_measurement`, `cycle`,
`run_lineage`, `index_partition`, `index_composition`, `attestation`,
`profile_attestation`. Источник истины — `STATE_DDL`
(`core/store/state_sqlite.py`); копия ниже — для чтения, и она **не** является
контрактом. `profile_attestation` появилась в v4 вместе с полосой гейтов; **v5** добавила
частичность извлечения в `document_version` (`extract_partial`/`extract_partial_reason`/
`extract_pages_converted`/`extract_page_count` — `core/store/state_sqlite.py:113-138`).

> **Дрейф-оговорка.** DDL-копия ниже однажды уже разошлась с кодом (в `run_lineage` не
> хватало двух аддитивных колонок). Сверять нужно с `STATE_DDL`, а не с этим блоком.
> Известные отсутствия в копии до правки: `run_lineage.products_json`/`refresh_policy`,
> `profile_attestation`, `PRAGMA busy_timeout`.

### 4.2. Схема `state.db` — реализовано (копия, не контракт)

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
-- Защита от параллельного писателя: второй процесс ждёт до 5 с, затем ошибка
-- («database is locked»). Это защита носителя, а не пакета: эксклюзивность пакета держит
-- файловый lock `state/{package_id}/.lock` (§4.4).
PRAGMA busy_timeout = 5000;

CREATE TABLE schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 1. Продукт: готовность и последние завершённые измерения.
CREATE TABLE product_state (
    product_id            TEXT PRIMARY KEY,
    canonical_name        TEXT NOT NULL,
    vendor                TEXT,
    onboarding_state      TEXT NOT NULL,        -- pending_seeds|ready|blocked|retired
    onboarding_reason     TEXT,                 -- no_seeds|profile_unbound|...
    added_at              TEXT NOT NULL,
    added_by              TEXT NOT NULL,
    re_added_at           TEXT,
    retired_at            TEXT,
    last_corpus_run_id    TEXT,                 -- corpus завершён (не обязательно eval)
    last_evaluated_run_id TEXT,                 -- eval-фаза прошла (pending-ячейки допустимы)
    last_completed_run_id TEXT,                 -- все ячейки разрешены (evaluated/reviewed/NA)
    last_completed_cycle_id TEXT,
    last_attempt_run_id   TEXT,
    last_attempt_status   TEXT,
    snapshot_digest       TEXT
);

-- 2. Seed-набор (durable; файл seeds.yaml — авторский источник, таблица — операционный).
CREATE TABLE seed_set (
    seed_id        TEXT PRIMARY KEY,            -- short_hash(product_id, normalized_url)
    product_id     TEXT NOT NULL,
    url            TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    role           TEXT NOT NULL,               -- primary|docs|satellite|host_root
    source         TEXT NOT NULL,               -- add_product|operator|discovery|manual
    source_ref     TEXT,
    profile_key    TEXT,
    state          TEXT NOT NULL,               -- enabled|disabled|invalid
    added_at       TEXT NOT NULL,
    added_by       TEXT NOT NULL,
    last_verified_at TEXT,
    retired_at     TEXT,
    UNIQUE (product_id, normalized_url)
);

-- 3. URL → последняя документ-версия + условная валидация.
CREATE TABLE url_state (
    product_id          TEXT NOT NULL,
    url                 TEXT NOT NULL,
    normalized_url      TEXT NOT NULL,
    doc_version_id      TEXT,
    etag                TEXT,
    last_modified       TEXT,
    last_http_status    INTEGER,
    last_checked_at     TEXT,
    last_changed_at     TEXT,
    next_check_at       TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error_kind     TEXT,
    reuse_kind          TEXT,                   -- live|revalidated|reused|stale
    seed_id             TEXT,                   -- из какого seed'а обнаружен URL
    PRIMARY KEY (product_id, normalized_url)
);

-- 4. Версии документов (immutable; метаданные — бессрочно).
CREATE TABLE document_version (
    doc_version_id    TEXT PRIMARY KEY,         -- dv_<hash>
    doc_id            TEXT NOT NULL,            -- product_id:content_hash
    product_id        TEXT NOT NULL,
    url               TEXT NOT NULL,            -- representative URL; aliases — в url_state
    content_hash      TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    level             TEXT NOT NULL,
    raw_path          TEXT,                     -- может быть NULL после retention
    chunker_version   TEXT,
    chunk_ids_digest  TEXT,
    -- v5: частичность извлечения (NULL — не измерено; NULL ≠ «полное»).
    extract_partial           INTEGER,          -- 1 частично, 0 полное, NULL неизвестно
    extract_partial_reason    TEXT,
    extract_pages_converted   INTEGER,          -- прочитано страниц
    extract_page_count        INTEGER,          -- заявлено документом
    first_seen_run_id TEXT NOT NULL,
    last_seen_run_id  TEXT NOT NULL,
    first_seen_at     TEXT NOT NULL,
    last_seen_at      TEXT NOT NULL,
    status            TEXT NOT NULL,            -- active|superseded|evicted
    UNIQUE (product_id, content_hash, extractor_version, level)
);

-- 5. Текст чанков — durable субстрат evidence (raw может быть вытеснен retention).
CREATE TABLE chunk_store (
    content_hash TEXT PRIMARY KEY,              -- hash чистого текста (I19)
    text         TEXT NOT NULL,
    lang         TEXT
);

CREATE TABLE chunk_meta (
    chunk_id         TEXT PRIMARY KEY,          -- product:dv:chunker:no
    doc_version_id   TEXT NOT NULL REFERENCES document_version(doc_version_id),
    product_id       TEXT NOT NULL,
    chunk_no         INTEGER NOT NULL,
    content_hash     TEXT NOT NULL REFERENCES chunk_store(content_hash),
    chunker_version  TEXT NOT NULL,
    heading_path     TEXT,
    context_prefix   TEXT,
    lang             TEXT
);

-- 6. Снимок продукта за прогон (только после eval/coverage).
CREATE TABLE product_snapshot (
    snapshot_id       TEXT PRIMARY KEY,         -- short_hash(run_id, product_id)
    product_id        TEXT NOT NULL,
    cycle_id          TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    status            TEXT NOT NULL,            -- complete|partial|failed
    scope             TEXT NOT NULL,            -- full|product_only
    coverage_complete INTEGER NOT NULL,
    coverage_digest   TEXT,
    doc_set_digest    TEXT,
    partition_build_id TEXT,
    docs_total        INTEGER NOT NULL DEFAULT 0,
    docs_reused       INTEGER NOT NULL DEFAULT 0,
    docs_new          INTEGER NOT NULL DEFAULT 0,
    cells_total       INTEGER NOT NULL DEFAULT 0,
    cells_carried     INTEGER NOT NULL DEFAULT 0,
    cells_measured    INTEGER NOT NULL DEFAULT 0,
    cells_pending     INTEGER NOT NULL DEFAULT 0,
    baseline_complete INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL
);

-- 7. Индекс измерений ячеек (для carry-forward; сами cells/evidence — в run DB).
-- **Нет писателя:** таблица создана, но ни одна фаза в неё не пишет (перенос ячеек не
-- реализован, §10); в реальных state.db — 0 строк. Порт не имеет save_cell_measurement.
CREATE TABLE cell_measurement (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    param_id          TEXT NOT NULL,
    state             TEXT NOT NULL,
    status            TEXT,
    prompt_hash       TEXT,
    model_id          TEXT,
    llm_scenario      TEXT,
    index_build_id    TEXT,
    partition_build_id TEXT,
    doc_set_digest    TEXT,
    coverage_digest   TEXT,
    catalog_row_digest TEXT,
    cell_reuse_key    TEXT,
    evidence_digest   TEXT,
    measured_at       TEXT NOT NULL,
    PRIMARY KEY (run_id, product_id, param_id)
);

-- 8. Lineage прогонов и циклов.
-- **Писателя нет:** ни одна фаза не пишет строку `cycle`; `cycle_id` живёт в `run_lineage`
-- и `product_snapshot` (проверено: `grep -rn "INSERT INTO cycle" core/` — пусто).
CREATE TABLE cycle (
    cycle_id      TEXT PRIMARY KEY,
    package_id    TEXT NOT NULL,
    label         TEXT,
    started_at    TEXT NOT NULL,
    cohort_digest TEXT,
    methodology_version TEXT,
    execution_contract_digest TEXT
);

CREATE TABLE run_lineage (
    run_id            TEXT PRIMARY KEY,
    package_id        TEXT NOT NULL,
    cycle_id          TEXT NOT NULL,
    previous_run_id   TEXT,
    cohort_digest     TEXT NOT NULL,
    seeds_digest      TEXT NOT NULL,
    execution_contract_digest TEXT NOT NULL,
    change_class      TEXT NOT NULL,
    status            TEXT NOT NULL,            -- running|complete|partial|failed|aborted
    started_at        TEXT NOT NULL,
    completed_at      TEXT,
    error             TEXT,
    products_json     TEXT,                     -- JSON-массив product_id (NULL = неизвестно)
    refresh_policy    TEXT                      -- auto|revalidate|refresh|offline
);

-- 9. Реестр иммутабельных индексных партиций и композиций.
CREATE TABLE index_partition (
    package_id        TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    partition_build_id TEXT NOT NULL,
    content_digest    TEXT NOT NULL,
    chunker_version   TEXT NOT NULL,
    embed_model_id    TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    lexical_engine_version TEXT NOT NULL,
    vector_store      TEXT NOT NULL,
    store_path        TEXT NOT NULL,
    chunks            INTEGER NOT NULL,
    dim               INTEGER,
    created_at        TEXT NOT NULL,
    last_used_run_id  TEXT,
    PRIMARY KEY (package_id, product_id, partition_build_id)
);

CREATE TABLE index_composition (
    composition_id  TEXT PRIMARY KEY,           -- cb_<hash>
    package_id      TEXT NOT NULL,
    members_json    TEXT NOT NULL,              -- [{product_id, partition_build_id, content_digest}]
    created_at      TEXT NOT NULL
);

-- 10. Человеческие аттестации (не переносятся молча между циклами).
-- **Нет писателя:** ни одна фаза не пишет сюда; в реальных state.db — 0 строк. Порта
-- get_attestation/save_attestation нет. Действующая аттестация охвата живёт в
-- packages/{case}/scope_attestation.yaml + cell_evidence.scope_acceptance (§11.5).
CREATE TABLE attestation (
    attestation_id  TEXT PRIMARY KEY,
    product_id      TEXT NOT NULL,
    param_id        TEXT NOT NULL,
    source_scope    TEXT NOT NULL,
    attested_by     TEXT NOT NULL,
    attested_at     TEXT NOT NULL,
    doc_set_digest  TEXT NOT NULL,              -- к какому корпусу относилась
    coverage_digest TEXT NOT NULL,
    superseded_at   TEXT
);

-- 11. Аттестация профиля пакета-анкера для G1/G2 (state v4, полоса гейтов).
-- Профиль квази-константен (2–5 правок в год при еженедельных прогонах): человек
-- аттестует содержимое профиля (profile_digest) один раз. НЕ путать с cell-level
-- attestation выше (та — source_scope охвата, I2b).
CREATE TABLE profile_attestation (
    attestation_id  TEXT PRIMARY KEY,
    case_id         TEXT NOT NULL,
    package_id      TEXT NOT NULL,
    anchor          TEXT NOT NULL,
    gate            TEXT NOT NULL,              -- G1 | G2
    profile_digest  TEXT NOT NULL,
    digest_material TEXT NOT NULL,              -- JSON: компоненты дайджеста (для diff)
    attested_by     TEXT NOT NULL,
    attested_at     TEXT NOT NULL,
    superseded_at   TEXT,
    notes           TEXT
);

CREATE INDEX idx_seed_product ON seed_set(product_id, state);
CREATE INDEX idx_doc_product  ON document_version(product_id, status);
CREATE INDEX idx_snapshot_product ON product_snapshot(product_id, created_at);
CREATE INDEX idx_cell_measure_product ON cell_measurement(product_id, run_id);
CREATE INDEX idx_partition_product ON index_partition(package_id, product_id);
```

### 4.3. Что остаётся в `runs/{run_id}/cohound.db` — реализовано частично

> **Статус: реализовано частично.** В `runs` есть только **одно** из перечисленных в
> дизайне полей — `execution_contract_digest` (`core/store/sqlite.py`). Полей `cycle_id`,
> `previous_run_id`, `cohort_digest`, `seeds_digest`, `change_class`, `product_plan_json` в
> `runs` **нет**, таблицы `run_product` **нет** (см. §14.1 — это design). Lineage живёт
> только в `state.db.run_lineage` (§12.3), и `RunManifest` этих полей тоже не несёт.

Фактическое содержимое run DB (`SCHEMA_VERSION = "3.13.0"`), **20** таблиц включая
`schema_meta` (19 предметных; проверяется `connect(':memory:')` + `sqlite_master`):

- `runs` — идентичность прогона и провенанс: `run_id`, `case_id`, `anchor_product_id`,
  `cohort_ref`, версии (methodology/package/catalog/status_dict/scoring), `embed_model_id`,
  `index_build_id`, `retrieval_*` (`retrieval_profile`/`retrieval_params_json`/
  `retrieval_cutoff_K`), оси краула (`crawl_strategy`/`crawl_rate_limit`/
  `crawl_config_digest`/`crawl_provenance_json`), `registry_spec_digest`,
  **`execution_contract_digest`**, `manual_artifacts_digest`/`manual_artifacts_json`,
  `partial_docs_json`, `publishability_json`, `ranking_epsilon` (запрошенный и
  вычисленный), `settings_provenance_json`, `search_effort_budget_json`,
  `deploy_profile`, `edge_schema_version`, `edge_capabilities_json`,
  `snapshot_date`, `state`. Прежняя редакция перечисляла здесь неполный набор и
  пропускала семь колонок — читать `core/store/sqlite.py:40`;
- `corpus_docs` / `corpus_chunks` — срез документов/чанков **этого** прогона. Текст чанков
  здесь действительно дублируется (self-containment), но см. §4.7 — пишется он поздно;
- `index_manifest` — по строке на продукт: `(index_build_id=composition_id, product_id,
  partition_build_id, …)`;
- `cells`, `cell_evidence`, `corpus_coverage`, `search_log`, `search_exhaustion`,
  `registry_facts`, `gate_decisions`, `run_inputs`, `llm_scenarios`, `products`,
  `manual_artifacts`, `crawl_sessions`, `crawl_session_items`, `corpus_members`,
  `archive_observations`.

> Дублирование текста чанков в run DB — осознанная плата за self-containment (бэкап/restore,
> пост-хок команды). Экономия достигается не на записи в БД, а на сети/экстракции/эмбеддингах/LLM.

### 4.4. Транзакционность и восстановление — реализовано частично

- `document_version`, `chunk_meta`, `chunk_store`, `url_state` пишутся **до** фиксации
  партиции (сначала диск, потом БД — как сейчас для индекса).
- `product_snapshot` и `last_corpus_run_id`/`last_evaluated_run_id`/`last_completed_run_id`
  пишутся **после** соответствующего этапа (`record_run_outcome`,
  `core/pipeline/state_sync.py`):
  - `last_corpus_run_id` — при наличии строки покрытия;
  - `last_evaluated_run_id` — если фаза оценки прошла и покрытие есть (даже если часть
    ячеек `pending`);
  - `last_completed_run_id` — только при `scope=full`, непустых ячейках, **все** разрешены
    (`evaluated`/`reviewed`/`N/A`) и `evaluate_ok` (полный baseline).
- Падение между этапами: state.db может содержать «осиротевшие» версии/партиции — они
  content-addressed и безопасны; следующий прогон либо переиспользует их, либо создаст новые.
  `run_lineage.status = aborted` фиксируется при старте следующего прогона (recovery).
- **Concurrency: файловый lock пакета есть (сделано 29.10.2026).** Ресурс — пакет, а не
  БД: сеть, бюджет краула, `corpus_raw/`, `index_store/` и `state.db` принадлежат пакету.
  Носитель — `state/{package_id}/.lock` (`core/util/package_lock.py`): `flock(LOCK_EX)` на
  дескрипторе, поэтому лок снимается ядром при завершении процесса (в том числе `SIGKILL`),
  а файл **не удаляется** — удаление гоняет ABA (A держит inode X, B удаляет и создаёт Y, C
  берёт Y ⇒ B и C «владеют» локом). Содержимое файла — `{host, pid, started_at, command}`:
  оно для оператора, а не для алгоритма. Занятый лок — `CliError` с кодом `state` и
  `EXIT_USAGE`, сообщение называет держателя и путь файла.
  **Протухание: «неизвестно» ≠ «мертво».** Перехват разрешён только для **мёртвого
  локального** pid (`os.kill(pid, 0)`; `PermissionError` считается живым); чужой хост или
  нечитаемый держатель **не** перехватываются — тот же довод, что в `liveness.py`. Перехват
  виден (`PackageLock.reclaimed_from`), иначе «пакет брошен посреди прогона» выглядел бы как
  чистый старт. `pid == os.getpid()` при занятом локе — отказ «повторный вход», а не
  перехват.
  **Где берётся:** `run` — после онбординг-префлайта (заблокированный прогон state не
  создаёт, значит и лока не создаёт), `crawl run|resume|restart` — на всю операцию,
  `package state adopt` — кроме `--dry-run`. Читающие команды (`plan`, `report`,
  `state show`, `crawl status|cancel`) лок не берут.
  **Чего лок не даёт:** он не заменяет `busy_timeout` и не сериализует записи в БД —
  SQLite WAL + `PRAGMA busy_timeout = 5000` остаются. Он отвергает **второго владельца
  пакета**, а не вторую транзакцию.
  **Следствие для тестов (найдено этой полосой):** одиннадцать тестов `run` не задавали
  `COHOUND_STATE_DIR`, поэтому все xdist-воркеры делили `<repo>/state/package/state.db` —
  реальная гонка, которую лок вскрыл отказом. Закрыто фикстурой `isolated_state_dir`
  (`tests/conftest.py`) в трёх модулях.

### 4.5. Retention (**снято решением 29.10.2026**: бессрочно + измерение) — ➖

> **Статус: ➖ снято решением, не «не сделано».** Оси `crawl.retention_days` не будет:
> вытеснение текста чанков делает часть evidence неверифицируемой (I19), то есть покупает
> место на диске ценой доказательной базы. Вместо потолка — **измерение**: размер БД по
> продуктам публикуется числом (`docs/reports/measurements/`), и решение о вытеснении
> принимается по замеру, а не по предвидению (A3). Условие пересмотра названо явно:
> измеренный рост перестаёт помещаться на целевом железе.
>
> Фактическое вытеснение сегодня есть только у **кэшей** (`eval_cache`, `extract_cache`,
> `cache/http`) по их бюджетам — это не evidence, их бюджет остаётся.

| Данные | Политика |
|---|---|
| `corpus_raw/{product}/{sha256}` | бессрочно (сырьё — то, из чего пересчитывается `content_hash`; I19) |
| `document_version`, `chunk_store`, `chunk_meta` | бессрочно (метаданные и текст — субстрат evidence; I19) |
| `product_snapshot`, `cell_measurement`, `run_lineage` | бессрочно (история измерений) |
| `index_partition` / `index_store` | иммутабельно; GC только явным вызовом `gc_index_builds` (проверяет, что сборка не входит ни в одну композицию пакета) и `gc_index_compositions` (§8.6) |
| `eval_cache`/`extract_cache`/`cache/http` | **as-built:** существующие бюджеты/eviction |

Практическое следствие для переноса ячеек (волна 2): raw и текст чанков остаются на месте,
поэтому вопрос «raw вытеснен, а документ нужен для переноса» **не возникает** — условие
переноса сводится к совпадению `content_hash` и retrieved-набора **этой** ячейки
(`retrieved_doc_set_digest`, per-cell — §10.1).

### 4.6. Bootstrap: принятие уже существующих прогонов (**as-built**) — ✅ сделано

> **Статус: реализовано 29.10.2026.** `hound package state adopt|show` (`core/cli/package/parser.py`,
> ядро — `core/pipeline/state_adopt.py`): `plan_adoption` читает прогонную БД **только**
> (`sqlite3 mode=ro`, без гейта `schema_version` — иначе старый прогон не принять), запись
> идёт **исключительно через `ProductStatePort`** (сырого SQL-писателя в модуле нет).
> Предупреждение `state_not_adopted` добавлено в `run --update`.
>
> **Ограничение, которое надо знать до запуска:** adoption переносит корпус, но **не
> включает `incremental`**. `_mode_for` (`core/pipeline/onboarding.py:277`) требует
> `last_snapshot(scope=full).status == "complete"`, а снимок получает `status` из покрытия:
> у всех продуктов `ru-pg-dbms` покрытие неполное, значит снимок `partial` → режим `full`.
> Плюс ячейки не переносятся при расхождении `methodology_version` (2.4.0 в старых
> прогонах против текущей 0.1.0), а без ячеек `baseline_complete=False`. Итог: сегодня
> adoption наполняет durable-субстрат (`document_version`, `chunk_store`/`chunk_meta`,
> `url_state`, `index_partition`, снимок, lineage), но `--update` всё равно идёт полным
> краулом (`new` → `full`; `plan_incremental` вызывается только для `incremental`,
> `core/pipeline/corpus.py:6616`). Это честное следствие «нет ни одного complete-baseline»,
> а не дефект команды; записывать `complete` при неполном покрытии было бы ложью.
>
> **Критерий принятия продукта** — наличие строки `corpus_coverage` (зеркало
> `record_run_outcome`, который ставит `last_corpus_run_id` независимо от `complete`).
> Буквальное чтение прежней редакции (`complete = true`) дало бы тот же исход для этого
> пакета — отказ во всех продуктах, — но различие в другом: при строке-признаке корпус
> переиспользуется субстратом, а не отвергается целиком.

Без этого шага первый `run --update` после появления `state.db` увидит у всех старых
продуктов `last_corpus_run_id = NULL` и сделает их `new` — то есть полный краул всей
когорты. Это прямо противоречит требованию «старые продукты обновляются инкрементально».

**Правило** (as-built, с отличиями от первоначальной спецификации — помечены).

`state.db` — производное хранилище; его можно построить из существующих
`runs/{run_id}/cohound.db` + `index_store/`. Принятие (**adoption**) выполняется один раз:

```
hound package state adopt --package <pkg> [--from-run <run_id>] [--actor <human>] [--dry-run]
hound package state show --package <pkg> [--json]
```

- По умолчанию выбирается **последний пригодный** прогон пакета; непригодные остаются в
  списке кандидатов с причиной. Пригодность — фазы `corpus`/`evaluate` в `runs.state`
  (as-built), а не `runs.status = complete`: колонки статуса прогона нет.
- Принятие **per-product**: продукт без строки `corpus_coverage` остаётся `new` и назван
  в плане с причиной (`no_coverage`/`no_index_manifest`/`no_documents`). Один плохой
  прогон не блокирует adoption остальных.
- Из прогона переносятся:
  - `corpus_docs` → `document_version` (`doc_version_id` — **формулой корпуса**
    `core.pipeline.incremental.doc_version_id`, импорт, а не копия: расхождение формулы
    сделало бы принятые документы невидимыми для инкрементального плана) + `url_state`
    (`etag`/`last_modified` в прогонной БД отсутствуют → `NULL`, первый incremental
    сделает безусловный GET и сравнит `content_hash`; это названо в `limitations`);
  - `corpus_chunks` → `chunk_store`/`chunk_meta` (текст есть только у продуктов, дошедших
    до Pass B — запись идёт на границе продукта, §4.7; иначе чанки будут пересобраны из
    `document_version`);
  - `index_store/{package}/{build}/{product}` → `index_partition` (content-addressed,
    путь и `content_digest` пересчитываются и сверяются с манифестом **и** с диском;
    расхождение → партиция не принимается с видимой причиной);
  - `cells`/`cell_evidence` → `cell_measurement` **только если**
    `runs.methodology_version == current METHODOLOGY_VERSION`; иначе ячейки не
    принимаются (`methodology_mismatch`). Это защищает от переноса вердиктов, посчитанных
    по старым правилам;
  - `product_snapshot` и `run_lineage` (lineage — последним, `previous_run_id` не
    замыкается на себя).
- **Отличие от спецификации:** снимок получает `status` **из покрытия** (`complete`/
  `partial`/`failed`), а не всегда `complete`. Записывать `complete` при неполном покрытии
  значило бы соврать — и именно из-за этого `_mode_for` оставляет режим `full`.
- `attestation` переносится только при совпадении retrieved-набора продукта и
  `coverage_digest`;
  иначе `superseded_at` и `❌` → `pending`. (**Не реализовано в этой полосе** — таблица
  `attestation` в state.db пока без писателя вообще.)
- Adoption идемпотентен: повторный запуск обновляет/дополняет, не дублирует; уже принятый
  прогон виден как `already_adopted`.
- В `run --update` без adoption: если `state.db` пуст, а завершённые прогоны есть, —
  **warning** `state_not_adopted` и явная подсказка команды; молчаливый полный краул
  допустим, но должен быть виден в отчёте (`mode=new`, `reason=no_state`).
- `add-product` **не** пишет `state.db` (это операционное состояние, а не авторский
  артефакт). Продукт материализуется в state при первом `run`, который его увидел.

> Альтернатива «автоматически принимать последний complete-прогон при первом `--update`»
> удобнее, но скрывает выбор baseline. Явная команда + warning — честнее; автоматизацию
> можно добавить флагом `--adopt-latest` позже.

### 4.7. Durability чанков: запись на границе продукта (as-built, дефект закрыт)

Запись в run DB **асимметрична**, и это важно знать до того, как полагаться на «resume
переиспользует добытое»:

- `corpus_docs` пишутся **инкрементально** — сразу после коммита документа:
  `_persist_docs` вызывается в `_finish_one` сразу после `ingest.docs.append` (и в
  `_ingest_member_doc` — для члена контейнера);
- `corpus_chunks` пишутся **на границе каждого продукта** — сразу после того, как его
  Pass B (embed → физическая сборка партиции) отчитался успехом: `_persist_chunks`
  (`core/pipeline/corpus.py`) вызывается в продукт-цикле. Запись по-прежнему обусловлена
  успешной сборкой («disk first, DB second», K2/§13) — просто теперь она не ждёт
  остальных продуктов;
- `index_manifest` пишется **после всех продуктов**: PK строки включает
  `composition_id`, а он выводится из состава когорты (`_persist_manifest`); строки идут
  только для партиций, физически зафиксированных;
- `corpus_coverage` пишется на границе продукта **и** переписывается в конце фазы
  (`INSERT OR REPLACE`): конец фазы авторитетен и закрывает продукты, не вошедшие в
  цикл (zero-source/`coverage_missing`).

**Что переживает падение.** Работа **завершённых** продуктов: их `corpus_docs`,
`corpus_chunks`, физическая партиция на диске и строка покрытия. Теряются незавершённый
продукт и реестр (`index_manifest` + композиция). Реестр восстанавливается при resume
**без пересборки**: `pb` контент-адресуем, `_partition_reuse_reason` находит
зафиксированную партицию и переиспользует её (ноль эмбеддингов), после чего манифест и
композиция пишутся в конце.

**Измеренный дефект (до правки).** Pass B шёл после **всех** продуктов, поэтому падение
внутри фазы теряло чанкирование всего прогона: 1203 `corpus_docs` / **0**
`corpus_chunks`, 518 / **0**, 1517 / **0** (и `corpus_coverage`/`index_manifest` — ноль).
Durable-субстрат чанков в `state.db` (`chunk_store`/`chunk_meta`) существует отдельно и
касается только документов, прошедших через state-путь.

**Провенанс спуска в контейнеры — тоже write-through (F1 закрыт).**
`archive_observations`/`corpus_members` пишутся **сразу** за появлением факта, а не в конце
фазы: `ArchiveDescentWalker` получает синки `persist_observation`/`persist_member`
(`core/pipeline/corpus.py:7716-7717`, реализации `:2057`/`:2087`), поэтому SIGKILL/OOM теряет
максимум контейнер (наблюдение) или член (строку) «в полёте». Идемпотентность —
`observation_id` детерминирован (`run|product|root|hash|depth`), `corpus_members` — UPSERT по
PK. Запись в конце фазы (`:9544-9558`) осталась, но это **reconciliation-pass**: она добирает
то, что не дошло из-за отказа стора на горячем пути (такой отказ виден в `errors`), а не
единственная запись. Прежняя редакция этого документа называла конец фазы единственной
записью и считала это незакрытым остатком — это было неверно.

**Память освобождается на границе продукта (A3/A4).** После того как продукт зафиксирован
(документы, чанки, партиция), фаза **отпускает** его данные: `ingest.docs = []`,
`ingest.chunks = []`, `del product_chunks` (`core/pipeline/corpus.py:9262-9265`). Граница
продукта — это не только граница durability, но и граница **памяти**: держать чанки всех
продуктов до конца фазы значило бы, что RSS растёт с числом продуктов, а не с размером
одного. Счётчики при этом не теряются: они ведутся отдельно (`doc_counters`) и пишутся в
`IndexBuild.doc_count` — прежнее поле `IndexBuild.docs` со списком объектов убрано
(`core/pipeline/corpus.py:1505`), потому что «сколько документов в партиции» — число, а не
список, и хранение списка ради числа удерживало память ровно там, где её отпускают.

**Наблюдаемость фазы — стадийные бакеты времени (`3fbdbfb`).** Время фазы разложено по
стадиям, а не по продуктам-целиком: события `product_end` / `product_index_end` несут
`fetch_wait_s`, `extract_wait_s`, `chunk_s`, `embed_s`, `build_s`, `overhead_s`
(`_CorpusStageTimings`, `core/pipeline/corpus.py:2286`; агрегат —
`_stage_timings_aggregate:2399`), и то же попадает в `metrics.json` как
`resources.stage_timings` (`ResourcesObservation.stage_timings`,
`build_resources_block`). Это **наблюдаемость, а не контракт**: значения не входят ни в
`execution_contract_digest`, ни в `pb`, ни в `content_digest` (доказано пинами в
`tests/pipeline/test_corpus_stage_timings.py`).

> **`overhead_s` не подрезается до нуля.** Отрицательное значение — дефект инструментации,
> и он должен быть **виден**, а не замаскирован `max(0, …)`: иначе «остаток» врёт в
> безопасную сторону. Сейчас `overhead_s` поглощает inline-экстракцию, коммит
> последовательного пути и `_persist_*`; если на реальном корпусе он доминирует, это повод
> ввести отдельный бакет (`commit_s`), а не уменьшить число. `product_end` выводится
> **до** Pass B, поэтому все пять бакетов в одной строке не живут: полная строка с одним
> остатком — в `resources.stage_timings.total`.

**Сеанс краула при прерывании.** Штатная финализация
(`finalize_session`, `core/pipeline/crawl_session.py`) выводит статус из
`crawl_session_counts`: `completed`, если нет failed/cancelled, иначе `partial`. Для
прерванного прогона оба ответа — утверждение о теле краула, которого никто не проверял
(у убитого прогона обычно `pending > 0`), а `partial` уже занят политическим усечением и
частичным отказом. Поэтому обработчик прерывания (`build_corpus` →
`_finalize_interrupted_session`) ставит **терминальный статус из словаря** без записи
провенанса: `cancelled` для `KeyboardInterrupt`/`SystemExit`, `failed` для прочих
исключений, с причиной в `error`. Различить «прервано» и «краул упал сам» эти значения
не могут — отдельное терминальное состояние потребовало бы строки в
`docs/status-dictionary.yaml` (решение владельца методологии, не реализации).
`SIGKILL`/OOM-killer не перехватывается вовсе: такой сеанс остаётся `running` до
recovery-on-open следующего запуска.

---

## 5. Определение «новый продукт»

> **Статус: ✅ реализовано.** Резолв режима — `resolve_product_modes` + `_mode_for`
> (`core/pipeline/onboarding.py`), проверен построчно. Оговорка: слова «переносятся как
> `carried`» в §5.1/§5.2 описывают **design**-часть (§10) — фактического переноса ячеек нет;
> as-built здесь только классификация режима (`new`/`full`/`incremental`).

### 5.1. Определение

В `state.db` различаются три рубежа:

- `last_corpus_run_id` — corpus собран, партиция зарегистрирована; eval мог не состояться;
- `last_evaluated_run_id` — eval-фаза прошла для продукта; часть ячеек может быть
  `pending` (кандидат на человеческое ревью, I6/I7) — это **не** провал измерения;
- `last_completed_run_id` — все ячейки разрешены (`evaluated`/`reviewed`/`N/A`).

Продукт `p` **новый** для прогона `R`, если `last_corpus_run_id IS NULL`.

Продукт **неполный** (`full`, но не `new`), если `last_corpus_run_id` есть, а
`last_evaluated_run_id IS NULL` (eval не состоялся) **или** последний полный snapshot
(`scope=full`) имеет `coverage_complete=False`.

Продукт **существующий** (`incremental`), если есть `last_evaluated_run_id` и последний
полный snapshot имеет `coverage_complete=True` и `scope=full`. `pending`-ячейки не мешают
инкрементальному режиму: они переносятся как `pending`/`carried` и остаются в очереди ревью.

> Это определение не зависит от diff'а когорты. Продукт, добавленный в когорту, но уже
> имеющий завершённое измерение (например, вернули после `retired`), — **не** new по факту
> наличия snapshot'а; но правило re-add (ниже) делает его `new`, потому что за время
> отсутствия состояние устарело.

### 5.2. Матрица случаев

| Случай | `last_corpus_run_id` | `last_evaluated_run_id` | Режим | Комментарий |
|---|---|---|---|---|
| Продукт только добавлен | — | — | `new` | полный краул всех seed'ов |
| Прошлый прогон упал на corpus | — | — | `new` | полный краул; state может переиспользовать уже добытые doc-версии |
| Corpus прошёл, eval не состоялся | есть | — | `full` | **не** инкрементальный: покрытие/ячейки не завершены |
| Eval прошёл, есть `pending`-ячейки | есть | есть | `incremental` | resolved-ячейки carry-forward; `pending` переносятся как `carried` |
| Полный прогон завершён (все resolved) | есть | есть | `incremental` | revalidate + reuse + carry-forward |
| Прошлый прогон `partial` (coverage incomplete) | есть | есть/— | `full` | инкремент от неполного baseline запрещён |
| `--only-product` | обновляется | обновляется | `incremental`/`new` по состоянию | `scope=product_only`; baseline (`last_completed`) не обновляется |
| Продукт `retired`, затем re-add | есть | есть | `new` | `re_added_at` выставлен; полный краул |
| Переименование `canonical_name` | есть | есть | `incremental` | `product_id` иммутабелен; меняется только отображаемое имя |
| Удаление из когорты | — | — | `retired` | партиции/история сохраняются; композиция без продукта |
| Повторное `add-product` существующего | есть | есть | `incremental` | cohort `noop`, seed'ы merge; новых seed'ов → revalidate |
| Изменён только seed-набор | есть | есть | `incremental` | новые seed'ы — полный ingest; удалённые — `disabled` |

### 5.3. Псевдокод

```python
# core/pipeline/onboarding.py
def resolve_product_modes(
    cohort: Cohort,
    seeds: SeedsConfig,                 # sources/seeds.yaml (authoring; fallback corpus_urls)
    state: ProductStatePort,
    profiles: Mapping[str, DomainProfile],
    *,
    cycle_id: str,
    previous_run_id: str | None,
    change_class: str,
    scope: str = "full",
) -> ProductPlanSet:
    plans, blocked = [], []
    for entry in sorted(cohort.entries, key=lambda e: e.product_id):
        pid = entry.product_id
        st = state.get_product_state(pid)          # None — нормально для нового продукта
        seed_rows = [s for s in seeds.enabled_for(pid)]   # authoring-источник, не state
        reasons = []

        # Отсутствие строки state — не причина блокировки: это первый прогон продукта.
        if st is not None and st.onboarding_state == "pending_seeds":
            reasons.append("no_seeds")
        if not seed_rows:
            reasons.append("no_seeds")
        for s in seed_rows:
            if not s.profile_key or s.profile_key not in profiles:
                reasons.append(f"profile_unbound:{s.normalized_url}")

        if reasons:
            plans.append(ProductPlan(
                product_id=pid,
                mode="new",
                onboarding_state="pending_seeds",
                seed_count=0,
                last_completed_run_id=None,
                last_corpus_run_id=None,
                last_evaluated_run_id=None,
                refresh_policy="revalidate",
                scope=scope,
                reasons=reasons,
            ))
            blocked.append(pid)
            continue

        # Режим определяется состоянием **полного** baseline, а не любого scoped-снимка.
        # last_evaluated_run_id — eval-фаза прошла (ячейки могут быть pending на ревью);
        # last_completed_run_id — все ячейки разрешены (evaluated/reviewed/NA).
        if st is None or st.last_corpus_run_id is None:
            mode = "new"
        elif st.re_added_at is not None:
            mode = "new"          # вернули после retired: состояние могло устареть
        elif st.last_evaluated_run_id is None:
            mode = "full"
        elif (snap := state.last_snapshot(pid, scope="full")) is None or snap.status != "complete":
            mode = "full"
        else:
            mode = "incremental"

        plans.append(ProductPlan(
            product_id=pid,
            mode=mode,
            onboarding_state="ready",
            seed_count=len(seed_rows),
            last_completed_run_id=st.last_completed_run_id if st else None,
            last_corpus_run_id=st.last_corpus_run_id if st else None,
            last_evaluated_run_id=st.last_evaluated_run_id if st else None,
            refresh_policy="revalidate" if mode == "incremental" else "refresh",
            scope=scope,
            reasons=[],
        ))
    return ProductPlanSet(
        cycle_id=cycle_id,
        previous_run_id=previous_run_id,
        change_class=change_class,
        products=plans,
        blocked=blocked,
    )
```

Перед `resolve_product_modes` run **синхронизирует** `seeds.yaml` → `state.seed_set`
(`reconcile_seed_set`): upsert по `(product_id, normalized_url)`, отсутствующие в файле —
`state=disabled` + `retired_at` (не удаление). Authoring-файл — источник истины; таблица —
операционный кэш/провенанс. Это делает удаление seed'а из git явным и обратимым.

Фаза `onboarding` возвращает `blocked` (не `failed`), если `blocked` непуст:
это человеческая предпосылка (нужно добавить seed'ы), а не сбой кода. Сообщение:

```
product_not_ready: acme-db (no_seeds); next run blocked.
Добавьте seed в sources/seeds.yaml пакета (подкоманда `hound package seeds add`, §2.3).
```

---

## 6. Алгоритм run

> **Статус раздела: ⬜ не сделано в части §6.4/§6.5.** ✅ §6.1, §6.2, §6.3, §6.6, §6.7,
> §6.7.1, §6.8, §6.9 (as-built); ⬜ §6.4 (перенос ячеек) и §6.5 (`run_product`, статус —
> снимком в `state.db`).

### 6.1. Порядок фаз и onboarding-префлайт (✅ as-built)

`PHASES` (`core/pipeline/runner.py`) — 15 фаз:

```
normalize → lint → plan → g1 → discovery → g2 → registry
→ inheritance → corpus → evaluate → review → g3 → compute → g4 → report
```

**`onboarding` — не фаза.** В `PHASES` её нет. Онбординг — **CLI-префлайт** в composition
root (`core/cli/commands/run.py`), выполняемый между построением плана и запуском
`Runner`'а, то есть логически между `plan` и `g1`:

- `plan` строит `RunPlan` (чистый, без I/O) и `cohort_digest`/`seeds_digest`;
- префлайт (`preflight`, `core/pipeline/onboarding.py`) читает `state.db` + `seeds.yaml`,
  вычисляет `ProductPlanSet` и блокирует неготовые продукты;
- `g1` утверждает уже готовую когорту (человек видит, какие продукты new/full/incremental).

**Куда префлайт пишет.** Ни `run_product`, ни `runs.product_plan_json` не существуют — ни
таблицы, ни колонки нет ни в `core/store/sqlite.py`, ни где-либо ещё. Продукт-план живёт в
памяти (`ProductPlanSet`), передаётся фазам и отражается в `--json`/`--dry-run` payload;
durable-след прогона — снимки и рубежи в `state.db` (`record_run_outcome`, §4.4).

**Гейт по `--until`.** Префлайт попадает в диапазон, если `--until` не раньше `g1`
(`_onboarding_in_scope`): `--until plan` прогон **до** онбординга останавливает, и
seed-префлайт не запускается — иначе он блокировал бы прогон, который не собирается
краулить.

`Runner.run` не меняется: `blocked` останавливает прогон.

### 6.2. `derive_run_id` (замена, не надстройка)

```python
# core/pipeline/plan.py
def derive_run_id(inputs: RunInputs, *, cycle_id: str, cohort_digest: str,
                  seeds_digest: str, execution_contract_digest: str,
                  run_scope: str = "full",
                  refresh_policy: str = "auto") -> str:
    material = "\x1f".join((
        inputs.run.case_id,
        inputs.run.package,
        inputs.run.anchor,
        cycle_id,
        cohort_digest,
        seeds_digest,
        execution_contract_digest,
        run_scope,
        refresh_policy,
        ",".join(sorted(inputs.run.focus.modules)),
        ",".join(sorted(inputs.run.focus.params)),
    ))
    return f"{inputs.run.case_id}-{short_hash(material, 12)}"
```

- `sorted(products)` **удаляется** из материала: состав когорты уже в `cohort_digest`.
- `snapshot_date` **удаляется** из материала: цикл задаётся `cycle_id`; `snapshot_date`
  остаётся провенансом и входит в `index`/manifest.
- `run_scope` (`full` | `only_failed` | `product:<id>`) входит в id: иначе scoped-прогон
  совпал бы с полным `run_id`, и `Runner` пропустил бы фазы как resume.
- `refresh_policy` (`auto|revalidate|refresh|offline`) входит в id: смена политики
  наблюдения — это другое измерение, а не resume.
- `cohort_digest` = `H(canonical JSON of sorted (product_id, anchor_relation, role_overlap);
  case_id; frozen_at; cohort_ref)`. `selection_reason` **не** входит: авторский текст не
  меняет измерение.
- `seeds_digest` = `H(product_id, order, normalized_url, role, profile_key, enabled)`, где
  `order` — **авторская позиция корня внутри продукта** (`core/pipeline/digests.py`):
  порядок корней — часть идентичности, потому что краул порядко-чувствителен (корень,
  идущий последним, может не выполниться вовсе). Порядок **продуктов** в файле
  канонизируется сортировкой. Пустой набор — стабильный дайджест пустого множества.
- `execution_contract_digest` входит в id: смена методологии/каталога/промптов/модели
  делает прежние фазы невалидными для resume. Иначе `--update` с новой
  `methodology_version` переиспользовал бы `ok`-фазы и старые вердикты.
- `--run-id` по-прежнему переопределяет.

Следствия:
- добавление продукта → новый `cohort_digest` → новый `run_id` → новый прогон (правильно:
  это новое измерение, а не мутация);
- повторный запуск того же цикла с теми же входами → тот же `run_id` → resume;
- изменение seed'ов → новый `run_id` (нельзя смешивать корпуса в одном прогоне);
- `--refresh`/`--only-failed`/`--only-product` без `--update` дают новый `run_id`
  (scoped/refresh), а не молчаливый resume старого.

### 6.3. Corpus: new/full vs incremental (реализовано частично)

```python
# core/pipeline/corpus.py (эскиз)
def build_corpus(plan, ..., product_plan: ProductPlanSet, state: ProductStatePort,
                 effective_seeds: SeedsConfig):
    for product in plan.products:
        pp = product_plan[product]
        roots = resolve_seed_roots(product, effective_seeds.for_product(product),
                                   domain_profiles, crawl_sources)
        if not roots:
            # O2: не пустой ok
            mark_product_failed(product, "no_seeds")
            save_coverage(product, expected=0, complete=False, reason="no_seeds")
            continue

        if pp.mode in ("new", "full"):
            work = expand_full(roots)                  # BFS + sitemap, как сейчас
        else:
            work = plan_incremental(product, roots, state, pp.refresh_policy)

        ingest_product(product, work, state, mode=pp.mode)

        # Переиспользование партиции — по pb + физической проверке диска (§8.5),
        # НЕ по snapshot.partition_build_id (колонка всегда NULL, см. ниже).
        if all_docs_reused(product) and partition_exists(product, partition_build_id):
            partition_id = partition_build_id
        else:
            partition_id = build_partition(product)    # embed + write
        state.register_index_partition(partition_row)
        state.set_last_corpus_run_id(product, run_id)

    composition = compose(plan.products, state)         # cb_<hash>
    save_composition(composition)
    return PhaseResult(status="ok", ...)
```

> **Это эскиз, а не код.** Часть вызовов здесь — целевые имена (`state.set_last_corpus_run_id`,
> `save_coverage`, `build_partition`), которых в `ProductStatePort` нет: фактический рубеж
> двигает `record_run_outcome` (`core/pipeline/state_sync.py`), покрытие пишет
> `_persist_corpus_coverage`. Смысл эскиза — порядок шагов, не сигнатуры.

**Переиспользование партиции: `snapshot.partition_build_id` здесь не работает.** Колонка
`product_snapshot.partition_build_id` в DDL есть, но **никогда не заполняется**:
`record_run_outcome` её не передаёт и в докстринге прямо пишет, что «`partition_build_id`
не выдумывается». Измерено: во всех снимках реальных `state.db` — `NULL`. Поэтому путь
«взять id последнего снимка» не мог бы работать as-built. Фактический механизм
переиспользования — **`pb`-уровень + физическая проверка**: `_derive_partition_build_id`
даёт `pb_`, `_partition_reuse_reason` проверяет каталог партиции и `manifest.json` на диске,
и только при совпадении эмбеддинги не считаются; непригодный существующий каталог даёт
**видимую** причину пересборки в `errors` (§8.5).

**Порядок корней — авторский, и он значим.** `url_map[pid] = _ordered_unique(values)` —
дедуп с сохранением порядка первого вхождения, **не** `sorted()`
(`core/pipeline/corpus.py`). Источник порядка — `sources/corpus_urls.yaml`/`sources/seeds.yaml`
(генерируются из паспортных `doc_domains`). Обоснование в коде: живой прогон 22.10 показал,
что лексикографический порядок ставил `https://host/` перед `https://host/docs/`, и
800-страничный корпоративный портал съел бюджет (~2 ч), а docs-корень — высшая ценность для
карты возможностей — не дренировался вовсе. Следствие для оператора: **корень, который идёт
последним, может не выполниться вовсе**. Порядок корней входит в `seeds_digest` (§6.2),
то есть его перестановка — `execution_change`, а не косметика. Продукты по-прежнему
итерируются `sorted(url_map)`.

**Scope краула = только объявленные хосты (каноническое описание).**

URL ставится в очередь корня, если он (1) проходит SSRF-валидацию, (2) принадлежит
**объявленному** хосту, (3) укладывается в path-prefix корня по границе сегмента, (4) не в
блэклисте оператора. SSRF-проверка — отдельная и **не** является проверкой scope.

- Хост корня — всегда в scope. Поддомен — **только если он сам объявлен**: либо как
  seed-корень продукта, либо как ключ `domain_profiles.yaml`. `www.` эквивалентен базовому
  хосту с обеих сторон. Хост, объявленный **собственным** корнем продукта, из
  «поддомен-допуска» корня-предка исключается: у него свой корень и свой бюджет.
- Предикат — `Frontier.host_in_scope` (`core/pipeline/crawl_frontier.py`); набор
  объявленных хостов собирается в `scope_hosts_by_product` (`core/pipeline/corpus.py`).
- **Это не то же самое, что `fetch_plan._host_matches`.** Там семантика **профиля**:
  поддомен наследует оси `fetch`/`extract`/лимиты родительского ключа — это верно и нужно.
  Наследование осей ≠ право тратить бюджет чужого seed-корня, поэтому scope обхода —
  отдельный предикат. Подменять один другим нельзя.
- Отказ **видим**: `out_of_scope_subdomains` (счётчик по хостам + ограниченные примеры) и
  `accepted_foreign_hosts` (сколько бюджета корня ушло на объявленный чужой хост).
- Терминальный артефакт — исключение **только** из path-prefix (P6b, §6.6): он не
  продолжает навигацию, но host-scope/SSRF/блэклист остаются обязательными.
- Класс изменения — `execution_change`: расширение scope меняет корпус, значит нужен новый
  прогон, а не правка текущего. **Измеренный пример:** корень, объявивший только
  родительский хост, затянул `edu.postgrespro.ru` — 563 элемента из 682 (учебные раздатки),
  при этом корень документации продукта не получил ни одного своего элемента.

**Дедуп по `content_hash` легитимен, но его счётчик не durable.** `duplicates`/
`duplicate_count` считаются внутри продукта и выводятся **только** как note фазы
(`_duplicate_notes` → `results[].notes`); в БД они не пишутся. Не искать `duplicate_count`
в run DB — его там нет; вопрос «сколько версий-алиасов схлопнулось» решается по логу
прогона, а не запросом.

**`plan_incremental`.**

```python
def plan_incremental(product, roots, state, policy):
    work = []
    # 1. Seeds всегда в работе (условный GET).
    for seed in roots:
        work.append(CrawlWorkItem(product, seed.url, depth=0, root=seed))
    # 2. Известные URL: revalidate (next_check_at в этом срезе не читается;
    #    расписание — следующий срез).
    for u in state.iter_url_state(product):
        if policy == "offline":
            work.append(Reuse(u)); continue
        work.append(Revalidate(u, etag=u.etag, last_modified=u.last_modified))
    # 3. Discovery: sitemap/feed diff (не полный BFS) — новые URL.
    if policy in ("auto", "revalidate", "refresh"):
        for loc in fetch_sitemaps(roots):           # sitemap — не страница, не ингестится
            if loc not in state.known_urls(product):
                work.append(CrawlWorkItem(product, loc, depth=0, root=root_for(loc)))
    return work
```

**Классификация исхода одного URL.**

```python
def classify(prior: UrlState | None, page: RawPage, extracted: ExtractedDoc) -> ReuseKind:
    if page.not_modified:                       # HTTP 304
        return "revalidated"
    if prior and extracted.content_hash == prior.doc_version.content_hash:
        return "revalidated"                    # 200, но контент тот же
    if prior is None:
        return "live"
    return "live"                               # контент изменился
```

- `revalidated` → переиспользуем `doc_version` (не экстрагируем повторно, если
  `extract_cache` hit; не чанкуем; не эмбеддим; не зовём LLM).
- `live` + новый `content_hash` → новая `document_version`, новый chunk-set, новая
  партиция продукта (эмбеддинги — только для новых чанков, если включён `embed_cache`;
  иначе для всех чанков продукта).
- `404/410` → `document_version.status = superseded`, `url_state.doc_version_id = NULL`,
  в coverage — `not_found` (разрешённое наблюдение отсутствия, как сейчас).
- `robots/timeout/parser` → `FAILED`, coverage `blocked`; `❌` demoted.

**Владение known-URL: scope-инференс, листья, бюджет.**

- **Группировка по scope-инференсу, без хранимого владельца.** `plan_incremental` отдаёт
  известные URL без владельца; `corpus` каждый цикл раскладывает их по авторским корням
  через `Frontier.in_scope` (`_group_revalidation_items`). Побеждает **самый специфичный**
  `root_path_prefix` (длина пути), при равенстве — авторский порядок. Постоянной
  `owner`-колонки нет намеренно: она бы протухала при смене seed-набора и стала бы вторым
  источником истины. Инференс воспроизводит исторического владельца для всех строк,
  записанных pipeline, потому что `Frontier.discover` никогда не ставил в очередь URL вне
  scope корня; каждый известный URL попадает ровно в один корень (или в orphan-бакет).
- **Дренаж внутри frontier владельца, после seed и до BFS.** Known-URL ставятся
  `Frontier.add_revalidate_seed` на глубину 0 **после** авторского seed и **до** BFS.
  Порядок несёт две гарантии: enqueue до BFS позволяет `_seen` схлопнуть известный URL,
  переоткрытый ссылкой (иначе безусловный второй GET теряет 304-экономию); а листовая
  семантика (`_commit_outcome` не зовёт `frontier.offer` для `is_revalidation`) не даёт
  ранее глубокой странице заново расширить discovery и сдвинуть coverage/стоимость.
- **`max_documents` — только discovery, не ревалидация.** `add_revalidate_seed` не
  проверяет `has_capacity`; `_commit_outcome` вызывает `note_fetched` только для
  `freshly_fetched and not is_revalidation`, а ревалидационный fetch учитывает отдельно
  в `Frontier.revalidation_fetched`; `_inflight_fresh` ревалидацию исключает. Потолок
  означает «документы, которые обход этого корня **добавляет**», а не повторную проверку
  уже установленных документов. 304 идёт через resume-path
  (`_reuse_for_page`/`_finish_one`) и не считается новым fetch; изменённый 200
  ревалидации — тоже не «добыча» для `max_documents`. Следствие: ровно **одна**
  `truncation_record` на каждый дренируемый авторский корень (не на URL) и отсутствие
  бюджетного голодания у корпусов больше потолка.
- **Resume-скан не заряжает ревалидацию в бюджет.** В `_drain_root` скан durable-элементов
  пропускает `revalidation_url_set` (все известные URL, включая отложенные cap'ом) до
  `_resume_source`. Иначе resumed-сессия зарядила бы их в `max_documents`, seed вернул бы
  `truncated`, а продукт ложно сообщил бы `complete=False` — свойство `B2`/`F13`,
  поднятое на уровень корня.
- **Дефект 29.10.2026: на resume бюджет заряжается не тому корню, и потолок умножается
  на число перезапусков.** Измерено на живом прогоне `ru-pg-dbms-3d3dc7c3c969`.
  Реконструкция «уже израсходованного» (`corpus.py:7379-7410`) идёт по продукту-wide
  множеству `resume_counted` (`:7066`), где побеждает **первый корень-претендент**
  («последующие корни его не пересчитывают», `:7379-7386`). У `digital_q_database`
  авторский порядок — host-корень `https://database.diasoft.ru/` (prefix `/`,
  `max_documents: 10`) первым, docs-корень `…/versiya-180/` (`max_documents: 800`) вторым
  (`packages/ru-pg-dbms/sources/corpus_urls.yaml:23-24`). Prefix `/` пропускает **все**
  URL продукта ⇒ host-корень заряжает на себя все пригодные к резюму элементы (≈1598),
  сразу усекается, а docs-корень получает `consumed = 0` и **полные 800** на новые
  страницы. Числа: до ребута dq имел 1610 `fetched` при потолке 800 — это ровно
  `2 × 800`, то есть два перезапуска; после resume за 20 минут прибавилось 321 (1610 →
  1931), и обход продолжился.
  **Почему это дефект, а не «щедрость».** `max_documents` объявлен как потолок обхода
  корня, а получился потолок **попытки**: один и тот же `run_id` даёт разный корпус в
  зависимости от того, перезагружался ли хост. `truncated`/`complete` наследуют это —
  значит `❌` и протокол исчерпания опираются на счётчик, который сбрасывается
  перезапуском.
  **Правильное правило уже есть в коде — для другого механизма.** Группировка
  ревалидации приписывает известный URL **самому специфичному** корню
  (`_group_revalidation_items`, `corpus.py:6653-6688`: «побеждает самый специфичный
  `root_path_prefix`»). Реконструкция бюджета должна пользоваться тем же предикатом, а не
  «кто первый». Это и есть правка: зарядка по владельцу, а не по претенденту.
  **Побочный вывод, который важнее правки.** Потолок 800 был объявлен по замеру 28.10
  («TOC `/versiya-180/` — 700 уникальных страниц»), но TOC — это только depth 1
  (измерено: 738 элементов). Реальное дерево: depth 2 — 893, depth 3 — 286, итого **1919
  URL** и 1110 уникальных документов на момент среза, обход ещё шёл. То есть потолок
  стоит ниже измерения — ровно то, что комментарий профиля (`domain_profiles.yaml`, ключ
  `database.diasoft.ru/docs/ru/q-database/versiya-180`) называет ложной аномалией:
  продукт объявляется `truncated`, и все его `❌` уходят в
  `❓`. Решение владельца 29.10.2026: **сначала дождаться конца прогона и измерить
  деревья по факту, потом поднимать потолки** — потолок ставится по замеру, а не по
  догадке (A3). Обе правки (учёт бюджета + потолки по замеру) — `execution_change`
  (потолок входит в `crawl_config_digest`), то есть новый `run_id`.
- **Корни Pangolin: дерево документации было вне scope (29.10.2026).** Живой прогон дал у
  `pangolin` **161 отказ `redirect_out_of_scope`**, все на `platformv.sbertech.ru` в пути
  `/docs/public/PSQ/<версия>/…`: `pangolin.sbertech.ru` редиректит на реальную
  документацию, а объявленным корнем того же хоста был маркетинговый путь
  `/products/rabota-s-dannymi/pangolin/`, который документацию не накрывает. Краул вёл себя
  верно — отказался тратить бюджет чужого корня.
  **Что объявлено (решение владельца: корни добавить, сканирование — следующим прогоном):**
  корень `https://platformv.sbertech.ru/docs/public/PSQ/8.1.0/` — редакция **из паспорта**
  (`version: Pangolin 8.1`), а не текущая 8.3.0: свидетельство другой редакции не
  подтверждает утверждение об объявленной (precision-first). Родитель `/docs/public/PSQ/`
  корнем не объявлен — его префикс накрывает 18 версий, бюджет делился бы между ними, а
  содержимое дублируется.
  **Бюджет по замеру:** дерево 8.1.0 — 15 разделов, **1299 страниц** (нижняя граница, сумма
  ссылок разделов); прежний `max_documents: 300` был втрое ниже измерения. Sitemap как
  источник размера не годится: 816 `<loc>`, под `/docs/public/PSQ/` — **ноль**.
  **Оси хоста не однородны по пути** (измерено): страницы документации отдают **200**
  обычному HTTP, маркетинговый корень — **401**. Поэтому профиль расщеплён по path-ключу
  (`host[/path]`, побеждает самый длинный `path_key`): docs — `http`/2000, остальной хост —
  `browser`+`honest`/300. До замера один host-ключ с `browser` заставлял статическое дерево
  платить цену браузерного рендера.
  Замер и сырьё — `docs/reports/measurements/2026-10-29-psq-docs-tree-scope.md`. Новый
  `run_id` — `ru-pg-dbms-7f9a23e3bcf9` (проверено `hound plan`); текущий прогон не затронут
  (профили читаются на старте процесса, `url_map` — из объявленных входов).
- **Orphans — наблюдение, не усечение.** Известные URL вне scope **всех** авторских
  корней (корень удалён, path-key сужен) дренируются в одном synthetic-бакете на
  `(product, origin)`: те же depth-0 листья, только ревалидация. Они видны в
  `IndexBuild.revalidation_orphans` и одной заметке фазы («author-scope drift»), но
  **никогда** не создают `truncation_record`, а сам orphan-факт не пишется в `errors`:
  synthetic-причина не должна демотировать `❌` без вины авторского корня, а I12 —
  статус не меняется. Группа known-URL, привязанная к авторскому **каталогу**
  (feed/channel), который сам не дренируется (он разворачивается в другие корни), тоже
  не теряется: она пишется в `revalidation_orphans` под отличимым ключом
  `"{product_id}:catalog:{catalog_url}"` с `"via": "catalog"` и счётчиками
  `selected`/`fetched`/`deferred`/`skipped`.
  Вывод корня из авторского scope — отдельная явная операция, не побочный эффект
  группировки.

**Фильтры work-set.** К known-URL применяются текущие `exclude_paths`/`exclude_documents`
и scope-проверка. Без потолка ревалидации решение принимает сам `Frontier`: неисполнимый
URL виден в `skipped_blacklisted`/`skipped_documents`. При заданном
`crawl.revalidate_max_documents` те же неисполнимые URL отсекаются **до** сортировки и
учёта слота (§8.7): иначе блэклист или pre-fetch-пропуск документа навсегда занимал бы
голову oldest-first очереди, не обновляя `last_checked_at`, и потолок каждый цикл тратил
бы слот на URL, который drain всё равно отвергнет. `max_documents` к ревалидации **не**
применяется (см. выше). URL вне scope не отбрасывается молча, а становится
orphan-бакетом. Операторский потолок числа условных GET —
`crawl.revalidate_max_documents` (§8.7), default-off; отдельной coverage-freshness оси он
не вводит и crawl-потолки не подменяет.

**Гарантия «новый продукт не потерялся».** `onboarding` уже проверил seed'ы; `corpus`
дополнительно (O2) не возвращает `ok` продукту с `expected_sources = 0`. `evaluate` (O3)
не оценивает продукт без строки `corpus_coverage`.

### 6.4. Evaluate (**design-only** в части переноса ячеек) — ⬜ не сделано

> **Статус: design-only.** `carry_forward_cells` в коде **нет**
> (`grep -rn "carry_forward\|measurement_origin" core/` — пусто, кроме DDL-колонки
> `cell_reuse_key` в таблице `cell_measurement`, у которой нет писателя).
> Фактический `evaluate_phase` переоценивает **все** ячейки продукта: `cells_carried`
> в снимках всегда 0 (измерено: 22 снимка в реальном `state.db` — у всех `cells_carried=0`).
> As-built-часть секции — только O3 (нет строки покрытия → `failed`-ячейки, не «неизвестно»).

```python
def evaluate_phase(ctx):
    for product in plan.products:
        coverage = store.read_corpus_coverage(run_id, product)
        if coverage is None:
            # O3: аномалия, не «неизвестно»  (as-built: _coverage_for даёт synthetic-строку)
            emit_failed_cells(product, failure_kind="coverage_missing")
            continue
        # design-only: carried = carry_forward_cells(...) при incremental + complete
        evaluate_missing_cells(product)
```

- Новый продукт: полный набор ячеек `plan.params × product`.
- Существующий: **сейчас** — тоже полный набор (переноса нет); в дизайне — carry-forward
  (§10), затем только оставшиеся ячейки.
- `failed`-ячейки (I4) — `status = null`, не статус.
- Продукт с `coverage is None` или `expected_sources = 0` — `failed`-ячейки с
  `failure_kind ∈ {coverage_missing, no_sources}`, **не** `❌`/`❓`.

### 6.5. Статус прогона и продукта (реализовано частично)

> **Статус: реализовано частично.** Таблицы `run_product` нет; per-product статус
> материализуется **снимком** в `state.db` (`product_snapshot`, `record_run_outcome`) и в
> отчёте прогона, а не строкой в run DB. Имена статусов ниже — из `product_snapshot.status`
> (`complete|partial|failed`) плюс `blocked` на уровне прогона.

`product_snapshot.status`:
- `complete` — coverage complete; **`pending`-ячейки допустимы** (они в очереди
  человеческого ревью, I6/I7, и не являются провалом измерения). Отдельный флаг
  `baseline_complete = (scope=full and evaluate_ok and cells_pending == 0)` определяет,
  можно ли обновлять `last_completed_run_id`;
- `partial` — покрытие есть, но неполно (усечение/нечитаемые origin'ы/блокирующие сбои);
- `failed` — покрытия нет (corpus/eval не состоялись: нет seed'ов, все origin'ы недоступны);
- `blocked` — на уровне **прогона**: onboarding-префлайт или гейт не пройдены.

Итоговый `run`:
- `blocked` — если onboarding-префлайт не пройден (продукт с `pending_seeds`), либо
  заблокировал **блокирующий** гейт. Блокирует только **G4** и **явный отказ**
  (`verdict=rejected`) по текущему предмету у любого гейта; `G1`/`G2`/`G3` без решения
  прогон не останавливают (`NON_BLOCKING_GATES`, `core/pipeline/gates.py:79`) — фаза идёт
  с `skip_reason=gate_not_blocking`, а в `gate_decisions` ложится waiver-строка
  `actor=policy` (§6.9). Прежняя редакция называла `g1`/`g2`/`g3` блокирующими — это
  поведение до решения владельца 26.10.2026;
- `partial` — если прогон дошёл до report, но часть продуктов неполна;
- `complete` — все продукты `complete`.

CLI: `EXIT_BLOCKED` для `blocked`, `EXIT_ERROR` для `partial` с блокирующими сбоями,
`EXIT_OK` для `complete` (как сейчас для сеанса).

### 6.6. Оси допуска терминальных артефактов (P6/P6b)

Краул различает два класса ответов по **поведению навигации**:

* **страница** — ответ, из которого извлекаются ссылки; RAM-потолок
  `crawl.max_response_bytes` (дефолт 20 MiB), сверх него — `payload_too_large`;
* **терминальный артефакт** — ответ, который навигацию **не продолжает**: документ
  (`pdf`/`docx`/`pptx`/`xlsx` — лист для экстрактора) **или** контейнер (`zip`/`7z`/`rar`
  из `crawl.attachment_formats` — вход для спуска). Такой ответ пишется потоком в
  staging-файл (не в RAM) и ограничен `crawl.max_artifact_bytes` (дефолт **250 MiB**,
  `DEFAULT_MAX_ARTIFACT_BYTES = 262_144_000`; это объявленный владельцем потолок хранения,
  а не степень двойки).

Класс определяется URL-суффиксом либо `Content-Type` (единый предикат —
`core/util/document_formats.is_terminal_artifact`). Контейнерная часть сужается
**позитивной вселенной** `crawl.attachment_formats` (дефолт `("zip", "7z", "rar")`);
незаявленный контейнер (`ova`/`iso`/`gz`/`tar`) остаётся обычным бинарным артефактом и
отсекается как прежде. Документная часть от конфига не зависит — она совпадает с реестром
экстрактора. OOXML (`docx`/`pptx`/`xlsx`) — тоже ZIP, но **документ**: различается по
маркеру `[Content_Types].xml` в голове байтов. `.rtf` **не** допускается: у реестра
экстрактора нет читателя RTF, допуск дал бы блокирующий `parser_failed` и мог бы
заблокировать `corpus_coverage.complete` (ждёт полосы `striprtf`).

**Два потолка, не один.** `crawl.max_response_bytes` — in-memory потолок страницы; он
**не** повышается для артефактов и HTML сверх него по-прежнему `payload_too_large`.
`crawl.max_artifact_bytes` — потоковый потолок артефакта (файл на диске, не RAM); сверх
него — `payload_too_large` с удалением staging-файла. Потолок применяется **и к фетчу, и к
resume**: `_resume_source` определяет класс по метаданным записи (`url`/`content_type`) и
сравнивает размер с artifact-потолком, поэтому staged-документ 100 MiB резюмится, а
страница того же размера — нет. На движке Scrapy `DOWNLOAD_MAXSIZE` поднимается до
`max(max_response_bytes, max_artifact_bytes)`, когда план допускает артефакты (иначе —
страничный потолок): Scrapy режет ответ **до** приёмника, и без этого `zip`/`pdf` умирали
на 20 MiB (as-built: на scrapy-движке тело всё равно буферизуется Scrapy в памяти —
файловый staging реализован в `HttpCrawler`; потолок снимает только преждевременный
отказ).

**Обход path-prefix (P6b).** Терминальный артефакт **не** подчиняется path-prefix
seed-root: `/docs/` вправе ссылаться на `/files/report.pdf` — это не «выход из раздела»,
потому что артефакт не продолжает навигацию. Обход применяется в трёх точках допуска:
`Frontier.discover` (BFS-ссылки), `Frontier.add_revalidate_seed` (известные URL) и
`_fresh_sitemap_seeds` (sitemap `<loc>` — артефакт вне префикса **ставится в очередь**, а
не считается `skipped_out_of_scope: path`). **Что при этом не отменяется:** host scope,
SSRF-валидация, `..`-traversal-guard (`decoded_scope_path → None` всегда отвергается) и
блэклист оператора (`exclude_paths`/`exclude_documents` побеждает — проверяется **после**
scope-допуска). Обход **видим**: счётчик + ограниченные примеры
(`artifacts_outside_prefix` на `Frontier`) попадают в провенанс корня рядом с
`skipped_blacklisted`/`skipped_documents`; тихий обход — дефект.

| Ось | Смысл | Дефолт |
|---|---|---|
| `crawl.max_response_bytes` | **RAM**-потолок страницы (прежняя семантика) | 20 MiB |
| `crawl.max_artifact_bytes` | потоковый потолок терминального артефакта (staging-файл, не RAM) | 250 MiB |
| `crawl.attachment_formats` | позитивная вселенная форматов-контейнеров | `zip`, `7z`, `rar` |
| `crawl.archive_max_member_bytes` | потолок одного члена контейнера (спуск) | 64 MiB |
| `crawl.archive_max_uncompressed_bytes` | потолок суммарного распакованного объёма (спуск) | 256 MiB |
| `crawl.archive_max_members` | потолок числа членов (спуск) | не задан |
| `crawl.archive_descent` | `plan` — только состав, `full` — спуск | `full` |

**Инварианты поведения.** Страница ≤ `max_response_bytes` идёт прежним буферизованным
путём **байт-в-байт** (те же `content`, `content_hash`, retention-файл). Артефакт свыше
RAM-потолка пишется в staging-файл и публикуется в `corpus_raw` **переездом**
(`os.replace`), без чтения в RAM; хеш считается потоково по файлу (I19 — адаптерному хешу
не доверяем). Документ из staging-файла **извлекается** как прежде: экстрактор работает от
байтов retained-файла (inline-путь перечитывает тот же файл, как это уже делает воркер
пула), поэтому класс допуска не меняет ни маршрут, ни результат экстракции. Границы спуска
(`archive_*`) — не методология, а исполнение: превышение = видимый
`payload_too_large`/skip, не тихий частичный текст.

**Резолв и версия.** Цепочка app < run < site, как у прочих осей краула. Ось — операционная:
её смена — версионная граница класса `execution_versions` (`execution_change`, §13), **не**
`methodology_version`; обязателен `golden-full`, пере-аннотация golden не нужна. В дайджест
(`CrawlLimits.as_dict()`) ось попадает **только при явном значении** (site/run-слой либо
явный app-конструктор): дефолты не меняют дайджест старых прогонов.

### 6.7. Контракт спуска в контейнеры (B2, as-built)

Срез **B2** — субстрат спуска: порт, адаптеры и политика. Сам по себе срез **не**
подключал `pipeline` к спуску (в графе импортов прогона ничего не менялось): допущенный
контейнер ретейленился и не отдавался экстрактору. Смысл среза в том, что проектных
решений для подключения не остаётся: всё, что решается **по индексу**, уже решено и
покрыто тестами без store, прогона и бэкенда.

> **Подключено срезом B3 — читать вместе с §6.7.1.** Прежняя редакция этой секции
> утверждала, что спуск «не подключён к `pipeline`» и является «следующим шагом». Это
> описание состояния до B3 и коду больше не соответствует: `ArchiveDescentWalker`
> вызывается из фазы корпуса (`core/pipeline/corpus.py:7676`), зонд артефакта — `:3256`,
> `:5390`, `:7787`; отказ читателя контейнера виден как
> `archive_observations.status = unreadable` / `container_reader_unavailable`, а не как
> тихое «не развернули». Подключение — инъекция `container_for` через `Ports.container`,
> поэтому `pipeline` по-прежнему не импортирует `adapters`.

**Код.** `core/ports/container.py` (`ContainerPort` + `ContainerMember`/`ContainerListing`,
`ContainerUnavailable`), `core/adapters/container_zip.py` (stdlib `zipfile`),
`core/adapters/container_7z.py` (`py7zr`, extra `extract-archives`),
`core/adapters/container_rar.py` (`rarfile` + внешняя утилита), `core/adapters/container_any.py`
(диспетчер по голове 8 KiB), `core/util/container_policy.py` (политика допуска членов).

**Инвариант 1 — листинг не есть извлечение.** `list_members` читает **только индекс**
контейнера (zip central directory / заголовок 7z / заголовок RAR). Размер члена —
**заявленный** размер из индекса, а не измеренный распаковкой; листинг 4 GiB архива не
распаковывает ни байта. Доказательство в тестах: архив с испорченным телом члена
листается, а чтение этого члена честно падает.

**Инвариант 2 — чтение потоком.** `open_member(path, member_path)` отдаёт итератор
чанков (1 MiB) и не требует держать в памяти ни член, ни архив. `zip` и `rar` читают
инкрементально (`ZipFile.open`, `RarFile.open`). `7z` — честная граница: у `py7zr` нет
инкрементального читателя (`read(targets=…)` отдаёт `BytesIO`, то есть член целиком в
RAM), поэтому адаптер извлекает **один** член во временный каталог и стримит файл
чанками. RAM ограничена, но распакованный член проходит через диск; в солидном архиве
`py7zr` всё равно распаковывает блок целиком.

**Bomb-guard — политика, не библиотека.** Заявленный размер — только **первый** гейт (он
врёт: zip может объявить одно, отдать другое). Оси `archive_max_member_bytes` /
`archive_max_uncompressed_bytes` / `archive_max_members` (§6.6) отсекают по индексу,
а `container_policy.bounded_iter(chunks, limit)` — **второй** гейт: он считает фактические
байты и поднимает типизированный `MemberTooLarge` на первом чанке, выводящем сумму за
потолок. Пробивший чанк не отдаётся: частичное тело выглядело бы как успешный
(усечённый) документ.

**Идентичность члена — `(archive_content_hash, normalized_member_path)`.** Хеш архива —
тот же, что посчитало ядро при приёме (I19), нормализованный путь —
`normalize_member_path` (POSIX; абсолютный путь, `..`-сегменты, буква диска, NUL и пустое
имя отвергаются — член не выходит за пространство имён архива; обратные слэши приводятся
к прямым, иначе `..\..\x` из Windows-zip выглядел бы одним безобидным сегментом).
Нормализация — только для идентичности и отчёта: `open_member` принимает **сырой** путь
архива, потому что именно он лежит в индексе.

**Архив — сырьё, члены — производный вид.** Ретейленится и хешируется **архив** (это и
есть `corpus_raw`/I19); байты членов отдельно **не** хранятся — они ре-выводимы из
ретейленного архива тем же `open_member`. Отсюда два следствия: (1) повторный спуск не
требует нового фетча, только чтения ретейленного файла; (2) «член удалён» не бывает
отдельной операцией — GC архива снимает и членов.

**Вложенность.** В срезе B2 спуск был описан как одноуровневый (член-контейнер попадал в
видимый пропуск `not_ingestible_format`). В B3 вложенность **исполняется** без потолка
глубины; конечность обеспечивают цикл по хешу содержимого и кумулятивный бюджет корня —
см. §6.7.1.

**Словарь видимых пропусков.** Причина на каждый пропущенный член; порядок прецедента
детерминирован (структурные и содержательные отказы раньше бюджетных — иначе «не тот
формат» маскировался бы под исчерпанный бюджет):

| Причина | Смысл | Урезает план (`truncated`) |
|---|---|---|
| `is_directory` | запись-каталог, тела нет | нет |
| `path_rejected` | путь не принадлежит пространству имён архива | нет |
| `encrypted` | что заявил читатель о шифровании контейнера: `encrypted` / `not_encrypted` / `unknown` (три состояния; «молчит» ≠ «не зашифрован») | нет |
| `not_ingestible_format` | суффикс вне реестра форматов + текстовых целей спуска | нет |
| `member_too_large` | заявленный размер члена выше `archive_max_member_bytes` | да |
| `member_limit_exceeded` | исчерпан `archive_max_members` | да |
| `total_uncompressed_exceeded` | сумма заявленных размеров выше `archive_max_uncompressed_bytes` | да |

**Прогноз до извлечения (форма будущих `archive_observations`).**
`container_policy.archive_forecast(listing, plan)` — JSON-сериализуемая запись:
`format`, `members_total`, `members_accepted`, `formats` (гистограмма суффиксов
принятых), `total_uncompressed`, `solid`, `encryption` (три состояния; ключ прогноза сохранён
как `encrypted` ради читаемости прежних наблюдений, значение — `encrypted`/`not_encrypted`/
`unknown`), `skipped`
(`{причина: {"count": N, "examples": […]}}`, примеры ограничены — как
`artifacts_outside_prefix`), `truncated`, `representative` (наименьший нормализованный
путь среди принятых), `unreadable`. Прогноз фиксируется **до** извлечения: он объясняет,
почему спуск взял меньше, чем «есть в архиве», и переживает отказ посреди извлечения.

**Солидность — честная оговорка.** `solid` берётся из индекса **как есть**: `zip` —
`false` по построению (каждый член — независимый deflate-поток), `7z` — флаг архива, если
бэкенд его отдаёт, иначе `null`, `rar` — `null` (`rarfile` флаг не сообщает, догадка
исказила бы прогноз). Почему это важно для стоимости: в **солидном** архиве чтение
одного члена стоит распаковки всего блока — то есть `N` членов стоят `N × блок`, а не
`N × член`. Zip этого недостатка не имеет; 7z и solid RAR — имеют, и прогноз обязан
показывать `solid` до спуска.

**Отсутствие бэкенда деградирует видимо.** `available()` — конъюнкция всех условий
(`rar` — пакет **и** внешняя утилита: `bsdtar`/`unrar`/`7z`), а `list_members` при
отсутствии возвращает `unreadable="backend_unavailable: …"` с точной причиной
(`py7zr не установлен (extra extract-archives)` / `rarfile не установлен …` /
`нет внешней утилиты (bsdtar/unrar/7z)`). Тихая подмена пустым листингом запрещена:
«не умею читать» и «внутри пусто» — разные факты.

### 6.7.1. Исполнение спуска (B3, as-built)

Срез B3 подключает субстрат к фазе корпуса: `attachment_skip` заменён реальным спуском.

**Зонд артефакта — до экстракции.** `core/util/document_probe.py` (класс/формат/размер по
байтам, `page_count` PDF **по структуре файла** — `/Type /Pages` + `/Count`, без рендеринга)
и `core/pipeline/artifact_probe.py` (плюс листинг контейнера через `ContainerPort`). Зонд
вызывается для каждого допущенного терминального артефакта и для каждого члена; запись
попадает в `PhaseResult.log_entries`. `ArchiveDescent=plan` останавливается здесь: листинг и
прогноз есть, членов нет.

**Инъекция.** `container_for` (`Callable[[path], ContainerPort | None]`, composition root
передаёт `container_any.container_for`) приходит в фазу как `Ports.container` — `pipeline`
по-прежнему не импортирует `adapters`. `None` → спуск не исполняется, но **видимо**:
`archive_observations.status = unreadable`, причина `container_reader_unavailable`.

**Члены и наблюдения.** `corpus_members` (PK `(archive_content_hash, member_path, product_id)`) —
идентичность §6.7: путь нормализован, `archive_path` позволяет ре-вывести член,
`doc_id`/`skip_reason` фиксируют исход, `extracted_content_hash` — хеш извлечённого текста
(не байтов). `archive_observations` — прогноз (`archive_forecast`, **до** извлечения) и
исход (`members_ingested`/`members_failed`/`skipped` с причинами и ограниченными примерами/
`stopped_reason`/`cycle_stops`/`bytes_written`). Запись есть для **каждого** контейнера,
включая пустой и нечитаемый. Обе таблицы добавлены аддитивно (`CREATE TABLE IF NOT EXISTS`)
в схеме 3.10.0; текущая `SCHEMA_VERSION` — `3.13.0` (`core/store/sqlite.py`), то есть
колонки с тех пор дополнялись в рамках той же таблицы.

**Вложенность без потолка глубины — и всё же конечная.** Оси `archive_max_*`
ограничивают **один** контейнер, поэтому введены два независимых механизма:

* **цикл по хешу содержимого**: множество хешей архивов, пройденных в этом корне; повтор
  (в т.ч. «архив, содержащий копию себя» — в конструктивной форме повторных байтов)
  не разворачивается, причина `archive_cycle`;
* **кумулятивный бюджет корня** — новая ось `archive_max_total_uncompressed_bytes`
  (дефолт 1 GiB, `execution_change`): сумма фактически прочитанных байтов по всем уровням.
  Исчерпание — **видимая** запись `archive_budget_exhausted` (`status=budget_exhausted`),
  а не тихое завершение.

**Поток (сверху вниз).** Каждый принятый член:

1. **стримится** `bounded_iter(open_member(...))` в **свой** временный файл — архив целиком
   в файл не пишется никогда;
2. **отправляется в фазовый пул** извлечения (`ExtractionPool.worker`) с окном упреждения
   `K = extract_window` — извлечение идёт **в воркере**, а не в родителе;
3. **доводится строго FIFO**: результат возвращается в порядке отправки, и только на пустом
   окне начинаются запись в видимые списки (`errors`, `members`, `skipped`) и любой
   вложенный спуск;
4. **ингестится** отдельным `CorpusDoc` маршрутом **по своему** суффиксу (pdf → `document`,
   md/html → `auto`): `url = {archive_url}!/{member_path}`, `raw_path` = ретейленный
   **архив** — байты члена отдельно не ретейнятся. Дедуп — по `content_hash` в пределах
   продукта (I17).

**Почему шаг 3 обязателен, а не косметика.** `_content_digest` сборки склеивает
`chunk_ids`/хеши/матрицу **в порядке вставки**, а `partition_build_id` порядко-независим,
поэтому коммит в порядке завершения дал бы тот же build id при другом дайджесте →
`ImmutableBuildError` и молча незаписанные `corpus_chunks`/`index_manifest`. Временный файл
члена живёт до его доводки (воркер читает его из другого процесса) и удаляется сразу
после; каталог спуска чистится в `finally`.

**Извлечение члена — в фазовом пуле, с окном и FIFO-доводкой (22.10, операционный
рычаг).** Измеренный дефект: один zip с 47 PDF занимал ~80 минут, при этом дерево
процессов показывало родителя на 561 % CPU и простаивающих воркеров — члены извлекались
**в родителе**, синхронно, по одному, а ось `extract_workers` не получала работы. Теперь
член уходит в тот же фазовый пул, что и страница (`ExtractionPool.worker`), с окном
упреждения `K = extract_window` (той же оси: новая ось попала бы в `CrawlLimits.as_dict()`
→ `crawl_control_digest` → `EXECUTION_CONTRACT_FIELDS` и форкнула бы `run_id` ради чистого
рычага пропускной способности — §13, это не смена условий измерения).

При `K = 1` (рециклинг воркеров) и при отсутствии пула (`split`, `defer_extract=False`)
путь тождествен прежнему inline — наблюдаемые артефакты совпадают байт-в-байт. Члены
получают trim арен glibc в `finally` воркера (`_extract_worker`) и входят в операторский
счётчик «документов через пул»; отказ извлечения члена — по-прежнему видимый
`extract_failed`, соседи не страдают, а отказ пула на `submit` деградирует до inline с
явной записью (как у страницы), не теряя членов. Отказ пула посреди спуска пересобирается
**страничным** путём (`_finish_extract`): спуск исполняется внутри `item.commit`, где
счётчик `pending` корня ещё включает сам архив, поэтому рекурсивная доводка той же фазовой
очереди запрещена.

**Покрытие — честная граница.** Нечитаемый контейнер (индекс не прочитан) — это
ненаблюдённое содержимое, поэтому элемент помечается `FAILED` существующим классом
`parser_failed` (новый `failure_kind` не вводится): `complete=False`, причина видна в
`archive_observations.reason` и `errors`. Читаемый контейнер с нулём ингестнутых членов
остаётся `FETCHED` — листинг **и есть** наблюдение («внутри нет ингестируемого» видно в
прогнозе/исходе), и это не маскируется под «контент прочитан»: запись наблюдения
существует даже при нулевом выходе.

**`split`.** Байты архива живут на стороне ядра (`corpus_raw`), поэтому спуск исполняется в
ядре (листинг/стриминг — локальный файл, не веб-эгресс). Извлечение члена контейнера швом
**покрыто**: `RemoteExtractor.supports_source_path = True`
(`core/adapters/remote_extractor.py:214`) — шов `/v1/extract/{upload,submit,collect,abort}`
принимает **байты артефакта**, а не URL, и для членов это единственный корректный путь (их
URL синтетический, фетчить его нельзя, а файл на диске есть). Прежняя редакция этой секции
утверждала `supports_source_path = False` и видимый пропуск `extract_unavailable` — это
описание до среза B1/B3 и коду не соответствует.

**Идентичность экстрактора под `split` — в пространстве имён ядра.** `extractor_version`
приходит в ответе `extract/collect` (`docling-doc-3:<level>/<device>/docling-<build>`), а не
подменяется строкой вида `remote-extractor-1` (`core/edge/extract_service.py`, докстринг
модуля; `RemoteExtractor.__init__` — `version` остаётся идентичностью **прокси**). Это снимает
прежнюю предпосылку «паритет `pb` local/split невозможен по построению»: `pb` включает
наблюдаемую версию, и условие паритета — **та же строка**, а не «тот же текст». Сам паритет
при этом **не измерен** (нужен один корпус под обоими профилями) — снята блокировка, а не
незакрытая задача.

**Дополнительные видимые причины пропуска члена:** `archive_cycle`,
`archive_budget_exhausted`, `extract_failed`, `extract_unavailable`,
`member_stream_too_large`, `member_read_failed`, `nested_descended`,
`container_reader_unavailable`.

### 6.8. Текстовые форматы и EPUB: читатель, маршрут, чанкер (as-built, 22.10)

**Измеренный дефект, который это закрывает.** В прогоне 22.10 `parser_failed` — самая
крупная блокирующая корзина (76), и 73 отказа — дефект маршрутизации, не сайта: 72 × `.txt`
(файлы целы, 874–35 005 Б, валидный UTF-8) и 1 × `.rtf` уходили в HTML-путь, trafilatura
возвращала `None` на не-HTML, и документ умирал в `parser_failed`. Поскольку
`PARSER_FAILED` входит в `BLOCKING_FAILURE_KINDS`, `corpus_coverage.complete` не мог стать
`True` **по построению**, а `❌` — не публиковаться. Ещё 47 × `.epub` стояли в очереди и
провалились бы так же.

**Класс форматов.** Plain text и markdown — **первый класс наравне с HTML и PDF** (решение
владельца), EPUB — отдельный читатель. Реестр один:
`core/util/document_formats.py` — `MARKDOWN_SUFFIXES` (`.md`, `.markdown`, `.mdown`,
`.mkd`), `PLAIN_TEXT_SUFFIXES` (`.txt`, `.text`, `.log`, `.sql`, `.rst`), `TEXTUAL_SUFFIXES`
(объединение), `textual_family(url, content_type) -> "markdown" | "text" | None`,
`is_epub(url, content_type)`. Приоритет сигналов: суффикс раньше `Content-Type`, а суффикс
документа (`.pdf`/`.docx`/…) и `.epub` **сильнее** текстового `Content-Type` —
`report.pdf`, отданный как `text/plain`, остаётся документом. `.epub` **не** входит в
`DOCUMENT_SUFFIXES` (этот реестр — вход docling) и не стал терминальным артефактом:
допуск краула не менялся.

| Семейство / формат | Сигнал | Читатель | Чанкер |
|---|---|---|---|
| HTML (страница) | остальное (по умолчанию) | `HtmlExtractor` (trafilatura) | `MarkdownChunker` |
| markdown | `.md`/`.markdown`/`.mdown`/`.mkd`, `text/markdown`, `text/x-markdown`, `application/x-markdown` | `TextExtractor` (`text-plain-1`) | `MarkdownChunker` (`md-heading-3`) |
| plain text | `.txt`/`.text`/`.log`/`.sql`/`.rst`, `text/plain` | `TextExtractor` (`text-plain-1`) | `TextChunker` (`text-para-2`) |
| документ | `.pdf`/`.docx`/`.pptx`/`.xlsx` + MIME | `DoclingDocumentExtractor` | `MarkdownChunker` |
| EPUB | `.epub`, `application/epub+zip` | `EpubExtractor` (`epub-1`) | `MarkdownChunker` |

**Маршрут в композите** (`core/cli/wiring._LocalExtractor`, `core/edge/__main__._EdgeExtractor`):
явный `HTML` → явный `DOCUMENT` → текстовое семейство (суффикс, затем `Content-Type`) →
EPUB → документный суффикс/тип → HTML. Новое значение `ExtractionRoute` **не** вводилось:
маршрут — производная формата страницы, а не операторская ось; добавление значения
затронуло бы `docs/status-dictionary.yaml` и всех потребителей оси, ничего не добавив к
поведению. Композит строится **всегда** (в том числе при `document_level=off` и без
docling): текстовый и EPUB-пути от docling не зависят, а возврат «голого» `HtmlExtractor`
вернул бы `.txt`/`.epub` в trafilatura.

**Текстовый читатель** (`core/adapters/extract_text.py`, `text-plain-1`) не интерпретирует
разметку: charset из `Content-Type` (fallback UTF-8 `errors="replace"`), снятие BOM,
CRLF/CR → LF, удаление NUL/управляющих символов (таб сохраняется), нейтрализация каркаса
промпт-инъекций, схлопывание >2 пустых строк до одной. Пустой/пробельный результат —
видимая ошибка `"текст пуст"`, а не пустой успех. `page_count=None`.

**EPUB через контейнерный субстрат** (`core/adapters/extract_epub.py`, `epub-1`; stdlib
`zipfile` + `xml.etree`, без новых зависимостей): `META-INF/container.xml` → rootfile
`full-path` → OPF → `manifest` (id → href, media-type) → `spine` (упорядоченные idref).
Главы склеиваются **в порядке spine** (порядок чтения; `manifest` — инвентарь ресурсов),
каждая — через тот же HTML-экстрактор (собственный HTML-ридер не дублируется), с
заголовком `## <title>` (иначе — имя файла), чтобы `heading_path` был осмысленным.
`href` разрешается относительно каталога OPF и percent-декодируется; выход за пространство
имён архива отвергается (`normalize_member_path` → `None`). Потолки — оси
`archive_max_member_bytes` и суммарный `archive_max_uncompressed_bytes` через
`container_policy.bounded_iter` (бюджет общий на документ). Отсутствующая/нечитаемая глава
остаётся **видимой заметкой** на своём месте в порядке чтения, а не тихим пропуском; нет
`container.xml`/OPF/spine или архив не ZIP — видимая ошибка, не исключение. Байты читателя
не пишутся в `corpus_raw` отдельно: ретейленится исходный `.epub` (I19).

**Чанкеры.** Абзацный чанкер `core/adapters/chunker_text.py` (`text-para-2`): блоки по
пустым строкам, слияние до `chunk_size`, блок, который помещается, не режется, блок сверх
`chunk_size` режется по границам предложений (`. ! ? ;` и `…`), и только «бесконечное»
предложение — жёстко (по возможности по пробелу). Порог фрагмента — `MIN_CHUNK_SIZE = 20`
символов (замер распределения: полоса `<20` — 0.76 % чанков, и в ней 1 «фразовый» кусок из
71, остальное `|`, номера страниц, пути); ниже порога кусок в индекс не попадает.
`heading_path=""` всегда,
`context_prefix` — первое предложение первого абзаца чанка (≤200 символов); он идёт
**шапкой фрагмента в промпт**, в эмбеддинг не входит (эмбеддится чистый `text`). Диспетчер —
`core/adapters/chunker_family.FamilyChunker` по `ExtractedDoc.family` (аддитивное поле
порта: `"markdown"`/`"text"`/`"epub"`; `None` = markdown-совместимое семейство, так ведут
себя HTML и docling).

**Контрактная граница.** `chunker_version` — поле `EXECUTION_CONTRACT_FIELDS`, поэтому с
двумя чанкерами оно стало **композитом** `"md-heading-3+text-para-2"`
(`chunker_family.composite_chunker_version`, тот же источник у декларации и у собранного
порта). Это версионная граница класса `execution_versions` (`execution_change`, §13):
меняется `run_id`, `METHODOLOGY_VERSION` — нет.

**Семейство живёт и в конверте `extract_cache` — не только в `ExtractedDoc`.** Ключ кэша —
`(content_hash ретенированных байтов, extractor_version, level)` — семейства **не
содержит**, но конверт (`core/pipeline/extract_cache.py`) несёт аддитивное поле `family`
(наряду с `format`, `content_hash`, `extractor_version`, `level`, `text`, `page_count`,
`doc_content_hash`). Без него хит кэша вернул бы `ExtractedDoc` **без** семейства, и
текстовый документ ушёл бы в markdown-чанкер. `family=None` в конверте (старые конверты,
до появления текстового чанкера) трактуется как **markdown-совместимое** семейство — так
ведут себя HTML и docling. Практическое следствие: смена семейства при том же
`(сырьё, extractor_version, level)` **не** инвалидирует кэш — это корректно, потому что
семейство выводится из суффикса/`Content-Type` того же URL, а ключ уже адресует те же
байты; но если читатель начнёт возвращать другое семейство для тех же байтов, конверт надо
версионировать (`format`), а не полагаться на ключ.

**Чего нет (осознанно).** `.rtf` остаётся без читателя: допуск дал бы блокирующий
`parser_failed` и мог бы заблокировать `corpus_coverage.complete`. Решение владельца —
полоса `striprtf` отдельно, не «заодно с текстом».

---

### 6.9. Гейты G1–G4: механизм (as-built)

Гейт **никогда не проходится автоматически**: оркестратор не «одобряет» гейт сам, он лишь
регистрирует решение. Носитель и аудит-след — таблица `gate_decisions` в run DB.
Два разных типа гейтов используют один механизм.

**Что блокирует прогон.** `NON_BLOCKING_GATES = {G1, G2, G3}` (`core/pipeline/gates.py:79`,
решение владельца 26.10.2026): нет действующего решения — фаза идёт дальше, а в
`gate_decisions` пишется строка-политика (`actor=policy`,
`external_ref=waiver:policy:nonblocking:{gate}`, `skip_reason=gate_not_blocking`), которая в
следе манифеста читается как `state: not_required`. Блокируют только **G4** и **явный
отказ** (`verdict=rejected`) по текущему предмету у любого гейта: отказ — это решение, а
молчание внешней системы не выдаётся ни за отказ, ни за одобрение. Прежняя редакция этой
секции описывала «гейт блокирует без решения» для всех четырёх — это поведение до
`729b736`; см. также `AGENTS.md` («Гейты G1–G4»).

**G1/G2 — внутренняя аттестация.** Человек утверждает содержимое **профиля** пакета-якоря
(когорта/роли/объём/паспорта — G1) и матрицу роль×продукт (G2). Пути прохождения, в порядке
приоритета (`_gate_phase`, `core/pipeline/runner.py`):

1. **per-run решение** — строка в `gate_decisions`, привязанная к текущему предмету
   (`subject_digest`) и с вердиктом `approved`. Это **приоритетный** путь: аудит-след и
   override на прогон;
2. **аттестация профиля** — durable-таблица `profile_attestation` в
   `state/{package_id}/state.db` (`STATE_SCHEMA_VERSION = "5"`): человек аттестует
   `profile_digest` один раз, и прогоны с тем же содержимым профиля не требуют повторного
   решения (профиль квази-константен: 2–5 правок в год при еженедельных прогонах).
   Резолвер — `make_profile_gate` (`core/pipeline/profile_attestation.py`), подключён в CLI
   (`_profile_gate_resolver`, `core/cli/context.py:1187`; вызов `make_profile_gate` — `:1214`). Аттестация применима **только** к
   G1/G2; отозванная (`superseded_at`) действующей не считается. При расхождении дайджеста
   сообщение **называет** изменившийся компонент (`describe_subject_diff`), а не «гейт не
   пройден».
   **Нет производителя в прогоне (design-only):** строку `profile_attestation` пишут только
   тесты — команды `hound profile attest`, которую сообщение предлагает как способ
   пере-аттестации, **нет** (в `core/pipeline/profile_attestation.py` она помечена
   «вводится отдельной полосой работ»). Измерено: 0 строк во всех реальных `state.db`.
   Поэтому описывать этот путь как работающий нельзя: **рабочий путь сегодня один —
   per-run строка `gate_decisions`** (он же приоритетный).

**G3/G4 — точки интеграции, а не внутренние решения.** coHound не решает, пройдёт ли
strict-ревью (G3) и публикация (G4): он **экспортирует предмет** и **записывает** ответ
внешней системы. Предмет детерминирован и не содержит меток времени:

- **на блоке** пишется `runs/{run_id}/gates/{gate}-request.json` (`write_gate_request`,
  `core/pipeline/gate_subject.py`): `run_id`, `gate`, `subject_digest`, `subject`,
  `decision_contract`, `export`. Один и тот же прогон в одном состоянии даёт побайтно тот
  же файл; перезапись идемпотентна. `subject_digest` из этого файла — ровно тот, который
  обязано вернуть совпадающее решение;
- **решение** записывается в `gate_decisions` с полями `subject_digest`, `source`
  (`internal|external`), `external_ref`, `verdict` (`approved|rejected`). Для
  `--source external` **обязательны** `--external-ref` и `--notes`: решение без ссылки на
  решающую систему и без причины — не аудит-след;
- **устаревшее одобрение не проходит.** `evaluate_gate` сверяет предмет: точное совпадение
  `subject_digest` → решает последнее решение с этим отпечатком; одобрение с другим
  отпечатком → `stale` (блок), отказ с другим отпечатком → `rejected_stale` (блок с
  сообщением, что отказ относится к прошлому состоянию и повторное ревью может быть
  обосновано);
- **явный отказ блокирует отдельным сообщением** («отклонён внешней системой (ref):
  notes»). Молчание внешней системы не выдаётся ни за отказ, ни за одобрение: отсутствие
  решения — `missing`, тоже блок, но с другим текстом.

**Waiver-политика (`run.yaml:gates.{g3,g4}: external|waived`, дефолт `external`).**
`waived` означает «решение делегировано процессу вне coHound целиком». Тогда фаза
возвращает `skipped` с `skip_reason = gate_delegated_external`, а факт делегирования
**записывается видимо**:

- waiver-строка в `gate_decisions` (`source=external`, `external_ref="waiver:run.yaml:gates.g3"`,
  `verdict=approved`, `actor="external-process"`, `target="waiver"`); повторный вызов её не
  дублирует;
- `gate_waivers` в манифесте прогона (`gate_provenance`, `core/pipeline/phases.py`) плюс
  `gate_policies` — что объявлено в `run.yaml:gates`;
- waiver-строки **не** считаются решением об одобрении при сверке предмета: иначе смена
  `waived` → `external` оставила бы гейт «пройденным» фактом делегирования.

Невидимый waiver запрещён: прогон, опубликованный по waiver, не должен выглядеть как
прогон с одобрением ревью/публикации.

**Политика — процесс, не условие измерения.** `run.yaml:gates` **не** входит в
`EXECUTION_CONTRACT_FIELDS`: иначе смена процедуры меняла бы `run_id` и рвала межцикловый
diff. В манифесте она видна, в контракте — нет.

**CLI.** `hound gate --runs-dir runs --run-id {run_id} --gate G1..G4 --actor {кто}
[--target …] [--source internal|external] [--verdict approved|rejected]
[--subject-digest …] [--external-ref …] [--notes …]`. `--subject-digest` по умолчанию
берётся из `runs/{run_id}/gates/{gate}-request.json` (`read_gate_request_digest`).

**Гейты per-run.** Новый `run_id` требует новых решений; строка `gate_decisions` прошлого
прогона не действует. Единственное, что переносится между прогонами, — `profile_attestation`
(и только когда у неё появится производитель, §6.9 выше).

---

## 7. Seeds

> **Статус: ✅ сделано** (авторский файл, нормализация, приоритет источников, привязка к
> профилям и CLI-набор `hound package seeds list/add/disable/enable/export`, §2.3). Ниже
> **planned** остаются только `seeds validate` и `seeds migrate` — их нет, и это названо
> по месту.

### 7.1. Канонический артефакт

`packages/{case_id}/sources/seeds.yaml` — авторские данные (git), не оперативное состояние.

```yaml
schema: seeds/1
products:
  acme-db:
    state: ready            # ready|pending_seeds
    seeds:
      - url: https://acme.example/products/db/
        role: primary
        source: add_product
        source_ref: sources/add_product/2026-10-19.json
        profile_key: acme.example/products/db
        added_at: '2026-10-19'
        added_by: alice
        enabled: true
      - url: https://docs.acme.example/
        role: docs
        source: discovery
        profile_key: docs.acme.example
        added_at: '2026-10-19'
        added_by: alice
        enabled: true
  acme-analytics:
    state: pending_seeds
    seeds: []
```

**Почему новый файл, а не расширение `corpus_urls.yaml`:**

- `corpus_urls.yaml` — плоский `{product_id: [url]}` без роли/провенанса/profile-binding;
  его генерирует `packages/{case}/sources/build_package.py` из паспортов, и он используется
  как операторский `--urls`. Расширять его — смешать «сгенерированный экспорт» и
  «авторский seed-набор».
- `seeds.yaml` — источник истины для `run`; `corpus_urls.yaml` остаётся **производным
  экспортом** для обратной совместимости и внешних скриптов:
  `hound package seeds export --format corpus-urls` — **✅ реализовано**
  (`seeds_export`, `core/pipeline/package_seeds.py:459`; `render_corpus_urls`,
  `core/config/seeds.py:257`; round-trip проверяется тестом).
- `build_package.py` должен либо генерировать `seeds.yaml`, либо выдавать
  `generator_drift` warning (как сейчас для `domain_profiles.yaml`,
  `package_add_product.py`). В MVP: `add-product` пишет `seeds.yaml`, а
  `build_package.py` получает warning, если файл существует и не совпадает с деривацией.

### 7.2. Нормализация и дедуп

`normalize_seed_url(url)`:
- схема только `http`/`https`;
- lowercase host, удаление default port (`:80`/`:443`);
- удаление fragment;
- сохранение path case (path может быть case-sensitive);
- trailing slash: `https://h/p` и `https://h/p/` — **разные** URL (сервер может отдавать
  разное); дедуп — по точному нормализованному виду;
- query сохраняется (может быть значимым), но seed'ы с query запрещены политикой
  (`seeds validate` warning — **planned**), чтобы не плодить дубликаты пагинации.

`seed_id = short_hash(product_id + "\x1f" + normalized_url, 24)`.

### 7.3. Валидация

1. `core.util.security.validate_url` (SSRF: private IP, metadata endpoints, file://, и т.п.).
2. Схема/хост/порт.
3. `profile_key` существует в `domain_profiles.yaml` **или** создаётся `add-product`.
4. Seed не выходит за scope продукта: host/path должен покрываться profile-key;
   host-root seed допустим только с `max_depth: 0` (иначе `I17`-риск: затянет чужие продукты).
5. Дубликат `(product_id, normalized_url)` — merge, не ошибка (idempotent re-add).
6. Seed не должен указывать на sitemap/feed URL — для них есть `crawl_sources.yaml`
   (`kind: sitemap|feed|channel`). `seeds validate` предупреждает (**planned**).

### 7.4. Привязка к `domain_profiles.yaml`

- `profile_key` — самый длинный ключ `host[+path]`, чей host совпадает и path является
  префиксом URL.
- `add-product` уже пишет профили для обнаруженных сайтов; `plan_seeds` проставляет
  `profile_key` после `write_domain_profiles_with_new_keys`.
- `seeds add` без ключа: autobind по самому длинному подходящему ключу
  (`resolve_profile_key`). Если ключа нет — **профиль не синтезируется**: seed остаётся с
  `profile_key: null`, краул пойдёт с дефолтными осями, и это видимое предупреждение
  `profile_unbound` (`core/pipeline/package_seeds.py:352`). Синтез осей (`wait_for`,
  `rate_limit`, `extract`) отклонён: они **измеряются** (A3), а не угадываются; ключ
  объявляет человек или `add-product`/`crawl-probe`, которые владеют этим путём (§2.3).
- Профиль, у которого `max_depth: 0` (host-root), не должен получать seed с глубиной > 0:
  `seeds validate` это проверяет (**planned**).

### 7.5. Приоритет источников seed'ов в `run`

Резолвинг — **по продукту**, а не «файл есть/нет»:

```
--urls (если задан — полностью заменяет набор для всех продуктов)
  > sources/seeds.yaml[product] (даже если seeds: [])
  > sources/corpus_urls.yaml[product] (legacy fallback для этого продукта)
  > [] → pending_seeds / blocked
```

- Если `--urls` задан, он **полностью** заменяет пакетный набор (как сейчас); в
  `run_inputs` фиксируется `seed_source = "operator"` и warning.
- Если `seeds.yaml` отсутствует **или в нём нет продукта**, для этого продукта
  используется `corpus_urls.yaml[product]` с warning `seeds_legacy_fallback`. Это
  миграционный путь для существующих пакетов (ru-pg-dbms): старые продукты не теряют
  seed'ы, когда `add-product` впервые создаёт `seeds.yaml` только с новым продуктом.
- **Миграция при первом `add-product`.** Перед добавлением нового продукта
  `ensure_seeds_file(pkg)` создаёт `sources/seeds.yaml`, импортируя **все** записи
  `corpus_urls.yaml` как `role: primary`, `source: migration`, `enabled: true`, и только
  затем добавляет новый продукт. Отдельная команда
  `hound package seeds migrate --from sources/corpus_urls.yaml --actor ...` — **planned**
  (§2.3); миграция выполняется автоматически внутри `add-product`.
- `run` не фетчит `doc_domains` паспортов молча (I19): паспорт — не seed.

### 7.6. `--url` как hint

- `--url` попадает в `resolved_url`/`url_candidates`, но `plan.seeds` формируется из
  **всех** `plan.sites`, а не только из него.
- Если `--url` указывает на хост, не покрытый discovery, он всё равно добавляется
  (после валидации) с `role: primary`.
- Если `--url` — единственный валидный seed (discovery ничего не нашёл), продукт
  `ready`, но `warnings: ["single_seed"]`; coverage будет `complete` только по этому
  seed'у, что честно отражено в отчёте.
- `--url` не отменяет SERP/scout: identity-разрешение всё равно выполняется.

### 7.7. Нет seed'ов

См. §2.2. Дополнительно:
- `seeds.yaml` содержит `state: pending_seeds`;
- `state.db.product_state.onboarding_state = pending_seeds`,
  `onboarding_reason = no_seeds`;
- `run` блокируется onboarding-префлайтом (O1);
- `corpus` (defense-in-depth) при пустом seed-наборе пишет coverage с
  `expected_sources=0, complete=False, reason=no_seeds` и `failed`-ячейки, если
  preflight был обойдён (unit-тест/прямой вызов).

---

## 8. Index architecture

> **Статус раздела: ✅ сделано (as-built по коду).** `pb_`/`cb_` разделены, композиции
> пишутся, GC reference-aware; единственное расхождение с §6.3 — источник `pb` при
> переиспользовании берётся с диска, а не из `snapshot.partition_build_id`.

### 8.1. Два идентификатора вместо одного

| ID | Формула | Смысл |
|---|---|---|
| `partition_build_id` (`pb_...`) | `H(_PARTITION_SCHEMA, package_id, product_id, chunker_version, embed_model_id, extractor_version, lexical_engine_version, vector_store, product_content_token)` | иммутабельная сборка **одной** продукт-партиции |
| `composition_id` (`cb_...`) | `H(_COMPOSITION_SCHEMA, package_id, sorted[(product_id, partition_build_id, content_digest)])` | композиция партиций когорты; это `runs.index_build_id` |

`product_content_token` — `_product_content_token`
(`core/pipeline/corpus.py`): sorted `(chunk_id, content_hash)`.

**`extractor_version` — не константа: в неё входит устройство (P9).** Строка имеет вид
`docling-doc-3:<level>/<device>/docling-<build>` (`core/cli/execution_axes.py:623-643`), где
`<device>` — объявленная ось `crawl.pdf_device` (`cpu|cuda`, `core/domain/enums.py:531`,
резолв — `resolve_pdf_device` в `core/pipeline/crawl_frontier.py`). Поэтому `pb` различает
`cpu` и `cuda` **явно**, а не через «повезло/не повезло с железом»; сама ось входит в
`run_id` через `extractor_stack` (§12.5), то есть `cpu↔cuda` — `execution_change`, а не
правка прогона. Значения `auto` нет намеренно: тихий CPU при недоступном устройстве назвал
бы GPU, на котором прогон не исполнялся (`cuda` без устройства — `EXIT_USAGE` с числами).

**`lexical_engine_version` теперь фактически заполняется.** Composition root
(`core/cli/wiring.py`) передаёт в `NpyIndexBuilder` версию реально используемого
лексического движка (`tantivy-<ver>`; пусто — лексического плеча в прогоне нет,
R0/R2 пропускаются). До исправления поле читалось у сборщика, у которого такого
атрибута не было, и всегда было пустым — `pb` не различал tantivy и in-process BM25
(дефект провенанса, исправлен).

**Что убирается из `partition_build_id`:** `params_token` и `snapshot_date`.
Каталог/фокус не меняют чанки/векторы; `snapshot_date` — провенанс, не содержимое.
Это позволяет переиспользовать партицию при смене фокуса/каталога. (Прежний
`_derive_index_build_id` включал и то, и другое; он **удалён** — заменён
`_derive_partition_build_id` + `derive_composition_id`, `core/pipeline/corpus.py`
и `core/pipeline/composition.py`.)

**Что входит в `composition_id`:** только состав и версии партиций. `retrieval_profile`,
`cutoff_K`, `retrieval_params_json` не входят (как и сейчас): ретривал-конфиг не меняет
чанкинг/векторы, но остаётся run-level провенансом и границей diff (§13).

### 8.2. Раскладка

```
index_store/{package}/
  {partition_build_id}/{product_id}/          # иммутабельная партиция (npy + manifest)
  _compositions/{composition_id}.json         # иммутабельный манифест композиции
```

`{partition_build_id}/{product_id}/` — как текущий `{build}/{product}`, но `build` теперь
per-product. `NpyIndexBuilder.build` получает `meta.extra["partition_build_id"]` и пишет
туда; `_guard_immutable` ключ `(package, partition_build_id, product_id)`.

Манифест композиции:

```json
{
  "schema": "composition/1",
  "composition_id": "cb_...",
  "package": "ru-pg-dbms",
  "created_at": "2026-10-19T00:00:00Z",
  "members": [
    {"product_id": "pangolin", "partition_build_id": "pb_...",
     "content_digest": "...", "chunks": 1234, "dim": 256},
    {"product_id": "acme-db", "partition_build_id": "pb_...",
     "content_digest": "...", "chunks": 87, "dim": 256}
  ]
}
```

Иммутабельность: если файл существует с другим содержимым — `ImmutableBuildError`;
тот же состав → тот же `composition_id` (idempotent).

### 8.3. Сборка при добавлении продукта

```
old composition: {A: pbA, B: pbB}
add C:
  build C -> pbC
  new composition = {A: pbA, B: pbB, C: pbC}
  A и B не пересобираются, эмбеддинги не пересчитываются
```

Если продукт `A` изменился:
```
build A -> pbA2
new composition = {A: pbA2, B: pbB, C: pbC}
pbA остаётся на диске (иммутабельно), но не входит в новую композицию
```

### 8.4. Retrieval

Новый `CompositionView` (`core/pipeline/composition.py`):

```python
@dataclass(frozen=True)
class CompositionView:
    composition_id: str
    members: Mapping[str, str]          # product_id -> partition_build_id

    def build_id_for(self, product_id: str) -> str: ...
    def product_ids(self) -> list[str]: ...
```

- `NpyVectorIndex` принимает `CompositionView` вместо одного `build_id`
  (`core/adapters/vector_npy.py`): в `search` для каждого `product_id` читает
  `self._view.build_id_for(product_id)`; `RetrievedChunk.build_id` = partition id;
  `NpyVectorIndex.build_id` = composition id (для провенанса/экспериментов).
- `seed_lexical_index` (`core/cli/wiring.py`) итерирует `view.members` и
  `store.iter_chunks(partition_id, product_id=product_id)`; `index.index(records,
  build_id=composition_id)`.
- `iter_index_manifest(composition_id)` возвращает строки с `partition_build_id`.
- I17 сохраняется: `search` фильтрует по `q.product_ids`; партиция продукта не содержит
  чужих чанков.

**Лексическое плечо: потоковый засев и гидртация из store (A1/Д-3).** Раньше засев держал
в памяти зеркало корпуса (список записей с текстом) — на реальном масштабе это линейный
рост RSS по числу чанков. Теперь:

- записи идут **генератором** из `store.iter_chunks(partition_build_id, product_id=…)`
  прямо в `index.index(records, …)` — ни список записей, ни их текст не живут в памяти
  (`core/cli/wiring.py:422`, докстринг `seed_lexical_index`);
- при поиске текст догружается **из store пачкой** — `StorePort.get_chunks(chunk_ids)`
  (`core/store/sqlite_store.py`, `SqliteStore.get_chunks`, шаг `_CHUNK_ID_BATCH = 500`),
  передаётся адаптеру как провайдер гидртации, поэтому адаптер не знает ни SQL, ни
  раскладки БД;
- два идентификатора остаются разными: `LexicalChunk.build_id` записи = **партиция**
  (`view.build_id_for(product_id)`), `index.build_id` = **композиция** (провенанс R0/R2);
- **`lexical_spec` — объявленный вход контракта** (`EXECUTION_CONTRACT_FIELDS`), и его
  состав менялся: флаги `stored` из спецификации убраны, `product_id` объявлен `stored`.
  Это `execution_change`, а не правка прогона: у прогонов до и после — **разный `run_id`**.
  Замер стоимости сборки: 0.25 с на 9398 чанков (`core/adapters/lexical_tantivy.py`,
  докстринг модуля) — индекс живёт во временном каталоге прогона и не персистится.

### 8.5. Эмбеддинги

- Партиция хранит векторы; переиспользование партиции = нет эмбеддингов.
- Для изменённого продукта: `embed_cache` с ключом
  `(content_hash текста чанка, embed_model_id)` (`embed_cache_key`,
  `core/pipeline/embed_cache.py:133`); при хите вектор берётся из кэша, эмбеддится только
  новый текст. **Кэш реализован и подключён** (`core/pipeline/embed_cache.py`;
  инстанцируется в composition root — `EmbedCache(str(paths.embed_cache))`,
  `core/cli/context.py:2120`, путь — `core/config/paths.py:155-156`; в фазе корпуса —
  параметр `cache` у `build_corpus`/`_embed_product`, `core/pipeline/corpus.py:234`).
  Прежняя редакция называла его «опционально, later» и «не входит в MVP» — это описание
  до реализации. Хиты **видны**: `embed_cached` и `embed_hit_ratio_pct` в прогрессе и
  `metrics.json` (`core/pipeline/corpus.py:2543-2581`) — «кэш включён» не доказывается
  фактом инициализации. Он по-прежнему **не** условие измерения: кэш — про стоимость, а не
  про провенанс; промах/хит не меняет ни `pb`, ни `run_id`. `embed_cache is None` —
  прежнее поведение байт-в-байт.
- `embed_model_id` обязан включать ревизию/снапшот модели; иначе смена весов при том же
  id даст ложное переиспользование. Это требование к composition root, не к `core/`.

### 8.6. Иммутабельность и GC

- Партиция и композиция — только создание; правка на месте запрещена (K2/§13).
- **Партиции**: `hound package builds-gc --package DIR [--runs-dir runs] [--protect ID ...]
  [--apply --actor]` (`gc_index_builds`, `core/store/backup.py`) удаляет сборки, не входящие
  ни в одну композицию пакета. Живое множество считается существующим `_live_compositions`
  по **всем** run DB каталога (`runs.index_build_id`, read-only); legacy-сборка (раскладка
  до композиций), названная прогоном, но не имеющая манифеста `_compositions/{id}.json`,
  добавляется в `protect` и выживает. Отказ (`ValueError`/`code=state`) при недоказуемости
  ссылок: нет `runs_dir`/ни одной run DB, DB не читается/без `runs`, ссылка пакета висит в
  воздухе, манифест не разбирается. По умолчанию dry-run; удаление — `--apply --actor`.
- **Композиции**: `hound package index-gc --package DIR [--runs-dir runs] [--apply --actor]`
  (`gc_index_compositions`). Живое множество — `runs.index_build_id` по **всем** run DB
  каталога (`runs.package_id` фильтрует; `NULL`-пакет консервативно защищает любой пакет;
  `NULL`-id — не ссылка). Отказ (`ValueError`/`code=state`) при недоказуемости: нет
  `runs_dir`/ни одной run DB, DB не читается, ссылка пакета висит в воздухе, манифест не
  разбирается. По умолчанию dry-run; удаление — `--apply --actor`.
- Операции **не сцеплены**: удаление композиции не трогает партиции (их соберёт отдельный
  `hound package builds-gc`), и наоборот. Реестр `state.db.index_composition` — durable-лог
  существовавшего (и `last_used_run_id`), **не** зеркало диска; GC его не чистит.
- **Рабочая последовательность** (владелец: все прогоны, кроме новейшего, расходуемы —
  дрейф Capability делает старые прогоны нерелевантными):
  1. удалить устаревшие каталоги `runs/{run_id}/` — run DB и есть **единственная** заявка
     на композицию;
  2. `hound package index-gc --package <DIR> [--runs-dir runs] --apply --actor <NAME>` —
     прунятся композиции, чья заявка исчезла вместе с прогоном;
  3. при желании — `hound package builds-gc --package <DIR> [--runs-dir runs]
     [--protect ID ...] --apply --actor <NAME>` для партиций, ставших недостижимыми после
     шага 2.
  Отказ при нуле run DB — именно то, что делает переход (1)→(2) безопасным: опорожнённый
  каталог прогонов не читается как «удалить все композиции». Ретенция прогонов — **вне**
  области этой функции: GC композиций не удаляет `runs/`, он лишь читает его.
  Отдельного флага `--keep-latest`/`--ignore-runs-before` нет: у артефакта один источник
  заявки (run DB), второй критерий создал бы конкурирующий авторитет.

### 8.7. Операторский потолок ревалидации (`crawl.revalidate_max_documents`)

**Ось, а не политика.** Потолок отсутствует по умолчанию: владелец исходит из правила
«не ограничивать, а измерять», поэтому это инструмент оператора для контроля стоимости
условных GET, а не дефолтное поведение. Без оси ревалидируются **все** известные URL,
`url_state` не получает `stale`-записей, новая причина усечения не появляется, а
`CrawlLimits.as_dict()` не добавляет ключ — `crawl_config_digest` и дайджест-граница
прогонов без оси не меняются.

**Per product, не per root.** Потолок задаётся один раз на продукт и применяется ко всем
его авторским корням и orphan-бакетам **вместе**; это не независимый лимит каждого корня.
Резолв — обычная цепочка `app < run < site` (`core/pipeline/crawl_frontier.py`,
`CrawlLimits.revalidate_max_documents`). Поскольку резолвер URL-скоуперирован, а у
продукта нет собственного URL, site-слой выбирается по первому авторскому корню; это
единственная точка, где «per product» требует представителя, и она не превращает потолок
в per-root.

**Отбор при срабатывании.** Сначала известные URL проходят **eligibility-фильтр** по
resolved-лимитам владельца (авторский root-key для root-группы, origin для orphan-бакета):
`is_blacklisted(url, limits)` и pre-fetch-пропуск документа при
`document_level is DocumentLevel.OFF` (тот же `document_suffix`, что использует сам
drain). Только исполнимые URL сортируются по `last_checked_at` (**NULLs first**), затем по
`normalized_url`; первые N ревалидируются, хвост откладывается. Такой oldest-first
ротирует автоматически: отложенные сохраняют старый `last_checked_at` и на следующем цикле
идут первыми. Стабильное «первые N» навсегда заморило бы хвост. Отложенные URL не
ставятся в очередь и помечаются `reuse_kind=stale` в `url_state` — это durable-сигнал
следующему циклу, а не тихий пропуск. Неисполнимые URL не откладываются и не получают
`stale`: они учитываются как `skipped`, поэтому не занимают слот потолка и не могут
заморозить ротацию. Инвариант учёта на продукт:
`selected + deferred + skipped == len(known)`; каждый известный URL попадает ровно в одну
из трёх категорий.

**Транспорт.** Отбор не ломает непрерывность ревалидационного блока в голове frontier
(author seed → revalidation → sitemap extras → BFS). Обе ветки транспорта (executor и
pool) впускают в волну этот блок **плюс** discovery-room, но не больше `wave_items`
(executor) / `max_workers` (pool): чисто ревалидационная волна при исчерпанном
`max_documents` не вырождается в волну размера `remaining` (executor при 64 — до 32
ревалидационных URL, pool — до половины `max_workers`). Предохранитель
`_EXECUTOR_MAX_WAVES_PER_ROOT` — BFS-guard и не расходуется волнами, в которых не было
ни одного discovery-элемента.

**Видимость.** У каждого корня, у которого были известные URL, в `truncation_record`
появляется аддитивный `revalidation: {"selected": N, "fetched": M}`; при срабатывании
потолка там же и у orphan-бакета появляется `"deferred": K`, а при наличии неисполнимых
URL — `"skipped": S`. `selected` — отобранные/enqueued URL, `fetched` — фактически
добытые (не 304/reuse), поэтому ранняя остановка не выдаёт `selected` за ревалидацию.
`pages_fetched` остаётся **discovery-only**; ревалидационный трафик виден отдельно в
`revalidation.fetched`, в провенансе сеанса (`revalidation_fetched`) и в
`truncation_summary`, а текстовая сводка печатает оба числа. Продукт помечается
`truncated=True` с `truncated_reason="revalidate_max_documents"` через существующий канал
усечения. Новой coverage/freshness-оси это не вводит: `coverage.complete` меняется только
по уже существующему правилу `truncated` (как у crawl-потолков), а `measured`/`pending`/
absence-правила не трогаются. Честность потолка выражена усечением и счётчиками, а не
подменой покрытия.

**Precedence.** В `truncation_summary` crawl-потолки (`max_documents`/`max_depth`/
`round_budget`) ранжируются **раньше** `revalidate_max_documents`: если корень усечён
crawl-потолком, сводка сообщает его причину, а потолок ревалидации остаётся вторичным.

**Split.** Профиль `split` не исполняет локальную политику отбора: если потолку есть что
ограничивать (есть incremental-items и их больше N), `corpus` пишет явную запись в
`errors` и ось игнорирует (ревалидируются все известные URL), а не делает вид, что потолок
применён. При пустом/достаточно малом work-set ошибки нет: ограничивать нечего.

**Остаточный разрыв `stale` (2.4.0).** В цикле срабатывания демотирование `❌` несёт
`truncated=True`: продукт неполон, поэтому `❌` не публикуется по существующему правилу.
Отложенный URL, который так и не ротировался (например, стал неисполнимым или никогда не
обновлял `last_checked_at`), сам по себе **не** является freshness-гейтом; отсутствие
отдельной проверки «`❌` не может опираться на stale-строку» — известный разрыв.
Потребление `reuse_kind=stale` в absence-решении относится к 2.4.0
(`measurement_origin`) и в этой версии не реализуется; словарь статусов не меняется.

---

## 9. Reuse matrix

> **Статус: ⬜ не сделано в части ячеек.** Строки до `Index partition` включительно — ✅
> as-built (проверено по коду). Строка «Cell/evidence» — ⬜ **design-only** (переноса ячеек
> нет, §10); строка «Evaluator verdict» описывает `eval_cache` **как он есть** — ✅,
> включая методологические поля в ключе (§10.5, `EVAL_CACHE_FORMAT = 2`).

| Артефакт | Ключ | Условие переиспользования | Инвалидация |
|---|---|---|---|
| HTTP-байты | `http:{url_hash}` (`cache/http`) | URL тот же; условный GET вернул 304 или контент тот же | `--refresh`, eviction, `--from-scratch` |
| ETag/Last-Modified | `url_state(product, normalized_url)` | тот же URL | 200 с новыми валидаторами; 404 |
| Retained raw | `corpus_raw/{product}/{sha256}` | `content_hash` тот же; файл не вытеснен | retention eviction |
| Extraction | `(sha256(raw), extractor_version, level)` (`extract_cache`) | точное совпадение | смена `extractor_version`/`level`. **`family` — часть конверта, не ключа** (§6.8): `None` = markdown-совместимое |
| DocumentVersion | `(product_id, content_hash, extractor_version, level)` | точное совпадение | новый контент/экстрактор/уровень |
| Chunks | `(doc_version_id, chunker_version)` | точное совпадение | смена `chunker_version`; новый doc version |
| Embeddings | `(content_hash, embed_model_id)` (кэш) или партиция | точное совпадение | смена `embed_model_id`/ревизии |
| Index partition | `partition_build_id` | content token + все версии совпали **и** физическая проверка диска/`manifest.json` (§8.5) | любой вход сборки; порча/неполнота каталога |
| Evaluator verdict | `eval_cache` key: `format`+`schema`+template+rendered prompt+model config+`max_retries`+правила (`EvalConditions`)+`catalog_row_digest`+`retrieval_profile`/`cutoff_K` (§10.5) | байт-в-байт по всему `echo` | смена промпта/модели/правил/каталога/условий ретривала |
| Cell/evidence — **design-only** | `cell_reuse_key` (см. §10, схема `cell-reuse-2`) | ключ совпал + coverage complete + **retrieved-набор этой ячейки** тот же | см. §10.3 |
| Coverage | не переиспользуется | всегда пересчитывается из текущих crawl-исходов | — |
| Golden/retrieval experiments | не переиспользуется | — | — |

### 9.1. Что **нельзя** переиспользовать без пере-измерения

> Список описывает **правило**; фактически переиспользования ячеек сегодня нет вообще
> (§10 — design-only), поэтому список — будущая граница, а не действующее ограничение.

- Ячейки при смене `methodology_version`, `scoring_policy_version`, `status_dict_version`,
  `capability_policy`, `unknown_policy`, `partial_score`.
- Ячейки при изменении строки каталога, влияющей на оценку: `rule`, `absence_rule`,
  `param_type`, `role_binding`, `concept_terms`, `search_langs`, `importance`.
- Ячейки при смене модели/слота/декодера/температуры/seed (I21).
- Ячейки при смене `retrieval_profile`, `cutoff_K`, `retrieval_params_json` (I20).
- Ячейки при изменении doc-set продукта (даже если изменился один документ — MVP:
  переоцениваем весь продукт; later — per-cell).
- `❌` при неполном покрытии (as-built: I2c через `coverage.complete`); `freshness=stale`
  без аттестации — **design-only** (поля `freshness` нет, §11.1).
- `scope_acceptance` при изменении doc-set/coverage (аттестация привязана к корпусу).
- Coverage `complete=True` — никогда не переносится; пересчитывается.

---

## 10. Evaluation / carry-forward (**design-only**) — ⬜ не сделано

> **Статус: design-only. Нет производителя в прогоне.** Ни `carry_forward_cells`, ни
> `cell_reuse_key`, ни `measurement_origin`, ни `carried_from_run_id` в коде нет
> (`grep -rn "carry_forward\|measurement_origin" core/` — пусто, кроме DDL-колонки
> `cell_reuse_key` в `cell_measurement`, у которой нет писателя). Счётчик `cells_carried`
> в снимках всегда 0 (измерено: 22 снимка в реальном `state.db` — все 0), ключа `carried`
> в отчёте нет. Вся секция — описание **будущего** механизма: правила ниже — контракт,
> который надо будет реализовать, а не описание текущего поведения. Пользуясь документом
> для runbook'а или SQL, эту секцию игнорировать.

### 10.1. Ключ переноса ячейки

```python
CELL_REUSE_SCHEMA = "cell-reuse-2"   # per-cell (решение владельца 29.10.2026); "1" был per-product

def cell_reuse_key(param, product_id, plan, prev: CellMeasurement,
                   composition: CompositionView, coverage: CoverageSnapshot,
                   attestation_digest: str, state: ProductStatePort,
                   prompt_template_digest: str, model_config_digest: str,
                   retrieved: Sequence[RetrievedChunk]) -> str:
    return short_hash("\x1f".join((
        CELL_REUSE_SCHEMA,
        product_id, param.param_id,
        catalog_row_digest(param),                 # canonical JSON строки каталога
        prompt_template_digest,                    # ТЕКУЩИЙ шаблон+rule, не prev.prompt_hash
        plan.model_id, plan.llm_scenario,          # ТЕКУЩАЯ маршрутизация (I21)
        model_config_digest,                       # decoder/temperature/seed/endpoint_kind
        plan.retrieval_profile, str(plan.cutoff_k), plan.retrieval_params_json,
        retrieved_doc_set_digest(retrieved, state),# ТЕКУЩИЙ набор ЭТОЙ ячейки, см. ниже
        coverage.digest(),                         # complete + blocked + unreadable + truncated
        plan.methodology_version, plan.status_dict_version, plan.scoring_policy_version,
        plan.capability_policy, plan.unknown_policy, str(plan.partial_score),
        attestation_digest,
        composition.build_id_for(product_id),      # partition id
    )), 24)
```

`prompt_template_digest` — отпечаток **текущего** шаблона/правила (`compiled.template_version`
+ текст rule), а не `prev.prompt_hash`: иначе смена шаблона не инвалидировала бы перенос.
`model_config_digest` — текущая конфигурация модели, а не `prev.model_id`.
`coverage.digest()` — текущий; предыдущее значение используется только как данные
`prev`-строки для сравнения.

**`retrieved_doc_set_digest` — не `doc_set_digest` продукта (решение 29.10.2026: per-cell).**
Ключ несёт отпечаток документов, из которых реально извлечены чанки **этой** ячейки
(`sorted(doc_version_id)` по `retrieved` + их `content_hash`), а не всех документов
продукта. Прежний дизайн (per-product) был консервативен: изменение любого документа
продукта инвалидировало все его ячейки, и цена этого консерватизма не измерялась.
Per-cell даёт точную инвалидацию: меняется документ, который не попал в top-K ячейки, —
ячейка переносится.

Следствие для порядка Ш6: **ключ считается после retrieval**, поэтому `carry_forward`
перестаёт быть батч-проходом перед оценкой (§10.2) и становится частью ячеечного цикла.
Retrieval при этом выполняется **всегда** — он и так выполнялся до вызова оценщика;
переносится только момент проверки ключа. Отдельного порога «сколько документов
изменилось» нет: порог был бы ещё одной осью для калибровки (A3 — мерить, а не
предугадывать).

Отпечаток обязан быть **порядко-независимым** (`sorted`) и не включать `score`/`rank`:
порядок выдачи может измениться от пересборки партиции при неизменном наборе документов,
а перенос в этом случае допустим (текст-то тот же). Если позже окажется, что порядок
влияет на вердикт, в ключ войдёт `prompt_hash`, а не ранг.

> **Важно:** `coverage.digest()` и `retrieved_doc_set_digest` обязаны быть **стабильными
> между прогонами** при неизменном наблюдении. В `coverage.digest()` **не входят**
> `computed_at`, `run_id`, `product_id`, `freshness` и счётчики `reused/revalidated/new`
> (они меняются от того, был ли ответ 304 или 200 с тем же телом, — это не семантическое
> изменение). Входят: `complete`, `expected`, `observed`, `blocked`, `unreadable_origins`,
> `partial_blocks`, `truncated`, `reason`. `freshness` проверяется отдельно в
> `carry_forward_cell` (`stale` ⇒ запрет переноса). Тест: «два прогона без изменений
> контента, один live и один 304 → одинаковый `cell_reuse_key`».

### 10.2. Перенос — **per cell, внутри ячеечного цикла**

Per-cell (решение владельца 29.10.2026) означает, что перенос — не батч-проход по продукту,
а решение по одной ячейке **после** её retrieval: до retrieval неизвестно, какие документы
в неё вошли, а значит ключ ещё не вычислим.

```python
def carry_forward_cell(param, *, product_id, coverage, plan, state, store, run_id,
                       prev_run_id, composition, retrieved,
                       prompt_template_digest, model_config_digest,
                       attestation_digest) -> Cell | None:
    """Переносит ячейку из prev_run_id, если её ключ совпал. None — надо мерить."""
    if coverage is None or not coverage.complete:
        return None
    if coverage.freshness == "stale":            # offline: нельзя публиковать ❌
        return None
    prev = state.get_cell_measurement(prev_run_id, product_id, param.param_id)
    # pending переносится как pending (очередь ревью), resolved — как carried.
    if prev is None or prev.state not in ("evaluated", "reviewed", "pending"):
        return None
    key = cell_reuse_key(param, product_id, plan, prev, composition, coverage,
                         attestation_digest, state, prompt_template_digest,
                         model_config_digest, retrieved)
    if key != prev.cell_reuse_key:
        return None
    evidence = list(store.iter_evidence(prev_run_id, product_id, param.param_id))
    if not evidence_chain_valid(evidence, product_id, composition, state, retrieved):
        return None
    cell = store.get_cell(prev_run_id, product_id, param.param_id)
    new_cell = cell.model_copy(update={
        "run_id": run_id,
        "measurement_origin": "carried",
        "carried_from_run_id": prev_run_id,
        "cell_reuse_key": key,
        "retrieved_doc_set_digest": retrieved_doc_set_digest(retrieved, state),
        "coverage_digest": coverage.digest(),
        "index_build_id": composition.composition_id,
        "partition_build_id": composition.build_id_for(product_id),
        "updated_at": now(),
    })
    store.save_cell(new_cell)
    for ev in evidence:
        store.add_evidence(ev.model_copy(update={
            "evidence_id": derive_evidence_id(run_id, ev),   # per-run, детерминированно
            "run_id": run_id,
            "origin_evidence_id": ev.evidence_id,
            "doc_version_id": ev.doc_version_id,
            "partition_build_id": composition.build_id_for(product_id),
        }))
    return new_cell
```

Точка вызова — ячеечный цикл Ш6, **между** retrieval и `evaluate_cell`:

```python
for param in plan.params:
    retrieved = retrieve_for_cell(param, product_id, ...)   # как сейчас, I16/I20
    carried = carry_forward_cell(param, retrieved=retrieved, ...)
    if carried is not None:
        cells_carried += 1
        continue                                            # LLM не вызывается
    evaluate_cell(param, retrieved=retrieved, ...)           # прежний путь
```

Порядок важен и не переставляется: `retrieve_for_cell` — это ровно тот вызов, который Ш6
делает и сегодня, поэтому per-cell не добавляет ни одного лишнего поиска; экономия — только
пропуск LLM.

`evidence_chain_valid` (теперь с `retrieved`):
- `ev.product_id == product_id` (I17);
- `ev.doc_version_id` входит в `document_version` продукта со `status=active`;
- `ev.content_hash == document_version.content_hash`;
- **`ev.doc_version_id` входит в `retrieved` текущей ячейки** — иначе переносится
  доказательство из документа, который в этом прогоне в top-K не попал: вердикт выглядел
  бы подтверждённым текстом, которого ячейка не видела;
- `ev.source_id` резолвится в URL (`store.resolve_source_url`);
- `ev.source_scope`/`scope_acceptance` не противоречат O7;
- `ev.index_build_id` — предыдущая композиция, но `partition_build_id` совпадает с текущей
  (партиция не менялась).

### 10.3. Когда обязательна переоценка

| Изменение | Переоценка |
|---|---|
| Новый продукт | все ячейки |
| Продукт `full` (corpus был неполным) | все ячейки |
| Изменён документ, **вошедший** в retrieved-набор ячейки | эта ячейка |
| Изменён документ, **не вошедший** в retrieved-набор | **нет** (per-cell, 29.10.2026) |
| Изменён `retrieved_doc_set_digest` из-за сдвига top-K | эта ячейка (набор её документов другой) |
| `coverage.complete = False` | все ячейки продукта (carry-forward запрещён) |
| Смена `methodology_version`/`scoring_policy_version`/`status_dict_version` | все ячейки всех продуктов |
| Смена модели/слота/декодера | все ячейки (eval cache тоже miss) |
| Смена каталога (rule/absence_rule/…) | затронутые параметры |
| Смена `retrieval_profile`/`cutoff_K`/`rrf_k` | все ячейки (I20) |
| Смена `scope_acceptance`/doc-set для `❌` | absence-ячейки |
| Только `canonical_name`/`selection_reason` | нет |

### 10.4. I17/I18/I19/I20/I21

- **I17:** carry-forward копирует evidence только с `product_id` продукта; doc-set —
  только документы продукта/сателлитов. Композиция не смешивает партиции.
- **I18:** carry-forward не делает сетевых вызовов; оценщик по-прежнему без веб-тула.
- **I19:** evidence несёт `doc_version_id` + `content_hash`; `doc_version` существует и
  входит в doc-set; raw→hash→index chain сохраняется (reused-документ прошёл этот chain
  в прошлом цикле и ревалидирован в текущем).
- **I20:** `cutoff_K` входит в `cell_reuse_key`; смена K → переоценка.
- **I21:** `llm_scenario` входит в ключ; маршрутизация не меняется при переносе.

### 10.5. Eval cache: ключ, который знает правила (**✅ сделано**)

Было: `EvalVerdictCache` (`core/pipeline/eval_cache.py`, `EVAL_CACHE_FORMAT = 1`) ключевал
по отрендеренному промпту + конфигурации модели. Промпт описывает **вопрос**, но не
**правила ответа**: смена scoring policy, словаря статусов, версии методологии или строки
каталога меняет вердикт, не меняя текста промпта. Кэш формата 1 мог вернуть вердикт,
посчитанный по старым правилам — это ложный провенанс, а не экономия.

**Сделано.** `EVAL_CACHE_FORMAT = 2`; ключ — sha канонического JSON **полного** набора
полей `echo` (`build_eval_key`, `core/pipeline/eval_cache.py`). Состав:

| Группа | Поля | Источник |
|---|---|---|
| Пространство ключей | `format`, `schema` | `EVAL_CACHE_FORMAT` / `EVAL_SCHEMA_VERSION` |
| Промпт | `template`, `prompt_sha256` (отрендеренный текст), `prompt_hash` (аудит §5) | компилятор промпта |
| Модель | `model_id`, `model_version`, `seed`, `temperature`, `decoder`, `endpoint_kind` | объявленный слот (I21) |
| Политика повтора | `max_retries` | аргумент прогона: повтор переиздаёт запрос, значит «первый валидный ответ» от неё зависит |
| Правила | `methodology_version`, `status_dict_version`, `capability_policy`, `unknown_policy`, `partial_score` | `EvalConditions` (`eval_conditions_from_run`) |
| Каталог | `catalog_row_digest` | вся строка каталога каноническим JSON |
| Ретривал | `retrieval_profile`, `cutoff_K` | провенанс ретривала (I16/I20), не пересчитывается |

Три решения, которые стоит прочитать, потому что они не «очевидны из списка»:

1. **Политика входит значениями, а не номером версии.** В ключе лежат
   `capability_policy`/`unknown_policy`/`partial_score` (из `scoring_policy_spec`,
   `core/domain/scoring.py`) — то есть то, с чем реально исполняется формула §6. Версия
   политики могла бы быть поднята без смены семантики или, хуже, семантика могла бы
   измениться без bump'а; значения этого не допускают.
2. **`catalog_row_digest` — дайджест всей строки, а не перечень «влияющих» полей.**
   Перечень пришлось бы поддерживать руками, и забытое поле тихо вернуло бы ложный хит.
3. **Пустое условие запрещено.** `EvalConditions.__post_init__` отвергает пустые строки и
   `partial_score` вне `[0,1]`; `status_dict_version = None` — честное «словарь не
   объявлен» (установка без `docs/`), отличимое от «объявлена вот такая». Поле, которое
   всегда пустое, — ложь о том, что оно учтено.

**Кэш без объявленных условий не включается.** Если `eval_cache` передан, а
`eval_conditions` — нет, `evaluate_cell` поднимает `ValueError`
(`core/pipeline/evaluate.py:1975`): молчаливый откат к «кэш без правил» вернул бы ровно ту
ошибку, которую правка закрывает.

**Формат 1 читается как промах**, и это **желаемое** поведение: конверт лежит под другим
ключом и несёт другой набор полей `echo`, сверка `echo` при чтении — точное равенство
словаря (`eval_cache.py`, `get`). Кэш не должен переживать смену правил.

**Что остаётся вне ключа осознанно:** `run_id`/`build_id` (одна сборка — много прогонов),
`product_id` (входит в текст промпта), `strict`/coverage/resolver/прецедент
(пере-исполняются на хите и потому не меняют вердикт).

---

## 11. Coverage / честность

> **Статус раздела: ⬜ не сделано в части `freshness`.** Реальные колонки покрытия — as-built; всё,
> что опирается на `freshness` (§11.2, §11.4), — **design-only**: такой колонки нет, и
> построенные на ней правила не исполняются.

### 11.1. Поля `corpus_coverage` — реализовано (реальный состав)

Фактическая таблица (`core/store/sqlite.py`, `SCHEMA_VERSION = "3.13.0"`; миграция старой БД
запрещена гейтом версии — файл другой версии отвергается, `11` §2.6):

```sql
-- как есть, без выдуманных ALTER'ов:
CREATE TABLE corpus_coverage (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    complete          INTEGER NOT NULL,
    expected_sources  INTEGER NOT NULL DEFAULT 0,
    observed_sources  INTEGER NOT NULL DEFAULT 0,
    blocked_json      TEXT NOT NULL DEFAULT '{}',           -- {error_kind: count}
    unreadable_origins_json TEXT NOT NULL DEFAULT '[]',     -- ["origin", ...]
    partial_blocks_json     TEXT NOT NULL DEFAULT '{}',     -- {origin: {error_kind: count}}
    truncated         INTEGER NOT NULL DEFAULT 0,
    computed_at       TEXT NOT NULL,
    reason            TEXT,                                  -- no_seeds|all_seeds_rejected|...|partial_extraction
    resolved_manually INTEGER NOT NULL DEFAULT 0,             -- блокирующий отказ закрыт объявленным ручным артефактом
    PRIMARY KEY (run_id, product_id)
);
```

> **`resolved_manually` — не «мягкая» полнота.** Блокирующий отказ, закрытый **объявленным**
> ручным артефактом (13-manual-artifacts §3.3), перестаёт понижать `complete`, но **остаётся**
> в `blocked_json` как аудит: поле меняет последствие, а не факт отказа. Поэтому читать
> покрытие «по `blocked_json` пусто» нельзя — правильный вопрос «`complete` и почему».
> Прежняя редакция SQL-блока выдавала его за «как есть» без этой колонки.

> **Чего нет (и не надо писать против этого SQL).** Колонок `mode`, `freshness`,
> `reused_sources`, `revalidated_sources`, `new_sources`, `removed_sources`,
> `doc_set_digest` **не существует**. Из восьми «новых ALTER'ов» прежней редакции этого
> документа в коде есть ровно один — `reason`; остальные семь никогда не добавлялись.
> Счётчики reused/new/removed и `freshness` — **design-only** (и `CoverageFreshness`-enum
> тоже нет, §3.1).

Реальные поля и их смысл:

- `expected_sources` — всего элементов сеанса по продукту;
- `observed_sources` — элементов, дошедших до `FETCHED` (включая 304/same-hash);
- `blocked` — `error_kind → count` по `FAILED`-элементам (канонические классы);
- `unreadable_origins` — origin'ы, от которых не получено ничего (сортировано);
- `partial_blocks` — `origin → {error_kind: count}` не-`not_found` сбои (аудит);
- `truncated` — продукт попал в `truncated_products` сеанса;
- `complete` — `False`, если краул усечён политикой, хотя бы один origin нечитаем, есть
  `FAILED` с `error_kind` из `BLOCKING_FAILURE_KINDS`, **либо** `observed_sources == 0`
  при `expected_sources > 0` (корпус без единого наблюдения не доказывает отсутствия).
  Продукт, у которого все seed'ы вернули 404/410, — `complete=False`: отсутствие нельзя
  доказывать по исчезнувшему сайту без человеческой аттестации. Это усиливает определение
  §4.4, а не ослабляет его. `reason` при этом остаётся `null` (причина видна по счётчикам
  и `blocked`), поэтому ячейки получают `failure_kind=coverage_missing`, а не отдельный
  класс: введение нового значения `failure_kind` — самостоятельная методологическая
  граница (словарь статусов), в 2.3.0 её нет.

**Ключевое различие (design-часть):** «reused» в coverage означало бы
**переиспользование контента**, а не «не наблюдали» — все reused-URL были бы реально
опрошены (304). Сегодня это различие невыразимо: счётчиков reused/revalidated/new в
таблице нет. `complete=True` при этом остаётся честным по своим правилам (I2c), и
семантика §4.4 не меняется.

### 11.2. `❌` и неполный корпус (as-built; `freshness` и правило reuse — design-only) — ⬜ в части `freshness`/`reuse`

> **Правило сменилось и стало классовым.** Прежняя редакция этого документа описывала
> «`❌` только при `complete` **или** подписи» — то есть делала **всякое** «нет» зависимым от
> полноты краула. Код (`9ec346e`) считает иначе: условие зависит от **класса отрицания**
> (`denial_strength`), а не от одной полноты. Ниже — as-built; правило объявлено в
> `check_absence_coverage` (`core/domain/invariants.py:286`), исполняется в конвейере через
> `_enforce_absence_coverage` (`core/pipeline/evaluate.py:1390`), и зеркало есть в отчёте.

`❌` публикуется, если `coverage.complete == True` (и `freshness != stale` — design-only,
поля нет, §11.1). При **неполном** покрытии (`complete == False`) решает класс:

| `denial_strength` | Неполнота охвата | Почему |
|---|---|---|
| `explicit` | **не отменяет** `❌` | дословная цитата вендора — свидетельство, а не вывод из полноты корпуса |
| `authoritative` | **не отменяет** `❌` | авторитетная энумерация по построению |
| `bounded` | требует `scope_acceptance` | «нет» зависит от пространства; без принятия риска публиковать нельзя |
| `unobserved` / `not_stated` | `❌` **не дают вообще** | I2: это не «нет», это «не смотрели»/«не сказано» |

`coverage is None` (записи покрытия нет — legacy/без краула) — проверка **неприменима**,
вердикт не меняется.

**Нарушение даёт `❓`, а не `pending` и не `failed`.** Ячейка **остаётся в карте** как
`❓` (`not_stated`, I12) с машинной причиной `absence_coverage_unsatisfied` в
`evidence_scope.reason` **и** в `cells.validation_notes`, называющей недостающий компонент и
виновника (`{product_id}/{param_id}`). Протокол отсутствия при этом **не публикуется**
(`absence_evidence=[]`): «искали и не нашли» — утверждение, которое здесь не доказано.
`failed` не годится — отсутствие принятия риска процедурное, а не отказ прибора (I4), и
ячейка обязана остаться в карте. Прежняя редакция говорила «понижается до `pending`» с
причиной `corpus_incomplete_absence` — это неверно: `CORPUS_INCOMPLETE_REVIEW_REASON` сегодня
служит **префиксом причины** у `❓` при классе `unobserved`, а не механизмом понижения
`❌`→`pending`.

**Подпись — per product.** `scope_acceptance` читается из
`packages/{case}/scope_attestation.yaml:attestations` (§11.5) и передаётся в Ш6; продукт без
подписи ведёт себя ровно как выше. В `EXECUTION_CONTRACT_FIELDS` подпись не входит
(авторизация, не измерение).

`freshness=stale` (режим `offline`) — **design-only**: поля нет, режим `offline` сегодня
деградирует до `revalidate` с видимой ошибкой на продукт (§2.4), поэтому публиковать
«`❌` всегда понижается при stale» нечего — правило не исполняется.

> **Аудит 29.10.2026: вопрос про `❌` на reused-корпусе — про проводку, а не про семантику.**
> Различение «наблюдён в этом цикле» уже существует, но живёт **только** в durable-состоянии:
> `url_state.reuse_kind ∈ {live, revalidated, reused, stale}` + `last_http_status` +
> `last_checked_at` (`core/store/state_sqlite.py:93`), пишется из `corpus.py:4491-4527`
> (при 304 — `revalidated`/`304`, `last_changed_at=NULL`). Условный GET реально уходит:
> `_validators` (`corpus.py:4274`) отдаёт `If-None-Match`/`If-Modified-Since` из `url_state`,
> 304 переиспользует версию документа **до** экстракции (`_reuse_for_page:4386` →
> `_reuse_not_modified:4356`), и в `state.db` живого прогона на снимке 29.10 09:20 таких
> строк **467** из 5979.
>
> В **прогонную** БД это не доезжает: у `corpus_docs` (`core/store/sqlite.py:103`) нет ни
> `http_status`, ни флага reuse, и при 304 строка перезаписывается `INSERT OR REPLACE` с
> `fetched_at=now` — то есть 304-документ в отчёте выглядит как живой 200. Плюс отдельный
> случай, где сетевого подтверждения не было вовсе: resume берёт retained-байты
> (`_resume_page`, `corpus.py:3110`: «`not_modified` всегда `False`»), и такие элементы сеанса
> имеют `http_status=NULL` (на том же снимке — **502** из 2125 `fetched`).
>
> Отсюда честная постановка: `❌` на документе, пришедшем из retained-байтов **без**
> серверного подтверждения, публиковать нельзя — это «не смотрели», а не «нет». Для
> ревалидированного (304) — можно: сервер ответил в этом цикле. Разница различима уже
> сегодня, но **не в прогоне**; чтобы правило исполнялось, признак должен доехать до
> `corpus_docs`/покрытия — это версионная граница `runs` (§14.1), не текстовое уточнение.

**Решение владельца 29.10.2026: `❌` требует `live|revalidated`.** Правило исполняется на
двух уровнях, потому что у двух групп классов разный носитель утверждения:

1. **Уровень evidence** (`explicit`, `authoritative`). «Нет» здесь держится на конкретном
   документе (цитата/энумерация), поэтому проверяется **каждый** документ, на который
   опирается `❌`: в этом цикле он получил серверный ответ (`live` или `revalidated`).
   Хоть один `reused`/`stale` — `❌` не публикуется.
2. **Уровень корпуса** (`bounded`, `parameter_reference`, `distribution_absence`,
   `registry_absence`). «Нет» выводится из корпуса, поэтому `reused`/`stale`-документ в
   корпусе продукта снимает `❌` так же, как неполное покрытие снимает его у `bounded`:
   отсутствие не доказано, если часть корпуса в этом цикле не наблюдалась.

**Что именно записывается в `corpus_docs.reuse_kind` — сильнейшее наблюдение прогона, и
понижать его нельзя.** Признак пишется из пути страницы **этого** прогона: живой fetch —
`live`/`revalidated`, чтение retained-байтов (`_resume_page`) — `reused`. Но resume
перечитывает и те документы, которые в этом же `run_id` уже были получены от сервера (до
перезапуска хоста), поэтому запись **монотонна**: строка этого прогона, уже несущая
`live`/`revalidated`, не перезаписывается на `reused`. Иначе resume понижал бы наблюдение,
которое состоялось.
`reuse_kind IS NULL` в `corpus_docs` — «наблюдение не записано» (legacy-строки): трактуется
как неподтверждённое, то есть `❌` не публикуется. Писатель границы A всегда пишет значение,
так что `NULL` остаётся только у строк, созданных до неё.

**Замер 29.10.2026 — правило сегодня не срабатывает ни на одном документе, и это надо
сказать прямо.** По `crawl_session_items` видно **629 из 629** `fetched` у `pangolin` и
10 из 23 у `jatoba` с `http_status IS NULL` — их перечитали из retained-байтов при resume
после перезагрузки хоста. Но это **не** «не смотрели»: `corpus_docs.fetched_at` у всех
509 документов `pangolin` и у 1143 из 1143 `digital_q_database` лежит **внутри** этого
`run_id` (старт 28.10 21:20 UTC), а `url_state.reuse_kind` не содержит ни одной строки
`reused`/`stale` (2157 `live` + 2009 `revalidated` + 2620 без классификации — это
известные, но не фетченные URL). То есть сервер отвечал в этом цикле, resume лишь повторно
разобрал байты. Правило — страж для документов, впервые наблюдённых в **прошлом** цикле
(`state adopt`, `--update`) и для `offline`, а не механизм, который сегодня снимает `❌`.
`revalidated` (304) правило **удовлетворяет**: сервер ответил, версия документа не изменилась.

**Нарушение даёт тот же исход, что `absence_coverage_unsatisfied`:** ячейка остаётся `❓`
(`not_stated`, I12) с машинной причиной `reuse_unconfirmed` в `evidence_scope.reason` **и**
в `cells.validation_notes`; `absence_evidence=[]`. `failed` не годится по I4.

**Класс изменения — `methodology_version`, не `execution_change`.** Меняется условие
публикации `❌`, то есть absence/evidence-модель — по `AGENTS.md` §13 это
`methodology_change` с обязательными `golden-full` **и** пере-аннотацией golden. Это не
бюрократия: правило снимает часть `❌` в `❓`, то есть меняет **результат** измерения, а
`content_change` его не описывает.
**Следствие для порядка работ:** правило нельзя везти отдельным коммитом «после границы A» —
это была бы вторая версионная граница и второй `golden-full`. Оно входит **в ту же границу**,
что `corpus_docs.reuse_kind` (граница A), и поднимает класс всей границы до
`methodology_version`; один прогон golden-full на обе правки. Реализация — проверка в
`evaluate` рядом с `_enforce_absence_coverage` (`core/pipeline/evaluate.py:1390`), поле
`corpus_docs.reuse_kind` — из границы A (§14.1, §16).

### 11.3. Ноль seed'ов / ноль источников (as-built)

- onboarding-префлайт блокирует продукт с `pending_seeds` (O1).
- Если preflight обойдён: `corpus` (`_persist_corpus_coverage`) пишет
  `expected_sources=0, complete=False, reason=no_seeds` (или `all_seeds_rejected`/
  `no_urls_entry` — смотря какой путь привёл к пустоте), `evaluate` — `failed`-ячейки с
  `failure_kind=no_sources`, **не** `❌`.
- В отчёте — аномалия `corpus_coverage_anomalies` с продуктом и причиной.

### 11.4. `reused` vs `observed` в отчёте (**design-only**) — ⬜ не сделано

> **Статус: design-only.** Счётчиков `revalidated`/`new`/`removed`/`freshness` в покрытии
> нет (§11.1), ключа `carried` в отчёте нет (§10). Пример ниже — целевой формат.
> Источник данных при этом есть: `url_state.reuse_kind` (§11.2) и
> `product_snapshot.docs_new`/`docs_reused` (`core/pipeline/state_sync.py:189`), которые
> считаются из `document_version.first_seen_run_id`/`last_seen_run_id`; в отчёт они не
> попадают.

`battle_card`/`run_manifest`/JSON:
```json
{
  "product_id": "pangolin",
  "mode": "incremental",
  "coverage": {"complete": true, "expected": 187, "observed": 187,
               "revalidated": 180, "new": 7, "removed": 0, "freshness": "revalidated"},
  "cells": {"total": 42, "carried": 38, "measured": 4}
}
```
Человек видел бы, что 38 ячеек не пересчитывались, а 4 — пересчитаны; это не «тихая»
экономия. Сегодня такого вывода нет: отчёт показывает `doc_delta.reused` (переиспользованные
**документы**), а не ячейки.

### 11.5. `scope_acceptance` (as-built; `attestation` — design-only)

> **Имя сменилось.** Прежнее `scope_attested_by`/`scope_attestations` заменено на
> `scope_acceptance` и в домене (`core/domain/models.py:835`, `:960`), и в манифесте
> (`RunManifest.scope_acceptance`, `core/domain/models.py:1591`), и в run DB
> (`cell_evidence.scope_acceptance`, `core/store/sqlite.py:225`; миграция старой колонки —
> `:515-531`). Причина в коде названа прямо: подпись — **принятие риска**
> ненаблюдённой части, а не аттестация полноты. Старые манифесты читаются:
> `_migrate_legacy_keys` перекладывает исторический ключ. **SQL против
> `scope_attested_by` вернёт пусто** — это не «нет подписей», а неверное имя.

- **As-built:** аттестация охвата — `packages/{case}/scope_attestation.yaml`: `procedure`
  (процедура I2b, `hound package scope set|clear`) и `attestations` — **подписи per product**
  (`product_id`, `attested_by`, `attested_at`, `snapshot_date`, `notes`), ставится
  `hound package scope attest --package … --product … --actor …`
  (`core/cli/package/parser.py:430-457`). Плюс per-instance
  `cell_evidence.scope_acceptance` в run DB. Именно эти носители читает проверка I2b/I2c.
- **Канал проложен:** прогон читает подписи пакета (`load_scope_acceptance`,
  `core/pipeline/normalize.py:145`) и передаёт их в Ш6 как `scope_acceptance`
  **per product** (покрытие per product ⇒ подпись per product; проводка —
  `core/cli/context.py:1999`). Продукт без подписи ведёт себя ровно как раньше: `❌` при
  неполном корпусе не публикуется (ячейка остаётся `pending`-кандидатом с `review_reason`).
- **Почему не `gate_decisions`:** гейт per-run, а профиль/аттестация квази-константны
  (недельные прогоны; смена когорты/ролевой матрицы — 2–5 раз в год). Переписывать подпись
  каждый цикл значило бы создать трение, из-за которого её перестанут ставить. Актуальность
  подписи ограничена `snapshot_date`, а не номером прогона.
- **Провенанс:** использованные подписи попадают в манифест (`scope_acceptance`,
  `product_id → attested_by`), поэтому опубликованный `❌` прослеживается к человеку.
  В `EXECUTION_CONTRACT_FIELDS` подпись **не входит** — авторизация, не измерение
  (тот же класс, что `reviewed_by`): смена подписанта не даёт новый `run_id`.
- **Design-only:** `state.db.attestation` с `doc_set_digest`/`coverage_digest` — таблица
  есть, **писателя нет** (§4.2), 0 строк в реальных БД. «Переносится только если digest'ы
  совпали; иначе `superseded_at`» — намерение.
- **Остаточный дефект имени исправлен 29.10.2026.** Deprecated-алиас `scope_attested_by`
  удалён из `evaluate_cell`/`evaluate_phase` (`core/pipeline/evaluate.py`), вызов в
  `phases.py:1180` переименован в `scope_acceptance`. Алиас не оставлен намеренно: два
  имени одного поля разводили смысл («аттестация» против «принятие риска»), а обратная
  совместимость не требуется. Тест фиксирует отказ по старому имени
  (`tests/pipeline/test_evaluate.py::test_scope_acceptance_is_the_only_name`).
- Новый продукт не получает аттестацию автоматически (I6/I7).

### 11.6. `pending`/`N/A`

- Новый продукт: `N/A` — только человеком (`reviewed_by`), не автоматически.
- `pending` из-за неполного покрытия — с `review_reason=CORPUS_INCOMPLETE_REVIEW_REASON`
  (как сейчас); в отчёте — аномалия.
- `failed` — `status=null` (I4), входит в знаменатель Coverage (I5).

---

## 12. Versioning §13

> **Статус раздела: ✅ сделано в §12.1/§12.2/§12.5, ✅ частично §12.3, ⬜ открыто §12.6.**
> Классы и `execution_contract_digest` работают в прогоне; lineage живёт в
> `state.db.run_lineage`, в манифесте его нет; решения §12.6 реализованы частично.

### 12.1. Классы изменений

| Класс | Что меняется | Diff | Golden |
|---|---|---|---|
| `none` | ничего значимого | разрешён | — |
| `content_change` | только состав/контент документов (новая композиция, тот же execution contract) | разрешён с баннером `content_change` | — |
| `cohort_expansion` | добавлен продукт в когорту | **запрещён** (`execution_change`) | **`golden-full`** |
| `cohort_reduction` | продукт удалён/retired | **запрещён** (`execution_change`) | **`golden-full`** |
| `execution_change` | версии/модели/ретривал/краул/seed-набор | **запрещён** | **`golden-full`** (консервативно, см. ниже) |
| `methodology_change` | правила/scoring/absence/coverage/Comparability | **запрещён** | `golden-full` + пере-аннотация golden |

> **`execution_change` требует `golden-full` всегда, а не «при смене модели/каталога».**
> `golden_required_for` (`core/pipeline/run_identity.py:363`) возвращает ровно два значения —
> `full` или `none`; `fast` он не возвращает **никогда**. Причина названа в докстринге: по
> одному `execution_contract_digest` нельзя отличить смену модели/каталога (`full`) от смены
> seed'ов/ретривала (`fast`), поэтому берётся более строгое требование. Прежняя редакция
> таблицы обещала «иначе `golden-fast`» — такой ветки в коде нет.
> Тир `fast` при этом существует **как данные**, а не как требование: `GoldenTier.FAST/FULL`
> (`core/domain/enums.py:456`), `hound package golden` с `choices=["fast","full"]`, и линт
> пакета требует наличия **обоих** тиров (`core/pipeline/lint.py:239-241`). То есть
> `golden.csv` содержит 905 ячеек = 750 `full` + 155 `fast`; какой тир **прогонять** —
> решает требование класса, а не наличие строк.

### 12.2. `cohort_digest` и `seeds_digest`

- `cohort_digest` входит в `run_id`, `execution_contract_digest` и в `state.db.run_lineage`
  (в `runs` и в `RunManifest` его **нет**, см. §12.3).
- `seeds_digest` — тоже: смена seed-набора меняет корпус и coverage. Он **порядко-чувствителен**
  внутри продукта (§6.2): перестановка корней — другая идентичность, потому что другой
  корпус.
- `selection_reason`/`canonical_name` — **не** входят (авторский текст/отображение).

> **Дефект провенанса (проверено 29.10.2026): колонка `runs.cohort_ref` хранит другое поле.**
> В `RunConfig` два разных поля: `cohort: str` (`core/domain/models.py:632`) — имя файла
> когорты из `run.yaml`, и `cohort_ref: str | None` (`:646`) — авторская ревизия, которая
> **входит в `cohort_digest`**. `sqlite_store.py:440` кладёт в колонку `cohort_ref` значение
> `cfg.cohort`, а `cohort_digest` считает по `cfg.cohort_ref` (`core/cli/commands/run.py:494`).
> Следствие на живом прогоне `ru-pg-dbms-3d3dc7c3c969`: `runs.cohort_ref =
> 'cohort-ru-pg-dbms-2026-10-28-golden00'` (имя файла), а в дайджест ушло `None` — то есть
> манифест показывает «ревизию», которая в `run_id` **не входила**. Правка меняет состав
> колонки ⇒ версионная граница `runs` (см. §14.1), поэтому делается вместе с `run_product`,
> а не отдельным патчем.

### 12.3. Lineage — ✅ реализовано частично (носитель — только `state.db`)

> **Статус: реализовано частично.** Ни в `runs` (`core/store/sqlite.py`), ни в
> `RunManifest` (`core/domain/models.py`) нет полей `cycle_id`, `previous_run_id`,
> `cohort_digest`, `seeds_digest`, `change_class`, `golden_required`,
> `state_schema_version`. Единственное lineage-поле в run DB — `execution_contract_digest`
> (аддитивная колонка `runs`) плюс `gate_policies`/`gate_waivers` в манифесте.
> Полная цепочка живёт **в `state.db.run_lineage`**.

Фактический носитель — `state.db.run_lineage` (`RunLineageRow`, `core/ports/state.py`):
`run_id`, `package_id`, `cycle_id`, `previous_run_id`, `cohort_digest`, `seeds_digest`,
`execution_contract_digest`, `change_class`, `status` (`running|complete|partial|failed|aborted`),
`started_at`, `completed_at`, `error`, `products_json` (состав когорты),
`refresh_policy`.

`change_class` и `golden_required` **вычисляются в памяти**
(`build_run_identity`/`golden_required_for`, `core/pipeline/run_identity.py`): в lineage
пишется `change_class`, `golden_required` — нет (он выводится из класса заново).

`state.db.run_lineage` — граф: `previous_run_id` → текущий. `previous_run_id` выбирается:
1. `--since-run`, если задан (прогон обязан быть в `state.db`, иначе `EXIT_USAGE`);
2. иначе — **последняя** строка `run_lineage` пакета
   (`ORDER BY started_at DESC, run_id DESC LIMIT 1`), **без фильтра статуса**:
   `failed`/`aborted` тоже становятся predecessor'ом;
3. для resume текущего `run_id` — `None` (перенос ячеек не выполняется).

### 12.4. Когда bump'ить

- **`methodology_version`** — только смена семантики правил: словарь статусов, scoring,
  absence/сателлитная/evidence-модель, определения Coverage/Comparability/sensitivity.
  Требует `golden-full` + пере-аннотацию golden.
- **`package_version`/`catalog_version`** — авторские изменения пакета; `execution_change`.
- **`cohort_ref`** — новое значение при изменении состава когорты (например,
  `cohort-ru-pg-dbms-2026-10-19-r2`); `execution_change` + `golden-full`.
- **`cycle_id`** — каждый update-цикл; не версия, а идентификатор.
- **Семантика группировки/листьев ревалидации и потолок `revalidate_max_documents`** —
  `execution_change`, а не `methodology_change`: без bump'а `methodology_version`, без
  изменения `EXECUTION_FIELDS` и без bump'а схем partition/composition id.
  `execution_contract_digest` пока не видит
  семантику группировки/листьев, а для потолка — только его resolved-значение
  (`crawl_config_digest` хеширует resolved limits), поэтому границу следует называть
  свежим `cycle_id`, не изобретая новый механизм.
- **`index_build_id`/`composition_id`** — вычисляемый; не «bump», а следствие.

### 12.5. Межцикловый diff

`diff_boundary_violations` (`report.py`) сравнивал `EXECUTION_FIELDS`, включая
`index_build_id`. Любое изменение контента (а значит, и каждый недельный update) блокировало
diff — инкрементальность была полезна только для стоимости, не для сравнения.

**Реализовано (методология 2.3.0).**

- `execution_contract_digest` — отпечаток **условий измерения**, без контента. Ровно
  **28 полей** (`EXECUTION_CONTRACT_FIELDS`, `core/pipeline/digests.py`) — состав проверяется
  механически (`len(EXECUTION_CONTRACT_FIELDS)`), а не по памяти:

  | Группа | Поля |
  |---|---|
  | Версии правил | `methodology_version`, `scoring_policy_version` ⚠️, `status_dict_version` |
  | Версии пакета/когорты | `catalog_version` ⚠️, `package_version`, `cohort_digest`, `seeds_digest`, `passports_digest` |
  | LLM | `llm_scenarios`, `prompt_template`, `evaluation_policy` |
  | Ретривал | `retrieval_profile`, `cutoff_K`, `retrieval_params_json`, `chunking_spec`, `lexical_spec` |
  | Извлечение | `chunker_version`, `extractor_stack` |
  | Краул | `crawl_strategy`, `crawl_rate_limit`, `crawl_config_digest`, `render_modes_digest` |
  | SERP | `serp_rounds`, `serp_mode` |
  | Реестры и входы | `registry_spec_digest`, `manual_artifacts_digest` |
  | Развёртывание | `deploy_profile`, `edge_schema_version` |

  > ⚠️ **Два поля присутствуют в контракте, но не имеют источника — и потому не
  > инвалидируют ничего.** `build_execution_contract`
  > (`core/pipeline/run_identity.py:237`) принимает `scoring_policy_version` и
  > `catalog_version` аргументами, и **ни один вызывающий их не передаёт**
  > (`core/cli/commands/run.py:554,596`, `core/cli/commands/plan.py:261`,
  > `core/cli/commands/crawl.py:327` — только `status_dict_version`).
  > Следствие: в контракте оба поля — `None`, а в манифесте — `"0.0.0"`
  > (`core/pipeline/report.py:789-795`, `core/store/sqlite_store.py:2326,2328`). Значит смена политики расчёта
  > (например `partial_score` 0.5 → 0.25) или каталога **не меняет `run_id`**, хотя
  > §9.1 требует переоценки всех ячеек: два прогона с разными правилами сравнились бы
  > как один. Это дефект провенанса, а не «поле на будущее».
  >
  > Честные источники существуют, оба — внутри той же чистой функции: `catalog_version`
  > ← дайджест строк каталога (каталог уже едет в `plan_inputs.payload.params`;
  > `package.yaml` объявляет `package_version`, но не `catalog_version`), а
  > `scoring_policy_version` ← дайджест объявленной политики §6 (`scoring_policy_spec`,
  > `core/domain/scoring.py:81`) — тогда «версия» перестаёт быть ярлыком, который можно
  > забыть поднять, и становится функцией декларации, как `cohort_digest`/`seeds_digest`.
  > **Правка не сделана в этой полосе сознательно:** она меняет материал
  > `execution_contract_digest`, то есть даёт новый `run_id` и обнулила бы прогресс
  > идущего прогона (`ru-pg-dbms-3d3dc7c3c969`). Порядок: сначала прогон, потом граница
  > идентичности — и явно, а не побочно.
  > Класс изменения при этом останется `execution_change` (а не `methodology_change`):
  > `classify_change` (`run_identity.py:324`) различает только когорту и «контракт
  > отличается», поэтому смена методологических полей контракта читается как
  > `execution_change`; требуемый golden от этого не слабее — `golden_required_for`
  > для `execution_change` возвращает `full`.

  > **Наблюдаемые версии в контракт НЕ входят** (намеренно): `extractor_version`,
  > `embed_model_id`, `lexical_engine_version`, `vector_store`. Они не выводятся из
  > объявленных входов (зависят от wheel'ов/диска/фактически извлечённого), остаются
  > провенансом манифеста и `partition_build_id`, и их смена видна через `pb`/
  > `index_build_id` — то есть попадает в разрешённый класс `content_change`, а не
  > форкает `run_id`. Вместо `extractor_version` объявлен **`extractor_stack`**
  > (константы стека извлечения + версия библиотеки docling + **устройство**:
  > `docling-doc-3:<level>/<device>/docling-<build>`; ось `crawl.pdf_device` — объявленный
  > вход, поэтому `cpu`↔`cuda` — `execution_change`, §13). Прежняя редакция этого
  > документа перечисляла здесь четыре наблюдаемые версии — это было неверно; ещё более
  > ранняя называла «19 полей» — тоже неверно (девять полей не были названы:
  > `serp_rounds`, `serp_mode`, `manual_artifacts_digest`, `prompt_template`,
  > `chunking_spec`, `evaluation_policy`, `lexical_spec`, `render_modes_digest`,
  > `passports_digest`).
  > Секция `run.yaml:gates` тоже **не** входит (процесс, а не измерение, §6.9).
  > `scope_acceptance` не входит: авторизация, не измерение (§11.5).

  Переносится в `runs` (аддитивная колонка) и в `RunManifest`, чтобы `hound diff` читал
  его из БД прогона, а не пересчитывал из другого набора значений.
- `hound diff` сравнивает `execution_contract_digest`, а не `index_build_id`.
- `content_change` (изменение композиции при том же contract) — **разрешённый** diff с
  явным баннером и per-product doc-delta (`new/reused/removed` по `content_hash`).
- `cohort_expansion`/`cohort_reduction` входят в contract → diff запрещён, `golden-full`;
  помечаются **отдельно** от родового расхождения контракта (иначе расширение когорты
  читалось бы как обычный `execution_change`).
- Legacy-пары (у одного из манифестов контракта нет) сохраняют прежнее поведение —
  сравнение по перечню полей, включая `index_build_id`. Прошлые прогоны не становятся
  задним числом сравнимыми.

Это меняет определение Comparability → **`methodology_version` bump + пере-аннотация
golden**.

> **Решение владельца (19.10): принято.** Вводим `execution_contract_digest` и
> `content_change`; `hound diff` сравнивает contract, изменение контента — разрешённый
> diff с баннером и doc-delta. Это `methodology_change`: `METHODOLOGY_VERSION` bump +
> `golden-full` + пере-аннотация golden. **Реализовано** в истории версий как `2.3.0`
> (правило diff) и `2.4.0` (декларированные оси), но **текущий пин —
> `METHODOLOGY_VERSION = "0.1.0"`** (`core/domain/models.py:125`): 26.10.2026 владелец
> сбросил нумерацию, потому что 2.x описывали методологию, по которой не получено ни
> одного нормального результата, и служили ложной провенанс-записью «измерение по 2.4.0».
> Историю 2.x в `models.py` **не переписывать** (аудит-след), а в текстах ссылаться на
> правило, а не на номер: «правило 2.3.0» — это датировка решения, не текущая версия.
> Практическое следствие: манифесты старых прогонов несут `2.4.0` и читаются как есть;
> расхождение с текущей константой видно в `hound diff` строкой `methodology_change` и
> **не** является ошибкой чтения прогона.

### 12.6. Открытые решения человека

> **Статус: ✅ решения приняты по всем пунктам.** Реализация каждого — там, где назван её
> носитель: п. 1 (2.3.0), 3, 4, 8, 9 — в прогоне; п. 2 (правило `live|revalidated`) —
> граница A (`16` §8); п. 5 — export реализован (`cmd_package_seeds_export`,
> `core/cli/package/commands.py:1394`); п. 6 (**per-cell**) и п. 10 (`run_scope`
> персистится, политика не меняется) — решения 29.10.2026. Открытых вопросов в §18 не
> осталось.

**Принято владельцем 19.10:**

1. **Comparability/diff** — вводим `execution_contract_digest` + `content_change`
   (`methodology_version` bump + golden re-annotation).
2. **`❌` на reused-корпусе** — только `live`/`revalidated` в текущем цикле; `offline`/
   `stale` не публикует `❌` (семантика §4.4 не меняется).
3. **`add-product` при нуле seed'ов** — писать `pending_seeds` + cohort, возвращать
   `EXIT_BLOCKED`; следующий `run` блокируется onboarding-префлайтом (§6.1).
4. **Partial-run policy** — `--update` с неготовым продуктом блокирует весь прогон
   (не публиковать неполный срез); `--only-product` обрабатывает только выбранный
   продукт.

**Ещё открыто:**

5. **Seed-файл** — **принято:** новый `sources/seeds.yaml`; `corpus_urls.yaml` остаётся
   производным экспортом (`hound package seeds export --format corpus-urls`); миграция —
   `ensure_seeds_file` (§7.5).
6. **Retention текста чанков:** бессрочно (рекомендуется, это субстрат evidence) vs
   бюджет/вытеснение. Второе делает часть evidence неверифицируемой. — ✅ **закрыт
   29.10.2026: бессрочно, размер публикуется замером** (§4.5)
7. **Per-cell vs per-product invalidation** (MVP — per-product).
8. **Golden для нового продукта** — **принято и реализовано:** явный waiver по продукту
   (`sources/golden_status.yaml: exempt_products` — `product_id` + `reason` +
   `declared_by`), линтер предупреждает о продукте когорты без эталона и без waiver
   (`golden-coverage`, уровень `warning` — не блок: `golden.csv` не входит в `run_id` и не
   влияет на вердикты, поэтому гейт на нём запрещал бы первый замер продукта, эталон для
   которого аннотируют по его результату), отчёт показывает аномалию
   `golden_exempt_product`; аннотация golden — цель на потом. Носитель — объявление
   эталона пакета, а не `cohort.yaml`/`run.yaml`: waiver не входит в `cohort_digest`,
   иначе объявление «эталона для продукта нет» меняло бы `run_id` и рвало межцикловый
   diff.
9. **Паспорт/сателлиты нового продукта** — **принято:** `add-product` генерирует
   draft-паспорт из scout-артефакта (identity, vendor, сайты, satellites) и помечает
   `needs_human`; eval получает authored-контекст, но draft не считается полным
   паспортом. — **✅ реализовано** как **явный флаг**, а не поведение по умолчанию:
   `hound package add-product --apply --passport` (с `--passports-dir`, дефолт —
   `paths.passports`; `core/cli/package/parser.py:705-710`) вызывает
   `write_passport_draft` (`core/pipeline/package_add_product.py:1550-1558`), и в
   review-артефакт пишется `passport_written`. Без флага черновик не создаётся —
   это намеренно: паспорт авторизует evidence-контекст, поэтому его появление должно
   быть решением оператора, а не побочным эффектом онбординга.
10. **Bootstrap** (явный `state adopt` vs `--adopt-latest`) — ✅ **закрыт 29.10.2026:
    явная `hound package state adopt`** (§4.6, реализовано 29.10.2026), автоматического
    принятия последнего complete-прогона нет и не будет — оно скрывает выбор baseline.

---

## 13. Failure / partial semantics

> **Статус раздела: ⬜ не сделано в части §13.3/§13.5.** §13.1/§13.2/§13.4/§13.6 — as-built по
> фактическим механизмам (снимок `state.db`, покрытие, режимы продукта); §13.3 и
> межпрогонная часть §13.5 — **design-only** (переноса ячеек нет, `--only-failed` сеансовый).

### 13.1. Статусы продукта в прогоне (as-built; носитель — снимок, не `run_product`)

Статус продукта материализуется в `product_snapshot.status` (`state.db`), а не строкой
`run_product` (такой таблицы нет):
- `complete` — coverage complete; `pending`-ячейки допустимы (очередь ревью, не провал);
  `baseline_complete = (scope=full and evaluate_ok and cells_pending == 0)` управляет
  обновлением `last_completed_run_id`;
- `partial` — покрытие есть, но неполно (усечение / нечитаемые origin'ы / блокирующие сбои);
- `failed` — покрытия нет (corpus/eval не состоялись);
- `blocked` — на уровне **прогона**: onboarding-префлайт или гейт не пройдены.

### 13.2. Новый продукт недоступен

- robots deny all / DNS / timeout / все seed'ы `FAILED`:
  - `coverage.complete=False`, `expected_sources>0`, `unreadable_origins` непуст;
  - `evaluate` → `failed`-ячейки (`status=null`), **не** `❌`;
  - снимок продукта — `status=failed`;
  - итог `run` — `partial`/`failed`, `EXIT_ERROR`; в отчёте — аномалия с классами
    (`robots_disallow=N`, `source_unavailable=M`).
- Один из нескольких seed'ов недоступен, остальные читаемы: `partial`, `❌` demoted,
  `✅`/`⚠️` по доступным документам; продукт не становится `complete`.

### 13.3. Старый продукт не изменился (**design-only**) — ⬜ не сделано

> **Статус: design-only.** Всё, что ниже, требует переноса ячеек (§10), которого нет.
> As-built-часть: «все URL → 304/same-hash; doc-set тот же; coverage complete» —
> действительно достижимо и даёт `complete`; но партиция в этом сценарии
> **переиспользуется по `pb` + диску** (§6.3), а ячейки **переоцениваются полностью**,
> `cells_carried` остаётся 0, и LLM вызывается (или отдаёт хит из `eval_cache`).

- Все URL → 304/same-hash; doc-set тот же; coverage complete.
- Партиция не пересобирается; ячейки carry-forward; LLM не вызывается.
- `run_product.status=complete`, `docs_reused=all`, `cells_carried=all`.
- `last_completed_run_id` обновляется (измерение подтверждено).

### 13.4. Старый продукт изменился частично

- Изменённые документы → новые `doc_version`; doc-set digest меняется.
- Партиция продукта пересобирается. Эмбеддинги: `embed_cache` **реализован** (§8.5) —
  пересчитываются только новые чанки; промах кэша не меняет `pb`, потому что ключ
  `(content_hash текста, embed_model_id)` не содержит версии чанкера.
- MVP: все ячейки продукта переоцениваются; eval cache может дать хиты для ячеек, чей
  prompt и условия не изменились. **Условия** — теперь не только промпт: ключ кэша несёт
  методологический контекст (§10.5), поэтому смена правил даёт промах, а не хит.
- Снимок `complete` (если coverage complete), `docs_new=N`, `cells_measured=M`.

### 13.5. Повтор и resume

- `--only-failed` — **сеансовый** (as-built): оставляет `(product, url)` со статусом
  `FAILED` **текущего** сеанса `crawl:{run_id}`; при отсутствии сеанса фильтр не
  применяется (в `notes` — пояснение). `max_attempts` — потолок попыток по URL;
  исчерпание → терминальный `failed` (`attempts_exhausted`), не тихо.
  Межпрогонный вариант (по `url_state.consecutive_failures` предыдущего прогона) —
  **design-only** (§2.4).
- `run` без `--update` и с тем же `run_id` — resume: `Runner` пропускает `ok`-фазы;
  `corpus` переиспользует doc-версии и чанки завершённых продуктов (чанки пишутся на
  границе продукта, §4.7); незавершённый продукт и `index_manifest`/композиция
  восстанавливаются пересборкой только его партиции.
- `--reset` — сбрасывает сессию/фазы текущего прогона, но не state.db.
- Падение между corpus и evaluate: `last_corpus_run_id` установлен, `last_evaluated_run_id`
  — нет → следующий прогон `full` (O12), но doc-версии/партиция переиспользуются.

### 13.6. `--only-product`

- `scope=product_only` (`resolve_run_scope`, `core/cli/commands/run.py:446`); он входит в
  `run_id` через `derive_run_id` — то есть scoped-прогон **отдельный** прогон, а не режим
  полного.
- `last_completed_run_id` **не** обновляется (O10: рубеж двигает только `scope=full` при
  `evaluate_ok` и нуле `cells_pending`). Значение `scope` видно в
  `state.db.product_snapshot.scope` (`record_run_outcome`, `core/pipeline/state_sync.py:224`).
- **В отчёте `scope` нет — закрывается границей A (решение владельца 29.10.2026).**
  Прежняя редакция утверждала «в отчёте — `scope=product_only`, `partial=true`» — этого
  не существует: `run_scope` не персистится ни в `runs`, ни в `RunManifest`, и `report.py`
  его не выводит (`grep run_scope core/pipeline/report.py` — пусто). Читатель, которому
  нужно «полное измерение или нет», вынужден смотреть
  `product_snapshot.scope`/`last_completed_run_id` в `state.db`, то есть в другой БД. Это
  дефект видимости, а не политики: `--only-product` меняет смысл среза, и он должен быть
  виден в артефакте прогона.
  **Решение:** `run_scope` персистится (`runs.run_scope` + `RunManifest.run_scope`),
  политика **не меняется** — scoped-прогон обрабатывает только выбранный продукт и не
  двигает `last_completed_run_id` (O10). Состав — `16-runs-schema-boundary.md` §3.1/§8.
- Следующий полный прогон увидит `last_completed_run_id` от предыдущего полного прогона
  и сделает `incremental` (не `full`), если тот был complete. Это корректно: частичный
  прогон не должен ломать baseline, но и не должен считаться полным измерением.

---

## 14. Schema / ports / migrations (**design-only**) — ⬜ не сделано

> **Статус раздела: design-only.** ALTER'ов, перечисленных в §14.1, **нет** в коде. Не
> писать SQL против этих колонок. Фактическая схема run DB — 20 таблиц
> (`SCHEMA_VERSION = "3.13.0"`), фактическая схема `state.db` — 15 таблиц
> (`STATE_SCHEMA_VERSION = "5"`); реальные колонки — §4.2/§4.3. Порты, описанные в §14.3,
> тоже частично разошлись с `core/ports/state.py` (см. примечания там).

### 14.1. Run DB: новые поля/таблицы (**design-only**)

> **Статус: design-only. Ничего из этого блока в коде нет** — ни колонок, ни таблицы
> `run_product`. Проверено: `grep -rn "run_product\|product_plan_json" core/` — пусто;
> `corpus_docs`/`corpus_chunks`/`cells`/`cell_evidence` не получали этих ALTER'ов.
> Единственные существующие `partition_build_id` — в `index_manifest` и
> `cell_evidence` (обе уже в DDL, не ALTER).
>
> **Состав границы (решение владельца 29.10.2026: одна граница, не две).** Всё, что меняет
> схему прогонной БД или материал `run_id`, делается **одним** сдвигом — иначе прогресс
> обнуляется дважды, а межцикловый diff рвётся два раза без пользы:
>
> - `run_product` + `runs.product_plan_json` (эта секция);
> - исправление `runs.cohort_ref` — писать `cfg.cohort_ref`, а не имя файла когорты
>   (§12.2: сегодня манифест показывает ревизию, которой в `run_id` не было);
> - признак reuse у `corpus_docs` (`http_status`/`reuse_kind`), чтобы `❌` на
>   retained-байтах без серверного подтверждения стал выразим в прогоне (§11.2);
> - `chunk_id` с `doc_version_id` (§14.6 п. 11; `chunker_version` в него не входит —
>   грамматика `source_id` отдаёт 4-й сегмент под `build_id`) — это `content_change`
>   с пересборкой партиций, и делать его отдельно от сдвига схемы незачем;
> - ось `embed_device` (`cpu|cuda`, без `auto`) в `EXECUTION_CONTRACT_FIELDS` и
>   `vector_digest` в проверке переиспользования партиции (оговорка 5 `14`). Устройство
>   эмбеддера **не** идёт в `embed_model_id`: тот классифицируется как `content_change`,
>   то есть смена прибора прочиталась бы как смена содержимого, — прецедент `pdf_device`
>   (в контракт через `extractor_stack`) верный.
>
> Порядок жёсткий: **сначала `report` текущего прогона**, потом граница — правка
> `corpus.py`/`phases.py` на ходу ломает идущий процесс.
>
> **Полная спецификация границы — `16-runs-schema-boundary.md`**: DDL-дельта, ось
> `embed_device`, формат партиции `partition-2`, список тестов и порядок применения. Там же
> решение **не** поднимать `SCHEMA_VERSION`: все изменения аддитивны (`_ensure_columns` +
> `CREATE TABLE IF NOT EXISTS`), а гейт `_reject_foreign_schema`
> (`core/store/sqlite.py:570`) существует ради несовместимого чтения, а не ради счётчика
> ревизий. Значит завершённые прогоны остаются читаемыми и после границы.

```sql
-- design-only: НЕ выполнять, в коде этих колонок нет
ALTER TABLE runs ADD COLUMN cycle_id TEXT;
ALTER TABLE runs ADD COLUMN previous_run_id TEXT;
ALTER TABLE runs ADD COLUMN cohort_digest TEXT;
ALTER TABLE runs ADD COLUMN seeds_digest TEXT;
ALTER TABLE runs ADD COLUMN change_class TEXT;
ALTER TABLE runs ADD COLUMN product_plan_json TEXT;
-- (execution_contract_digest в runs УЖЕ есть — это не design)

CREATE TABLE run_product (
    run_id          TEXT NOT NULL,
    product_id      TEXT NOT NULL,
    mode            TEXT NOT NULL,              -- new|full|incremental
    status          TEXT NOT NULL,              -- pending|running|complete|partial|failed|blocked
    scope           TEXT NOT NULL DEFAULT 'full',
    phase           TEXT,
    coverage_complete INTEGER,
    docs_total      INTEGER NOT NULL DEFAULT 0,
    docs_reused     INTEGER NOT NULL DEFAULT 0,
    docs_new        INTEGER NOT NULL DEFAULT 0,
    cells_total     INTEGER NOT NULL DEFAULT 0,
    cells_carried   INTEGER NOT NULL DEFAULT 0,
    cells_measured  INTEGER NOT NULL DEFAULT 0,
    cells_pending   INTEGER NOT NULL DEFAULT 0,
    baseline_complete INTEGER NOT NULL DEFAULT 0,
    error_kind      TEXT,
    error           TEXT,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (run_id, product_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

-- design-only: corpus_docs НЕ имеет doc_version_id/level/doc_status/reused_from_run_id
ALTER TABLE corpus_docs ADD COLUMN doc_version_id TEXT;
ALTER TABLE corpus_docs ADD COLUMN level TEXT;
ALTER TABLE corpus_docs ADD COLUMN doc_status TEXT;        -- active|superseded|evicted
ALTER TABLE corpus_docs ADD COLUMN reused_from_run_id TEXT;

-- design-only: corpus_chunks НЕ имеет doc_version_id/partition_build_id
ALTER TABLE corpus_chunks ADD COLUMN doc_version_id TEXT;
ALTER TABLE corpus_chunks ADD COLUMN partition_build_id TEXT;

-- design-only: partition_build_id в index_manifest УЖЕ есть в DDL (не ALTER)
ALTER TABLE index_manifest ADD COLUMN partition_build_id TEXT;

-- design-only: cells НЕ имеет measurement_origin/carried_from_run_id/cell_reuse_key/
-- retrieved_doc_set_digest/coverage_digest/catalog_row_digest/partition_build_id
ALTER TABLE cells ADD COLUMN measurement_origin TEXT DEFAULT 'observed';
ALTER TABLE cells ADD COLUMN carried_from_run_id TEXT;
ALTER TABLE cells ADD COLUMN cell_reuse_key TEXT;
ALTER TABLE cells ADD COLUMN retrieved_doc_set_digest TEXT;  -- per-cell, не doc-set продукта
ALTER TABLE cells ADD COLUMN coverage_digest TEXT;
ALTER TABLE cells ADD COLUMN catalog_row_digest TEXT;
ALTER TABLE cells ADD COLUMN partition_build_id TEXT;

-- design-only: cell_evidence НЕ имеет doc_version_id/origin_evidence_id
-- (partition_build_id в cell_evidence УЖЕ есть в DDL, не ALTER)
ALTER TABLE cell_evidence ADD COLUMN doc_version_id TEXT;
ALTER TABLE cell_evidence ADD COLUMN partition_build_id TEXT;
ALTER TABLE cell_evidence ADD COLUMN origin_evidence_id TEXT;
```

`index_manifest` PK остаётся `(index_build_id, product_id)`, где `index_build_id` =
`composition_id`; `partition_build_id` — новое поле (в DDL оно уже есть, ALTER выше
относится к другим таблицам и приведён для полноты целевой схемы).

### 14.2. `run_inputs` — без `REPLACE` (**✅ реализовано**, payload — design-only)

Было: `save_plan_inputs` (`SqliteStore.save_plan_inputs`) делал
`INSERT OR REPLACE INTO run_inputs …`, то есть повторная запись **перезаписывала** снимок
плановых входов молча. Это был разрыв O11 (§3.3): «входы плана — источник истины» держался
на дисциплине вызывающего, а не на схеме.

**Сделано.** `save_plan_inputs` (`core/store/sqlite_store.py:1146`) пишет снимок один раз,
и у него три исхода:

* записи нет → `INSERT … ON CONFLICT(run_id) DO NOTHING`, возвращается `True`;
* запись есть и `payload_sha256` совпал → no-op (`created_at`/`payload_json` **не**
  трогаются: resume не сдвигает время снимка), возвращается `False`;
* запись есть и `payload_sha256` другой → `PlanInputsConflict` (сохранённая строка остаётся
  нетронутой).

Гонки двух писателей нет: вставка и сверка идут в одной транзакции (`with self.conn`), а не
«проверил-потом-вставил» без неё. Исключение объявлено **в порте**
(`PlanInputsConflict`, `core/ports/store.py`) — это контракт, и `ports ↛ store` не
нарушается; реализация импортирует его оттуда. Вызывающий
(`_snapshot_run_inputs`, `core/cli/context.py`) ловит конфликт **отдельной** ветвью и пишет
видимую строку `snapshot: run_inputs_conflict: …` с `run_id` и обоими хешами в
`instrumentation_errors`; контракт «никогда не бросает» сохранён. Тесты:
`tests/store/test_sqlite_store.py` (три исхода + независимость по `run_id`),
`tests/cli/test_run_self_contained.py` (конфликт виден, прогон не падает).

**Остаток (design-only): состав payload.** В снимке по-прежнему нет `cycle_id`,
`cohort_digest`, `seeds_digest`, `product_plan`, `previous_run_id`,
`state_schema_version`: сегодня `run_id` уже несёт `cohort_digest`/`seeds_digest`, а
`previous_run_id` живёт в `state.db.run_lineage`, поэтому снимок самодостаточен для
пересборки каталога/весов/когорты — но не для восстановления цепочки циклов. Добавление
этих полей меняет `payload_sha256`, то есть даёт конфликт с уже записанными снимками:
это версионная граница снимка (`snapshot_version`), а не «дописать поле».

**Остаточный пробел закрыт (29.10.2026).** В ветке `blocked`-onboarding
(`core/cli/commands/run.py`) возвращённый список `issues` раньше **отбрасывался**, поэтому
конфликт там не попадал в `instrumentation_errors` — а заблокированный прогон это ровно
тот случай, когда оператор читает, чего не хватило. Теперь `issues` подмешиваются в
`scenario_errors` (тот же список, что и ошибки персиста сценариев) и видны в отчёте.

### 14.3. Порты

**Статус: частично реализовано.** `ProductStatePort` **существует** — `core/ports/state.py`,
`Protocol` с `@runtime_checkable`, реализация `SqliteProductState`
(`core/store/state_sqlite.py`). Но состав методов ниже — **целевой**: часть объявлена
и работает (as-built), часть отсутствует (design-only) и помечена явно.

Реальные `*Row`-датаклассы (`core/ports/state.py`): `ProductStateRow`, `SnapshotRow`,
`SeedRow`, `UrlStateRow`, `DocumentVersionRow`, `ChunkRow`, `IndexPartitionRow`,
`IndexCompositionRow`, `RunLineageRow`, `ProfileAttestationRow`.

As-built методы (существуют сегодня):

```python
class ProductStatePort(Protocol):
    # продукт
    def get_product_state(self, product_id: str) -> ProductStateRow | None: ...
    def upsert_product_state(self, row: ProductStateRow) -> None: ...
    # снимки
    def last_snapshot(self, product_id: str, *, scope: str = "full") -> SnapshotRow | None: ...
    def record_snapshot(self, row: SnapshotRow) -> None: ...
    # seeds (reconcile, не upsert/disable поштучно)
    def reconcile_seed_set(self, product_id: str, seeds: Sequence[SeedEntry]) -> None: ...
    def seed_rows(self, product_id: str) -> list[SeedRow]: ...
    # URL/document/chunk
    def get_url_state(self, product_id, normalized_url) -> UrlStateRow | None: ...
    def iter_url_state(self, product_id) -> Iterable[UrlStateRow]: ...
    def upsert_url_state(self, row: UrlStateRow) -> None: ...
    def get_document_version(self, doc_version_id) -> DocumentVersionRow | None: ...
    def find_document_version(self, product_id, *, content_hash, extractor_version, level) -> DocumentVersionRow | None: ...
    def upsert_document_version(self, row: DocumentVersionRow) -> None: ...
    def iter_document_versions(self, product_id, *, status: str = "active") -> Iterable[DocumentVersionRow]: ...
    def put_chunk_text(self, content_hash, text, lang=None) -> None: ...
    def get_chunk_text(self, content_hash) -> str | None: ...
    def replace_doc_chunks(self, doc_version_id, chunks: Sequence[ChunkRow]) -> None: ...
    def iter_doc_chunks(self, doc_version_id) -> Iterable[ChunkRow]: ...
    # индексные партиции/композиции
    def register_index_partition(self, row: IndexPartitionRow) -> None: ...
    def iter_index_partitions(self, package_id) -> Iterable[IndexPartitionRow]: ...
    def save_index_composition(self, row: IndexCompositionRow) -> None: ...
    def get_index_composition(self, composition_id) -> IndexCompositionRow | None: ...
    # lineage
    def record_run_lineage(self, row: RunLineageRow) -> None: ...
    def previous_run(self, package_id: str) -> str | None: ...
    def get_run_lineage(self, run_id: str) -> RunLineageRow | None: ...
    def previous_lineage(self, package_id: str) -> RunLineageRow | None: ...
    # аттестация профиля пакета-якоря (G1/G2)
    def save_profile_attestation(self, row: ProfileAttestationRow) -> None: ...
    def find_profile_attestation(self, case_id, package_id, anchor, gate, profile_digest) -> ProfileAttestationRow | None: ...
    def iter_profile_attestations(self, *, case_id, package_id, anchor, gate, include_superseded=False) -> Iterable[ProfileAttestationRow]: ...
    def supersede_profile_attestation(self, attestation_id, *, superseded_at: str) -> None: ...
```

Design-only (объявлены в этом документе, **в коде отсутствуют**):
`set_onboarding_state`, `set_last_corpus_run_id`, `set_last_evaluated`,
`set_last_completed`, `iter_seeds`, `upsert_seed`, `disable_seed`, `iter_chunks`,
`save_snapshot`, `save_cell_measurement`, `get_cell_measurement`, `record_run`,
`update_run_status`, `register_partition`, `get_partition`, `save_composition`,
`get_composition`, `get_attestation`.

Смысл замен: `record_snapshot` вместо `save_snapshot`; `record_run_lineage` вместо
`record_run`/`update_run_status`; `register_index_partition` вместо `register_partition`;
`save_index_composition`/`get_index_composition` вместо `save_composition`/`get_composition`;
`reconcile_seed_set`/`seed_rows` вместо `iter_seeds`/`upsert_seed`/`disable_seed`;
`iter_doc_chunks` вместо `iter_chunks`. Методы пер-ячеечного измерения
(`save_cell_measurement`/`get_cell_measurement`) и `get_attestation` **не имеют писателя и
читателя** в прогоне: ячейки живут в run DB (`cells`), а durable-аттестация — это
`profile_attestation` (§6.9).

Реализация: `core/store/state_sqlite.py` (`SqliteProductState`), DDL — §4.2.
`pipeline` зависит только от Protocol; composition root (`core/cli/wiring.py`) строит
`SqliteProductState(paths.state / package_id / "state.db")`.

`StorePort` (run DB) расширяется (**design-only**):
- `save_run_product`, `iter_run_products`, `read_run_product`;
- `iter_evidence(run_id, product_id, param_id)`;
- `save_plan_inputs` — семантика без `REPLACE` (см. §14.2);
- `iter_chunks(build_id, product_id=...)` — `build_id` теперь `partition_build_id`
  (для lexical/vector); добавить `iter_chunks_by_composition(composition_id, product_id)`
  или оставить `iter_chunks` с `partition_build_id` и вызывать по member'ам.

`IndexBuilderPort` (**design-only**):
- `build(chunks, vectors, meta)` — без изменения сигнатуры; `meta.extra["partition_build_id"]`
  обязателен; `meta.product_id` — один продукт;
- `build_id` возвращается = `partition_build_id`;
- новый `IndexCatalogPort` (или методы state) — регистрация/чтение композиций. Не смешивать
  с `IndexBuilderPort` (запись vs чтение).

`VectorIndexPort`/`LexicalIndexPort` (**design-only**):
- конструктор/фабрика принимает `CompositionView`;
- `build_id` = `composition_id`; per-chunk `build_id` = `partition_build_id`.

`CrawlerPort` — без изменений: условный GET уже выражен в `FetchPlan`
(`if_none_match`, `if_modified_since`). Меняется только источник этих значений:
`url_state` (**design-only**), а не HTTP-кэш.

### 14.4. Артефакты и раскладка

As-built (существует):

```
packages/{case_id}/
  sources/corpus_urls.yaml               # операторский корпусный вход (--urls)
  sources/seeds.yaml                     # канонический seed-набор (MVP-1; миграция legacy)
  sources/add_product/{date}-{product_id}.json  # артефакт apply (seed_plan/onboarding)

state/{package_id}/state.db              # durable per-product state (есть, STATE_SCHEMA_VERSION="5")
state/{package_id}/state.db-wal

runs/{run_id}/
  cohound.db                             # входы плана, манифест, ячейки, гейты
  plan_inputs.json                       # производный экспорт
  logs/, prompts/, outputs/              # prompts/ — аудит исполненных промптов (V7): один
                                         # файл на prompt_hash, пишет Ш6 и пере-оценка

index_store/{package}/
  {partition_build_id}/{product_id}/     # иммутабельная продукт-партиция
  _compositions/{composition_id}.json    # иммутабельная композиция
```

`core/config/paths.py` уже содержит поле `state` (`PathsConfig.state`, `ENV_STATE_DIR`,
`config/paths.example.yaml`) — это **не** будущая работа.

Design-only (нет в коде):

```
state/{package_id}/.lock                 # ✅ есть (flock + JSON-держатель, 09 §4.4)

runs/{run_id}/
  cohound.db                             # + run_product, новые поля (нет)
  run.yaml                               # снимок эффективного cycle_id (нет)
  plan_inputs.json                       # + product_plan/lineage (нет)
```

Бэкап (**design-only**): `core/store/backup.py` умеет `backup_run`/`restore_run`
(библиотечные функции, CLI-команды `hound backup` **нет**). Включение
`state/{package}/state.db` в бэкап прогона не сделано; `--include-state` — предложение,
а не существующий флаг.

### 14.5. Layer matrix / категорийная нейтральность

| Модуль | Слой | Зависимости |
|---|---|---|
| `core/domain/enums.py`, `models.py`, `invariants.py` | domain | только stdlib/pydantic |
| `core/ports/state.py` | ports | domain |
| `core/config/seeds.py` | config | domain/config/util |
| `core/pipeline/onboarding.py`, `incremental.py`, `composition.py` | pipeline | domain/ports/util |
| `core/store/state_sqlite.py` | store | domain/ports, sqlite3 |
| `core/cli/wiring.py`, `commands/*` | cli | всё (composition root) |
| `core/adapters/index_npy.py`, `vector_npy.py` | adapters | ports/util |

`pipeline` **не** импортирует `store`/`adapters`/`cli`; `state` инжектится как
`ProductStatePort`. Грепп-тест §1.3: в `core/domain`, `core/ports`, `core/pipeline` нет
названий продуктов/вендоров; `seeds.yaml` — данные пакета, не код.

### 14.6. Что придётся заменить, а не надстроить

**Статус: сделано в пп. 1–7, 9–10; design-only в пп. 8, 11–12.** Ниже — целевые замены;
для сделанного указано, где именно, и что осталось.

1. `derive_run_id` (`core/pipeline/plan.py:79`) — ✅ **реализовано**: сигнатура
   `(inputs, *, cycle_id, cohort_digest, seeds_digest, execution_contract_digest,
   run_scope="full", refresh_policy="auto")`; `products`/`snapshot_date` в материале нет
   (§6.2). Дайджесты обязательны — молчаливого fallback на старую формулу не осталось
   (`core/pipeline/plan.py:251-268`: без них `ValueError`, если `run_id` не передан явно).
2. `_derive_partition_build_id`/`_product_content_token` (`core/pipeline/corpus.py`) —
   **уже разделены** (MVP-4 шаг 2): `partition_build_id` per-product, `composition_id` —
   по триплетам `(product_id, partition_build_id, content_digest)`
   (`core/pipeline/composition.py`). Устаревшие имена `_derive_index_build_id`/`_content_token`
   в коде отсутствуют; остаётся только `_product_content_token` для отпечатка содержимого.
3. `_persist_corpus_coverage` (`core/pipeline/corpus.py`) — ✅ **реализовано**: строка пишется
   для **каждого** продукта плана, включая zero-source (см. §11.1/§11.3).
4. `_coverage_for` (`core/pipeline/evaluate.py:1454`) — ✅ **реализовано**: `None` для
   продукта плана даёт synthetic zero-source строку (`reason=coverage_missing`,
   `:1478-1486`), которая запрещает `❌` и даёт failed-ячейки, а не «покрытие неизвестно».
5. `load_effective_seeds` (`core/pipeline/onboarding.py:146`) — ✅ **реализовано**:
   приоритет `--urls > seeds.yaml > corpus_urls.yaml` (`urls_override` заменяет набор
   целиком, `:167-198`; `seeds.yaml` — `:201-206`; legacy `corpus_urls.yaml` — `:203`).
   Вызывается из `run`/`plan`/`crawl` (`core/cli/commands/run.py:488`,
   `core/cli/commands/plan.py:220`, `core/cli/commands/crawl.py:305`). Прежняя редакция
   называла это design-only и указывала `core/cli/context.py` — неверно по обоим пунктам.
6. `NpyVectorIndex` (`core/adapters/vector_npy.py:213`) — ✅ **реализовано**:
   `CompositionViewPort` принимается как `view`, `build_id` композиции сверяется с
   `view.composition_id` и расхождение отвергается (`:219-226`), каждая партиция читается
   по своей карте.
7. `seed_lexical_index` (`core/cli/wiring.py:422`) — ✅ **реализовано**: итерация по
   `view.members` через `store.iter_chunks(partition_build_id, product_id=…)`, потоково
   (генератор, без списка записей), с гидртацией текста из store через `get_chunks`.
8. `--only-failed` (`core/cli/context.py`, `_filter_corpus_urls:1579`) — **сессионный и
   остаётся таким**: фильтр применяется к `FAILED`-элементам **текущего** сеанса, без
   сеанса не применяется (это видно в `notes`); сочетание с `--update` и с
   `--only-product` отвергается `EXIT_USAGE` (`core/pipeline/run_identity.py:172,192-193`).
   Межпрогонный смысл («повторить то, что упало в прошлом прогоне») — **design-only**;
   текущее поведение описано в §2.4.
9. `save_plan_inputs` (`core/store/sqlite_store.py`) — `INSERT OR REPLACE` →
   insert-once/conflict — **✅ сделано** (`PlanInputsConflict`, §14.2). Остаток —
   состав payload (design-only, §14.2).
10. `EvalVerdictCache` (`core/pipeline/eval_cache.py`) — ключ без методологических полей;
    **✅ сделано**: `eval_echo` расширен объявленным методологическим контекстом,
    `EVAL_CACHE_FORMAT` поднят до `2`, конверт формата 1 читается как промах (§10.5).
    Состав `cell_reuse_key` (перенос ячеек) остаётся design-only — это другое: ключ
    **кэша вердиктов** уже честен, ключ **переноса ячеек** не существует.
11. `_chunk_id` (`core/pipeline/corpus.py:2005`) — сейчас
    `f"{product_id}:{doc_hash}:{chunk_no}"`, где `doc_hash` — **content hash**; станет
    `f"{product_id}:{doc_version_id}:{chunk_no}"` (**design-only**; это корень частичного
    O4, §3.3). Решение 29.10.2026: `chunker_version` в `chunk_id` **не входит**, и это не
    упущение.
    - **Грамматика `source_id` не даёт места.** `source_id` —
      `product:doc:chunk_no[:build_id]` (`core/util/ids.py:33-44`), `parse_source_id`
      принимает ровно 3 или 4 сегмента и четвёртый трактует как `build_id`
      (`ids.py:57-74`). Пятый сегмент — отказ разбора; четвёртый под `chunker_version` —
      подмена смысла `build_id`, то есть сломанный резолв I10/I15.
    - **В пределах одной прогонной БД `chunker_version` — константа.** Это поле
      `EXECUTION_CONTRACT_FIELDS` (`core/pipeline/digests.py`), значит смена чанкера даёт
      новый `run_id` и новую БД. `corpus_chunks.chunk_id` — глобальный PK
      (`core/store/sqlite.py:146`), но две версии чанкера в одном PK-пространстве не
      встречаются, а смена чанкера и так даёт новый `partition_build_id` (он
      `chunker_version` содержит, §8.2). То есть другой текст никогда не окажется под тем
      же `chunk_id` внутри одного измерения.
    - **`doc_version_id` нужен именно потому, что `content_hash` мало.** Он хеширует
      `(product_id, content_hash, extractor_version, level)`
      (`core/pipeline/incremental.py:271-282`), то есть различает версии одного текста,
      извлечённого другим экстрактором или на другом уровне; сейчас такие чанки
      сталкиваются в один `chunk_id`, хотя текст разный.
    Правка **не косметическая**: `chunk_id` — часть `LexicalChunk`/`RetrievedChunk` и адрес
    гидртации из store, поэтому смена формата идентификатора — `content_change` с
    пересборкой партиций, а не «дописать поле». Именно поэтому она не сделана «по ходу».
12. `corpus_docs.doc_id`/`corpus_chunks.doc_id` — перейти на `doc_version_id` (старые
    прогоны остаются читаемыми; новые используют новый формат) (**design-only**).

---

## 15. Tests / acceptance

> **Статус раздела: ⬜ не сделано (acceptance-тестов нет).** Ниже — статус на каждый
> сценарий. «as-built» = поведение есть в коде сегодня (проверено по маршруту вызова; это
> **не** утверждение, что для сценария существует отдельный тест); «design-only» = сценарий
> проверяет механизм, которого нет (§10 carry-forward, `run_product`, `freshness`,
> `seeds`-UX). Тесты design-only-строк писать не против чего — они станут
> acceptance-критерием соответствующей полосы.

### 15.1. Функциональные сценарии

| # | Сценарий | Ожидание | Статус |
|---|---|---|---|
| T1 | `add-product` без `--url`, SERP недоступен | plan-only: `needs_human=["identity"]`, ничего не записано | **design-only** (`needs_human` — свободный текст, маркера `identity` нет) |
| T2 | `add-product --url <hint>` | seeds.yaml содержит hint + все обнаруженные сайты; `--url` не единственный seed | as-built (`apply_add_product` пишет `sources/seeds.yaml`) |
| T3 | `add-product --apply`, discovery дал 0 seed'ов | cohort + seeds `pending_seeds`; exit `EXIT_BLOCKED`; artifact `needs_human=["no_seeds"]` | as-built |
| T4 | `run --update` после T3 | preflight → `blocked`, `reason=product_not_ready`, сообщение с продуктом; нет cells/report | as-built (это CLI-префлайт, **не** фаза `onboarding` — §6.1) |
| T5 | Новый продукт с 2 seed'ами | full crawl обоих; coverage `expected>0`; все ячейки `observed`; `last_completed_run_id` обновлён | as-built (`record_run_outcome` двигает рубежи) |
| T6 | Два новых продукта за один update | оба `new`; старые `incremental`; композиция содержит все; старые партиции не пересобраны | as-built (reuse партиции — по `partition_build_id`, §6.3) |
| T7 | Старый продукт, все URL → 304 | `docs_reused=all`; `cells_carried=all`; LLM/embed не вызваны; partition id не изменился | as-built в части reuse документов/партиций и `embed_cache`; **design-only** в части `cells_carried` (переноса ячеек нет, §10) — ячейки переоцениваются, но при неизменном промпте и условиях отдают хит из `eval_cache` (§10.5), то есть LLM всё же не вызывается |
| T8 | Старый продукт, один документ изменился | новый `doc_version`; новая партиция только этого продукта; ячейки продукта переоценены; остальные carried | as-built частично: новая партиция и переоценка — есть; «остальные carried» — design-only |
| T9 | Новый продукт недоступен (robots/timeout) | `run_product=failed`; coverage `complete=False`; cells `failed`/`status=null`; нет `❌`; exit error | as-built по coverage/cells; `run_product` как таблица — **design-only** (статус продукта — в снимке, §13.1) |
| T10 | Повторный `run` после падения | resume того же `run_id`; `ok`-фазы пропущены; добытые doc-версии переиспользованы; нет дублей | as-built: `corpus_docs` пишутся инкрементально, `corpus_chunks` — на границе **продукта** (§4.7), провенанс спуска — write-through; теряется максимум незавершённый продукт и реестр (`index_manifest`/композиция), и реестр восстанавливается без пересборки (`pb` контент-адресуем) |
| T11 | `run --update` с расширением когорты | `change_class=cohort_expansion`; `golden_required=full`; diff запрещён с `execution_change: cohort_digest`; `previous_run_id` корректен | as-built (`run_identity` + `run_lineage`; `previous_run_id` — поле lineage, §12.3) |
| T12 | `--only-product` | `scope=product_only`; `last_completed_run_id` не изменён; отчёт помечен partial | as-built по scope/рубежам; пометка «partial» в отчёте — **не проверено** |
| T13 | `--only-failed` после T9 | повторяются только failed URL/продукты; успешные не фетчатся | as-built, но **сессионный** фильтр, не межпрогонный (§13.5) |
| T14 | Пустые seed'ы, preflight обойдён (unit) | coverage `expected=0, complete=False, reason=no_seeds`; cells `failed`; нет «тихого ok» | as-built |
| T15 | Продукт retired, затем re-add | режим `new`; полный краул; старые партиции сохранены, но не в композиции | **design-only** |
| T16 | Переименование `canonical_name` | режим `incremental`; краул не запускается; меняется только `product_state` | **design-only** |
| T17 | Смена `cutoff_K` | `cell_reuse_key` не совпал; все ячейки переоценены (I20) | **design-only** (нет `cell_reuse_key`) |
| T17b | Изменён документ вне top-K ячейки | ячейка **перенесена**, LLM не вызван (per-cell) | **design-only** |
| T18 | Смена `methodology_version` | carry-forward запрещён; eval cache miss; golden-full требуется | **design-only** |
| T19 | Смена модели/слота | carry-forward запрещён; eval cache miss (I21) | **design-only** |
| T20 | `offline` refresh | `freshness=stale`; `❌` → pending; отчёт явно показывает stale | **design-only** (`freshness`-колонки нет, §11.2) |

### 15.2. Инвариантные тесты

**Статус: design-only** (кроме I17/I18/I19-части — они проверяются и сегодня, поскольку
продукт-скоупед ретривал и «оценщик без веб-тула» уже реализованы).

- **I17:** retrieval по `product_ids=[A]` не возвращает чанки B; carried evidence
  `product_id=A`; композиция не смешивает партиции.
- **I18:** evaluator без web-tool; carry-forward не делает сетевых вызовов (mock
  crawler/search не вызывается) — вторая половина design-only.
- **I19:** каждая evidence-строка имеет `doc_version_id` + `content_hash`; doc-версия
  входит в doc-set; reused-документ прошёл `raw→hash→index` в прошлом цикле и
  ревалидирован в текущем.
- **I20:** `cutoff_K` в `cell_reuse_key`; golden и pipeline используют один K — design-only.
- **I21:** `llm_scenario` в `cell_reuse_key`; смена слота → miss — design-only.
- **O1–O13:** property-тесты на `resolve_product_modes` (все комбинации
  `last_corpus_run_id`/`last_evaluated_run_id`/`last_completed_run_id`/`re_added_at`/`scope`).

### 15.3. Регрессии

- `tests/selfcheck.py` — проверки DDL `state.db` и инвариантов O1–O13 там **нет** (это
  осознанно: дерево-гейты не дублируются unit-тестами, см. «Как читать»). Покрытие
  живёт в pytest: `tests/store/test_state_sqlite.py` (8), `test_state_store.py` (12),
  `tests/pipeline/test_state_sync.py` (24), `test_onboarding_modes.py` (11),
  `test_incremental.py` (17), `test_composition.py` (17).
- `tests/domain/` — property-тесты `ProductMode`, `CellReuseKey`, `composition_id`
  (детерминизм/порядко-независимость). `composition_id` — as-built; `CellReuseKey` — нет.
- `tests/packages/test_build_package.py` — `seeds.yaml` vs `corpus_urls.yaml` деривация
  (generator drift). As-built: деривация legacy → seeds есть; обратная — `seeds export
  --format corpus-urls` (round-trip проверяется тестом, см. §7).
- `tests/packages/test_cohort_revision.py` (11) — ревизия когорты: `--apply` двигает
  `cohort_ref`/`frozen_at`, повтор — no-op (байт-идентичный `run.yaml`), смена только
  `frozen_at` ревизию не двигает, формула `next_cohort_ref`, точечность патча.
- `tests/pipeline/test_golden_exempt.py` (23) — waiver `exempt_products`: разбор (без
  поля/без `reason`/без `declared_by`/дубликат/неизвестный ключ/`declined`), `cohort_digest`
  не меняется от объявления, линт `golden-coverage`/`golden-exempt-sync`, аномалия в
  `Artifacts` и `nda_questions.csv`.
- `scripts/ci.sh` — без изменений; новые тесты входят в pytest.

---

## 16. Rollout

> **Статус раздела: ⬜ не сделано (этап 5).** Этапы 0–4 реализованы (MVP-1…MVP-4 шаг 2);
> этап 5 (carry-forward/coverage honesty) **не начат**. Таблица оставлена как запись
> плана — «Что» в ней читается как намерение, а не как отчёт о сделанном.

### 16.1. Этапы

| Этап | Что | Риск | Ценность | Статус |
|---|---|---|---|---|
| **0. Контракты** | `seeds.yaml` schema, `ProductStatePort`, enum'ы, `state.db` DDL, `composition` формулы | низкий | фиксирует границы | ✅ сделано; enum'ы `CoverageFreshness`/`MeasurementOrigin`/`RunProductStatus` — ⬜ **нет** (§3.1) |
| **1. Seeds + preflight (MVP-1)** | `add-product` пишет seeds; `run` читает seeds; preflight блокирует `pending_seeds`; coverage zero-source; `_coverage_for` anomaly | низкий | закрывает «тихий пустой продукт» | ✅ сделано, включая аномалию `coverage_missing` (`core/pipeline/evaluate.py:1466`) |
| **2. Run identity + state (MVP-2)** | `derive_run_id` без products; `cycle_id`/`--update`; `state.db` (product/seed/snapshot/lineage); new/full/incremental | средний | продукт не теряется между прогонами | ✅ сделано; `run_product` — ⬜ нет (§14.1) |
| **3. Incremental corpus (MVP-3)** | `url_state`/`document_version`; conditional GET; reuse doc/chunks; per-product doc-set | средний | экономия сети/экстракции | ✅ сделано; чанки durable в run DB на границе продукта (§4.7) |
| **4. Partitions + composition (MVP-4)** | `partition_build_id`; `composition_id`; `CompositionView`; retrieval/lexical | высокий (индексный контур) | добавление продукта не пересобирает старые | ✅ сделано (шаг 1 и шаг 2) |
| **5. Carry-forward + coverage honesty (MVP-5)** | `cell_reuse_key`; `measurement_origin`; coverage freshness; eval cache key; tests | высокий (методология) | экономия LLM + честный отчёт | ⬜ **не начато** в части переноса/`freshness` (design-only: §10, §11.2, §11.4); **eval cache key — ✅ сделано** (§10.5): методологические условия в ключе, `EVAL_CACHE_FORMAT = 2` |
| **Later** | per-cell invalidation, sitemap `lastmod`, `seeds` UX, state GC, **измерение** паритета `pb` local/split (шов `/v1/extract/*` и идентичность экстрактора в пространстве ядра — ✅ есть; паритет не измерен), embed cache — ✅ уже есть (`core/cli/context.py:2120`) | — | оптимизация | ⬜ не начато |

### 16.2. MVP (3–5 шагов)

Историческая нарезка плана; статус — в §16.1.

1. **Seeds + preflight.** `sources/seeds.yaml` + `add-product` запись + `run` чтение +
   preflight-блокировка + coverage zero-source. Это самостоятельная ценность: продукт
   больше не «тихий пустой».
2. **Run identity + durable state.** `derive_run_id` (cycle/cohort/seeds), `--update`,
   `state.db` с `product_state`/`seed_set`/`product_snapshot`/`run_lineage`,
   `run_product` (**design-only**), new/full/incremental.
3. **Incremental corpus.** `url_state`/`document_version`/`chunk_store`, conditional GET,
   reuse doc/chunks; `--refresh`.
4. **Per-product index + composition.** `partition_build_id`/`composition_id`,
   `CompositionView`, retrieval/lexical, reuse старых партиций.
5. **Carry-forward + coverage.** `cell_reuse_key`, `measurement_origin`, coverage
   `reused/revalidated/freshness`, eval cache key, acceptance-тесты, golden-full gate
   — **design-only, не начато**.

### 16.3. Риски и компромиссы

| Риск | Митигация |
|---|---|
| Diff-семантика (`index_build_id`) ломает недельный diff | решение человека (§12.5); рекомендуемый `execution_contract_digest` + `content_change` |
| Две БД (run/state) рассинхронизируются при падении | content-addressed state; snapshot после eval; recovery `aborted`; идемпотентные upsert'ы |
| State DB растёт (chunk text, snapshots) | retention/GC (**design-only**, §4.5); текст — бессрочно (evidence), остальное — по бюджету |
| Конкурентные прогоны одного пакета | **закрыто 29.10.2026**: файловый lock `state/{package_id}/.lock` (§4.4) отвергает второго владельца пакета (`EXIT_USAGE` с именем держателя); WAL + `busy_timeout=5000` остаются защитой носителя, а не пакета |
| Индексный рефакторинг ломает старые прогоны | новые id-схемы; старые партиции читаются старым кодом; новые — только новый формат; back-compat не требуется |
| `add-product` делает сетевые вызовы (SERP/scout) | plan-only по умолчанию; `--apply` фиксирует провенанс; повторный apply идемпотентен |
| `golden-full` на каждое расширение когорты дорого | батчить добавления продуктов в один цикл; golden-full один раз на цикл |
| `--update` забыт → resume старого цикла | документация/CI: production-run требует `--update`; `run.yaml:cycle_id` + warning при повторном использовании |
| Категорийная нейтральность | seeds/state — данные; `core/` только generic-поля; грепп-тест §1.3 |

### 16.4. Альтернативы (отклонены)

- **Переиспользовать старый `run_id` и мутировать его БД.** Нарушает провенанс/§13;
  `run_inputs`/снимок `run.yaml` разойдутся; diff станет ложным.
- **Глобальный `history.db` на все пакеты.** Cross-package блокировки, blast radius,
  смешение `product_id`; per-package state чище.
- **Хранить seed'ы только в `state.db`.** Seed'ы — авторские данные, должны быть в git и
  проходить review; state — операционный кэш.
- **Единый batch `index_build_id` + «умный» кэш партиций.** Всё равно пересобирает id и
  требует копирования; per-product partition id — прямее.
- **Автоопределение нового цикла по дате.** Ломает resume после падения; `--update` явный.

---

## 17. Сводка ключевых решений

> **Статус раздела: ⬜ не сделано в части 9–11 и 13.** ✅ п. 1–8, 10 (кроме `freshness`), 12;
> ⬜ п. 9, 11, 13 (межпрогонный `--only-failed`), 14 (O5/O6/O13), 15 (остаток).
> Маркер стоит в каждом пункте.

1. **Seed'ы — первоклассный пакетный артефакт** `sources/seeds.yaml` (роль, source,
   profile_key, enabled); `--url` — только hint, не единственный seed. — ✅ **реализовано**
2. **`pending_seeds` — durable состояние**, а не молчаливый пропуск: `add-product`
   добавляет продукт в когорту, пишет пустой seed-набор и возвращает `EXIT_BLOCKED`;
   следующий `run` блокируется префлайтом (не фазой `onboarding`, §6.1). — ✅ **реализовано**
3. **`run_id` больше не зависит от списка продуктов**: `cycle_id + cohort_digest +
   seeds_digest + execution_contract_digest + run_scope + refresh_policy + focus`.
   Добавление продукта — новый прогон, не мутация. — ✅ **реализовано**
4. **Durable per-product state — `state/{package}/state.db`** (не глобальный `history.db`):
   `product_state`, `seed_set`, `url_state`, `document_version`, `chunk_store`,
   `product_snapshot`, `run_lineage`, `index_partition`, `index_composition`,
   `profile_attestation`. `cell_measurement`/`attestation` — в DDL, но **без писателя**
   (§4.2). — ✅ **реализовано** (кроме двух таблиц)
5. **«Новый» = нет `last_corpus_run_id`**, а не diff когорты; `full` = corpus был, eval
   нет; `incremental` = есть complete snapshot. — ✅ **реализовано**
6. **Новый/неполный продукт — полный краул; существующий — conditional GET + reuse**
   документов/чанков/эмбеддингов/партиций; `offline` — только для debug, `❌` запрещён.
   — ✅ **реализовано** (durability чанков в run DB — на границе продукта, §4.7)
7. **Индекс: per-product immutable partitions + cohort composition manifest**;
   `partition_build_id` не включает `params_token`/`snapshot_date`; `composition_id` —
   `runs.index_build_id`; добавление продукта не пересобирает старые партиции.
   — ✅ **реализовано**
8. **Retrieval через `CompositionView`** (product_id → partition_build_id); I17
   сохраняется; lexical/vector читают member'ы композиции. — ✅ **реализовано**
9. **Carry-forward ячеек по `cell_reuse_key`** (каталог, промпт, модель, retrieval,
   **retrieved-набор ячейки**, coverage, методология, аттестация, partition);
   `measurement_origin` `observed|carried`; evidence привязан к `doc_version_id` (I19).
   **Per-cell** (решение 29.10.2026): меняется документ вне top-K — ячейка переносится,
   LLM не вызывается. — ⬜ **design-only**
10. **Coverage пересчитывается всегда**; `reused` — переиспользование контента, не
    отсутствие наблюдения; `complete` требует `expected>0`; `❌` требует complete +
    non-stale (или аттестации). — ✅ **реализовано** в части `complete`/`expected>0`;
    `non-stale`/`freshness` — ⬜ **design-only** (§11.2)
11. **Eval cache key расширяется методологическими полями** — иначе кэш может вернуть
    вердикт по старым правилам. — ✅ **реализовано** (§10.5): `EVAL_CACHE_FORMAT = 2`,
    в ключе версии правил, политика расчёта, строка каталога и условия ретривала;
    конверт формата 1 читается как промах.
12. **§13: расширение когорты = `cohort_expansion` (execution_change) + `golden-full`**;
    lineage (`previous_run_id`, `cycle_id`, `execution_contract_digest`) фиксируется в
    `state.db.run_lineage` (не в `runs`/манифесте — §12.3). — ✅ **реализовано**
13. **`--update` явный**; `--since-run` — override predecessor; `--only-failed` —
    межпрогонный (**design-only**: сейчас сессионный); `--only-product` не обновляет
    baseline. — ⬜ **сделано частично** (межпрогонный `--only-failed` — design-only)
14. **O1–O13** — новые инварианты, закрывающие тихий пустой продукт, пустой coverage,
    ложный carry-forward и потерю провенанса. — ⬜ **сделано частично** (§3.3)
15. **Заменяем, а не надстраиваем:** `derive_run_id`, batch `index_build_id`,
    `_persist_corpus_coverage`, `_coverage_for`, `NpyVectorIndex` single-build,
    `--only-failed`, `save_plan_inputs` REPLACE, `chunk_id`. — **сделано** для
    `derive_run_id`, `_persist_corpus_coverage`, `index_build_id` (per-product),
    `_coverage_for`, `NpyVectorIndex` (композиция), `save_plan_inputs`;
    ⬜ design-only — межпрогонный `--only-failed` и `chunk_id` (§14.6).

---

## 18. Открытые вопросы / риски для решения человека

> **Статус раздела: ⬜ не сделано — 2 вопроса из 10 остаются открытыми** (п. 2, 6;
> п. 10 закрыт частично). Закрытые помечены ✅, открытые ⬜. П. 5 и 9 закрыты
> решениями владельца 29.10.2026; п. 7 закрыт механизмом (waiver + линт) в тот же день.

1. **Comparability/diff.** ~~Вводить ли `execution_contract_digest` и класс
   `content_change`…~~ **Решено и реализовано (2.3.0):** `hound diff` сравнивает
   `execution_contract_digest`, `content_change` — разрешённый diff с баннером и doc-delta
   (§12.5). — ✅ **закрыт**
2. **`❌` на reused-корпусе.** Требовать ли live-revalidation (304) в текущем цикле
   (рекомендуется, не меняет семантику §4.4) или разрешать `❌` на `stale` с аттестацией
   (смена absence-семантики → `methodology_version`)? — ✅ **закрыт 29.10.2026: `❌`
   публикуется только при `reuse_kind ∈ {live, revalidated}`** — сервер ответил в этом
   цикле. `reused` (retained-байты без контакта, 502 из 2125 `fetched` в прогоне
   29.10) и `stale` — `❌` не публикуется, ячейка остаётся `❓` с машинной причиной.
   **Реализация не требует `freshness`** (её нет и не будет в этой волне): нужен признак
   reuse в прогонной БД, то есть `corpus_docs.{http_status, reuse_kind}` — это состав
   границы A (`16-runs-schema-boundary.md` §3.3). Прежняя редакция считала пункт
   зависящим от `freshness` — это было неверно: различение уже существует в
   `url_state.reuse_kind`, не хватало проводки до `corpus_docs`.
3. **Поведение `add-product` при нуле seed'ов.** Писать `pending_seeds` + cohort и
   возвращать `EXIT_BLOCKED` (рекомендуется, run блокируется) или не добавлять продукт
   вовсе (тогда run о нём не знает)? — ✅ **закрыт** (реализован первый вариант)
4. **Seed-файл и миграция.** Новый `sources/seeds.yaml` (рекомендуется) vs расширение
   `corpus_urls.yaml`; как обновлять `packages/{case}/sources/build_package.py`, чтобы
   не было generator drift. — ✅ **закрыт** (seeds.yaml + legacy-миграция в
   `apply_add_product`)
5. **Retention текста чанков.** Бессрочно (рекомендуется: это субстрат evidence) vs
   бюджет/вытеснение (тогда часть evidence становится неверифицируемой). — ✅ **закрыт
   решением 29.10.2026: бессрочно, оси `crawl.retention_days` не будет**; вместо потолка
   размер публикуется замером (§4.5)
6. **Per-cell vs per-product invalidation.** MVP переоценивает весь продукт при изменении
   любого документа (консервативно, дороже по LLM); per-cell требует retrieval до
   проверки ключа и сложнее. Нужен порог/политика. — ✅ **закрыт 29.10.2026: per-cell.**
   Решение владельца: переоценивается только ячейка, у которой изменился **её**
   retrieved-набор. Следствия, которые меняют дизайн §10 (ниже он переписан):
   ключ переноса считается **после** retrieval и несёт `retrieved_doc_set_digest`
   (документы фактически извлечённых чанков), а не `doc_set_digest` всего продукта;
   `carry_forward` перестаёт быть батч-проходом до оценки и становится частью
   ячеечного цикла Ш6; `cell_measurement` получает колонку под retrieved-набор.
   Цена решения названа честно: retrieval выполняется **всегда** (он и так выполняется —
   переносится только момент проверки ключа), а экономия LLM становится точной, а не
   консервативной. Порога по числу изменённых документов **нет** — он был бы ещё одной
   осью, которую надо калибровать (A3: мерить, а не предугадывать).
7. **Golden для нового продукта.** Было: `golden.csv` содержал строки только для
   `pangolin`/`postgrespro`, поэтому новый продукт молча оказывался вне golden. Варианты:
   (а) требовать golden-строки для каждого продукта когорты; (б) явный waiver
   (причина + автор) с отчётом-аномалией; (в) golden только для «эталонной» подкогорты. —
   ✅ **закрыт по механизму (29.10.2026), вариант (а) + (б):** `packages/ru-pg-dbms/
   golden.csv` покрывает **все пять** продуктов когорты (905 ячеек = 5 × 181: `full` 750 =
   5 × 150, `fast` 155 = 5 × 31), а «продукт добавили, golden для него не написали»
   перестало быть молчанием: waiver объявляется полем `exempt_products` в
   `sources/golden_status.yaml` (`product_id` + `reason` + `declared_by`, разбор —
   `core/pipeline/golden_status.py`), неполноту покрытия видит линтер
   (`golden-coverage`, уровень **`warning`**), рассинхронизация объявления с данными —
   тоже `warning` (`golden-exempt-sync`), а отчёт пишет аномалию `golden_exempt_product`
   в `Artifacts.golden_exempt_products` и строкой в `nda_questions.csv`. Носитель — файл
   объявления эталона, **не** `cohort.yaml`: waiver не входит в `cohort_digest`, поэтому
   объявление не меняет `run_id` и не рвёт межцикловый diff.
   **Почему `warning`, а не блок (решение владельца 29.10.2026):** `golden.csv` не входит
   в `execution_contract_digest`, не влияет на вердикты и на карту (§11.3) — блокирующий
   гейт на нём объявлял бы проверочный инструмент условием замера и запрещал бы **первый**
   замер продукта: эталон аннотируют по результату первого прогона, ответного ключа до
   него не существует (так и появился пятый продукт когорты). Аннотация golden для нового
   продукта — по-прежнему цель на потом.
8. **Паспорт/сателлиты нового продукта.** Если `--passport` не задан, у продукта нет
   паспорта и сателлитов; retrieval работает только по корпусу. Варианты: (а) требовать
   draft-паспорт для допуска к eval; (б) разрешать eval без паспорта с `needs_human` и
   понижением `✅`; (в) генерировать draft-паспорт из scout-артефакта. — ✅ **закрыт
   вариантом (в), но как явный флаг:** `hound package add-product --apply --passport`
   (`core/cli/package/parser.py:705`) вызывает `write_passport_draft`
   (`core/pipeline/package_add_product.py:1550-1558`) и пишет `passport_written` в
   review-артефакт. Требования «паспорт обязателен для eval» **нет** — это остаётся
   решением человека, а не гейтом.
9. **Bootstrap/adoption.** Явная `state adopt` (рекомендуется, §4.6) vs авто-принятие
   последнего complete-прогона при первом `--update` (`--adopt-latest`). Явная команда
   сохраняет контроль над baseline, но требует шага в runbook. — ✅ **закрыт решением
   29.10.2026: явная `hound package state adopt`** (§4.6, реализовано 29.10.2026);
   `--adopt-latest` отклонён как скрывающий выбор baseline
10. **Partial-run policy.** Текущий дизайн блокирует весь run, если есть продукт с
    `pending_seeds` (O1). Альтернатива — обрабатывать готовые продукты, а неготовые
    помечать `blocked` в отчёте (частичный результат). Первое честнее для когорты
    (не публиковать неполный срез), второе — операционно удобнее. Рекомендуется первое
    для `--update` и второе для `--only-product`. — ✅ **закрыт целиком 29.10.2026**:
    первая часть — префлайт блокирует прогон целиком (§6.1); вторая — `run_scope`
    персистится в `runs`/манифест, а политика `--only-product` остаётся «обработать
    выбранный продукт, baseline не двигать» (§13.6, граница A). Открытых вопросов в §18
    не осталось.

---

## 19. Приложение: псевдокод `run --update` (**design-only**) — ⬜ не сделано

> **Статус: design-only.** Это эскиз целевого потока, а не описание `cmd_run`. Часть
> сущностей в коде **есть** и названа правильно (`resolve_cycle_id`/`resolve_run_scope` —
> `core/cli/commands/run.py`, `reconcile_seed_set`/`record_run_lineage` —
> `core/ports/state.py`), а часть — только намерение: `state.lock()` (файловый lock пакета
> **есть** как `core/util/package_lock.py` и берётся CLI-слоем, а не портом стора — §4.4),
> `RunLineage`-вызов `record_run`, `finalize_products` (агрегат статусов продукта —
> снимок в `state.db`, §13.1). Читать как намерение; фактический composition root —
> `core/cli/commands/run.py`.

```python
def cmd_run_update(args):
    inputs = load_inputs(args.run_yaml, args.package)          # pure
    state = SqliteProductState(paths.state / inputs.run.package / "state.db")
    # эксклюзивность пакета держит CLI-лок state/{pkg}/.lock (§4.4); busy_timeout — носитель
    prev = state.previous_run(inputs.run.package) if not args.resume else None
    cycle_id = resolve_cycle_id(args, inputs, prev)            # без неявного today()
    seeds = load_effective_seeds(args, inputs.package)         # --urls > seeds.yaml > corpus_urls
    reconcile_seed_set(state, seeds, actor=args.actor)
    cohort_digest = derive_cohort_digest(inputs.cohort)
    seeds_digest = derive_seeds_digest(seeds)
    contract_digest = execution_contract_digest(inputs, plan_axes=..., cohort_digest=cohort_digest,
                                                seeds_digest=seeds_digest)
    run_scope = resolve_run_scope(args, prev)                  # full|only_failed|product:<id>
    change_class = classify_change(prev, cohort_digest, seeds_digest, contract_digest)
    run_id = args.run_id or resolve_run_id(
        args, inputs, prev, cycle_id=cycle_id, cohort_digest=cohort_digest,
        seeds_digest=seeds_digest, execution_contract_digest=contract_digest,
        run_scope=run_scope,
    )
    plan = plan(inputs, run_id=run_id)
    product_plan = resolve_product_modes(
        inputs.cohort, seeds, state, load_domain_profiles(inputs.package),
        cycle_id=cycle_id,
        previous_run_id=prev.run_id if prev else None,
        change_class=change_class,
        scope=run_scope,
    )
    store = build_store(run_db_path(args.runs_dir, run_id))
    ensure_run(store, run_id, inputs)
    snapshot_run_inputs(store, run_id, inputs, plan, product_plan,
                        cycle_id, prev, change_class, run_scope, contract_digest)
    state.record_run_lineage(RunLineageRow(run_id, ..., previous_run_id=prev.run_id if prev else None,
                                          change_class=change_class, status="running"))
    phases = build_phases(
        inputs, plan, product_plan=product_plan, state=state,
        refresh=args.refresh, urls=effective_urls(seeds),
        composition_factory=CompositionView,
        previous_run_store=open_run_store(prev.run_id) if prev else None,
    )
    report = Runner(store, phases).run(run_id)
    finalize_products(store, state, run_id, product_plan, report)
    return report
```

**`resolve_cycle_id`.** `--cycle-id` > `run.yaml:cycle_id` > `run.yaml:snapshot_date`.
`--update` требует **нового** значения: если эффективный `cycle_id` совпадает с
`prev.cycle_id`, это `EXIT_USAGE` («новый цикл требует нового cycle_id»), а не молчаливый
resume. Без `--update` `snapshot_date` — стабильный fallback, поэтому retry после падения
resume'ит тот же `run_id`.

**`resolve_run_id`.** `--run-id` > resume незавершённого прогона того же
`cycle_id`/`cohort_digest`/`seeds_digest` (тот же `run_id`, `--only-failed` сужает work-set)
> `derive_run_id(..., run_scope, refresh_policy)`. `--update` + `--only-failed` —
`EXIT_USAGE` (новый цикл и ремонт прошлого несовместимы).

---

*Документ гибридный: as-built-секции (§6.1–§6.3, §6.5–§6.9, §7, §8, §12.1–§12.2,
§12.5, §20) описывают то, что работает в прогоне; design-only-секции (§2.3, §4.5, §4.6,
§6.4, §10, §11 в части `freshness`, §13.3, §14, §19) — намерение без производителя.
Статус — в маркере заголовка каждой секции и в реестре в шапке. Реализация design-части —
отдельными атомарными коммитами по этапам §16; изменения §13/§4.4 — только после явного
решения владельца методологии.*

---

## 20. Приложение: benchmark моделей оценщика (`experiment evaluator-benchmark`, A3)

> Приложение к документу онбординга: здесь описано измерение, которым **заменяется дефолт
> модели** сценария `evaluator` (A3: замена — по измерению, не по предвидению). Сам
> benchmark не меняет методологию: он меряет модель на уже зафиксированных условиях
> прогона.

### 20.1. Контракт измерения: что фиксировано

`hound experiment evaluator-benchmark` меряет кандидатов на **текущем прогоне после
индексации** (чанки уже в `corpus_chunks` + `index_manifest`). Всё, кроме модели,
фиксировано и **проверяется**, а не предполагается:

| Условие | Источник | Почему |
|---|---|---|
| индексная сборка/композиция | `runs.index_build_id` + `index_manifest` | I17: evidence — только чанки продукта; сравнение на разных сборках бессмысленно |
| `retrieval_profile`, `retrieval_cutoff_K`, `rrf_k` | строка прогона (`runs`/манифест), не выбор эксперимента | I16/I20: профиль и K — условия измерения, а не ручка benchmark'а |
| срез чанков ячейки | `retrieve_for_cell` **один раз** до моделей | модели видят байт-в-байт один и тот же вход; ретривал не повторяется на модель |
| промпт | `TEMPLATE_VERSION` (`core/pipeline/prompt.py`) | одинаковый шаблон; одинаковость входа доказывается совпадением `prompt_hash` по всем моделям |
| `seed` / `temperature` | `0` / `0.0` | детерминизм: greedy + фиксированный seed (§7.1) |
| `max_retries` / `strict` | `2` / `True` | регламент §7.1; повторы входят в цену и в `first_attempt_pass` |
| golden | `golden.csv` прогона | один и тот же эталон для всех моделей |

`store=None` у `evaluate_cell`: это **измерение, а не прогон** — ячейки и evidence не
персистятся, Ф0-покрытие/прецедент не применяются. Нарушение любого условия (иной
`seed`/`temperature`, `web_tool=True`, расхождение `prompt_hash` между моделями) —
отказ `BenchmarkContractError`, а не «почти сравнили».

Модель объявляется осями в форме `DeclaredExecutionAxes.llm_scenarios`: `model_id`,
`model_version`, `endpoint_kind`, `decoder`, `web_tool: false` (I18 — структурно: у
`EvaluatorPort` нет инструментов), плюс наблюдаемые `seed`/`temperature` порта. Источник
`model_id` — объявленный вход (`--llm-model` > `deploy.llm.<slot>.model_id`), а не env
`COHOUND_LLM_MODEL`: последняя действует только на LLM-разведку, чей выход — кандидаты
(G2), а не evidence (§7.4/I21, `03` §D.6).

### 20.2. Метрики и их определения

* **`validity_rate`** — доля ячеек с `validation.ok=True` (эшелон §10: схема + I1/I7/I22 +
  цитата-подстрока; переиспользуется как есть, не переписывается).
* **`failure_kinds`** — гистограмма `failure_kind`: `validation_failed` /
  `evaluation_failed` / `none`; отдельным ключом `harness_error` — сбой самого харнесса
  (изолирован и назван, а не выдан за свойство модели).
* **`agreement`** — Cohen's κ против golden `expected_status` через
  `core/util/agreement.py`. Вырожденный расчёт (`n=0` или нулевая вариация маргиналов)
  отдаётся как `kappa=None` **с причиной** — число не выдумывается. Рядом публикуется
  сырая матрица ошибок (status × status): κ скрывает направление ошибки, матрица — нет.
  κ считается по **парным** ячейкам; `failed` (`status=None`, I4) в κ не входят и видны в
  `validity`/`failure_kinds`.
* **`abstention`** — для golden `❌`: доля ответов `❌` против `❓`/`pending`/`failed`;
  отдельно для golden `✅`: **`fabricated_support`** — модель заявила ✅ с непустыми
  цитатами, ни одна из которых не подтвердилась (валидация ✅-выхода есть ровно проверка
  цитат). Это худший режим отказа: он не «ошибка схемы», а сфабрикованное доказательство,
  и считается явно, а не растворяется в agreement.
* **`first_attempt_pass`** — доля ячеек, прошедших валидацию без повтора (дисциплина схемы;
  повторы видны и в `tokens.calls`).
* **`tokens`** — суммы `prompt`/`completion` из `LLMResponse.usage`, среднее на ячейку,
  число вызовов (повторы их раздувают) и `usage_reported_cells`. Если endpoint не сообщил
  usage, в `limits` появляется явная строка: `0` не значит «бесплатно».
* **`latency_ms`** — mean/p50/p95 на ячейку (часы инъектируются, в тестах — детерминированы).
* **`determinism`** — при `--repeats N>=2`: доля ячеек, чей `status` изменился между
  повторами. Недетерминированный оценщик ломает resume и воспроизводимость, поэтому это
  first-class метрика, а не сноска. При `repeats=1` — `measured=false` (не «1.0»).
* **Неопределённость** — bootstrap-перцентильный 95 % CI (2000 ресэмплов, фиксированный
  seed) для `validity_rate` и κ. Вырожденные κ-ресэмплы исключаются, их число называется.
* **Покрытие** — `n_cells`, `n_products` и `limits`: golden-набор мал (мало продуктов и
  мало строк `❌`), поэтому вывод **не переносится** на другие домены без собственного
  измерения. Числа считаются из данных, не хардкодятся.

### 20.3. Почему модель — объявленный вход, а не env

`COHOUND_LLM_MODEL` — операционная среда **хоста**, а не вход прогона. `run identity`
(`core/cli/execution_axes.py`) намеренно игнорирует env: иначе два хоста с одним
`run.yaml` получили бы разные `run_id`, что ломает воспроизводимость и diff (§13).
Benchmark, переключающий модель через env, воспроизвёл бы ровно этот баг: один и тот же
артефакт «измерения» описывал бы разные модели на разных хостах. Поэтому `model_id`
приходит **только** из `--model` (повторяемый; дефолтный набор — данные CLI), а из
слота/env допустимы лишь `base_url`/`api_key`/`decoder`/`timeout`.

### 20.4. Классификация §13: смена модели оценщика

Смена модели сценария `evaluator` — версионная граница класса **`execution_change`**
(условия измерения, commodity-слой по A2), а **не** `methodology_version`: семантика
статусов, scoring и evidence-модель не меняются. Граница блокирует межцикловый diff с
пометкой `execution_change` и требует `golden-full` (§13, §7.4). Benchmark — то
**измерение**, которое обосновывает такую замену: он даёт validity/κ/abstention/
детерминизм/цену на одном и том же входе и с честным CI. Без него замена модели была бы
предвидением, а не измерением (A3).

Артефакт: `runs/{run_id}/benchmark/evaluator-<timestamp>.json`; `--json` печатает тот же
payload. Код возврата `0` — измерение состоялось (даже если модель плоха); ненулевой —
только при отсутствии предпосылки (нет сборки/чанков/golden) с видимой причиной.
