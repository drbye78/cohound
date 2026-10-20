# 09. Онбординг продукта и инкрементальные прогоны обновления

> **Статус:** **MVP-1, MVP-2a, MVP-2b и MVP-3a реализованы.** MVP-1 (§16.1, этап 1):
> `sources/seeds.yaml`, onboarding-префлайт, zero-source coverage. MVP-2a (часть этапа 2):
> durable `state/{package_id}/state.db` (§4.1/§4.2), `ProductStatePort`/`SqliteStateStore`,
> `resolve_product_modes` (new/full/incremental), синхронизация seed-набора и проводка в
> `run`. MVP-2b (run identity): `derive_run_id` без состава продуктов
> (§6.2: `cycle_id` + `cohort_digest` + `seeds_digest` + `execution_contract_digest` +
> `run_scope` + `refresh_policy` + focus), `--update`/`--cycle-id`/`--since-run`,
> `change_class`/`golden_required` по предыдущему lineage. MVP-3a (durable-субстрат
> инкрементального корпуса, схема `state.db` **3**): `url_state`/`document_version`/
> `chunk_store`/`chunk_meta` доступны через порт (§4.2), `replace_doc_chunks` атомарен и
> удаляет только осиротевшие тексты, миграция лечит дрейф аддитивных колонок;
> `core/pipeline/incremental.py` — `plan_incremental` (§6.3), `classify_reuse`,
> `doc_version_id`/`chunk_row_id`/`doc_set_digest`. MVP-3b (интеграция в корпус):
> `build_corpus` принимает `state`/`product_modes`/`refresh_policy`; условный GET
> (`If-None-Match`/`If-Modified-Since` из `url_state`) на локальном и executor-путях
> (валидаторы `etag`/`last_modified`/`not_modified` больше не теряются на шве);
> `incremental`-продукт получает known-URL из `plan_incremental` как depth-0 seeds поверх
> прежнего BFS; при `revalidated` (304 или тот же `content_hash`) doc-версия и chunk-set
> переиспользуются **с пересчётом `content_hash(text)`** (I19) и без вызова чанкера;
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
> `offline`-reuse (честная ошибка + деградация до `revalidate`), расписание
> `next_check_at`/потолок ревалидации, carry-forward (этап 5) — design-only.
> Документ описывает целевую архитектуру сценария «`add-product` → следующий прогон
> обновления». Он не меняет регламент `cohound-spec-v2.md`; там, где дизайн затрагивает
> семантику §13/§4.4, это явно помечено как **решение человека** (см. §12.6 и §17).
>
> **Отклонения реализованного среза от целевого дизайна** (осознанные, до этапов 3–5):
> `execution_contract_digest` пока собирается из честно доступных значений run-слоя
> (methodology/package/cohort/seeds/retrieval); версии краула, чанкера, экстрактора,
> эмбеддера, лексики, реестров, deploy-профиля и seam-схемы хешируются как `null` до
> соответствующих срезов; review-артефакт именуется
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
> `expected_sources > 0` (§3.3/O2). Оба — `methodology_change`: `METHODOLOGY_VERSION`
> 2.2.0 → 2.3.0, `golden-full` + одна пере-аннотация golden.
>
> **Рамки:** `core/` остаётся категорийно-нейтральным; вся предметная специфика — данные
> пакета (`packages/{case_id}/`). Слои: `pipeline ↛ adapters/store/cli`; `index_store/`
> иммутабелен; только публичные сайты, robots, без обхода CAPTCHA/авторизации.

---

## 1. Контекст и проблема

### 1.1. Что происходит сегодня

Сценарий «добавили продукт в пакет» разрывается в четырёх местах.

1. **`add-product` не пишет seed'ы.** `apply_add_product`
   (`core/pipeline/package_add_product.py:1229-1337`) пишет `cohort.yaml`, `run.yaml`,
   `domain_profiles.yaml`, review-артефакт `sources/add_product/{date}-{product_id}.json` и опционально
   паспорт. Ни `sources/corpus_urls.yaml`, ни `crawl_sources.yaml` не обновляются.
2. **`run`/`crawl` берут URL только у оператора.** `--urls`
   (`core/cli/commands/run.py:171`, `core/cli/commands/crawl.py:283-284`) — единственный
   источник `url_map`. `domain_profiles.yaml` лишь резолвит оси краула для уже переданных
   seed'ов (`core/pipeline/corpus.py:2003`), но не разворачивается в seed'ы.
3. **Новый продукт даёт пустой ingest без аномалии.** `build_corpus` итерирует
   `plan.products`, но URL берёт как `url_map.get(product_id, [])`
   (`corpus.py:1333-1334`, `3553`). `_persist_corpus_coverage` пишет покрытие только по
   продуктам, у которых есть элементы сеанса (`corpus.py:363-373`), поэтому для продукта
   без URL строки покрытия нет; `_coverage_for` возвращает `None`
   (`evaluate.py:355-370`, `1021`), и Ф0-гейт неполного корпуса не срабатывает. Продукт
   оценивается на пустом корпусе — «тихий пустой продукт».
4. **Добавление продукта обнуляет переиспользование.** `derive_run_id`
   (`core/pipeline/plan.py:72-90`) включает `sorted(run.products)`: новый состав когорты →
   новый `run_id` → новый `runs/{run_id}/cohound.db`, новый сеанс `crawl:{run_id}`; старые
   `phase_state`, `crawl_session_items`, `corpus_docs`, `corpus_coverage` не переиспользуются.
   Вдобавок `index_build_id` — отпечаток **всех** продуктов батча
   (`corpus.py:40-48`, `793-865`), поэтому добавление продукта пересобирает все партиции.

### 1.2. Почему это не лечится точечно

- Недостаточно «дописать seed'ы в `corpus_urls.yaml`»: нет durable-состояния продукта между
  прогонами, нет определения «новый vs существующий», нет переиспользования документов,
  чанков, эмбеддингов, партиций и ячеек.
- Недостаточно «переиспользовать старый `run_id`»: `run_inputs` — `INSERT OR REPLACE`
  (`sqlite_store.py:738-761`), а снимок `run.yaml` при отличии не перезаписывается
  (`context.py:608-618`) → расхождение провенанса.
- Недостаточно «сделать `index_build_id` по продукту»: retrieval сейчас конструируется на
  **один** `build_id` (`NpyVectorIndex`, `core/adapters/vector_npy.py:75-160`;
  `seed_lexical_index`, `core/cli/wiring.py:283-312`), поэтому нужна композиция партиций.

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
- участвует в выборе `resolved_url` при неоднозначности (`package_add_product.py:371-474`);
- становится **одним** seed'ом с `role: primary`, но не отменяет discovery других сайтов;
- не считается «единственным и приоритетным сайтом»: все валидные сайты из `plan.sites`
  попадают в seed-набор.

**Что пишет `--apply` (список — не порядок; порядок записи — в «Атомарность» ниже).**

1. `sources/seeds.yaml` — новый канонический артефакт (см. §7). Merge + дедуп по
   нормализованному URL; существующие seed'ы не затираются. Если файла ещё нет —
   `ensure_seeds_file` импортирует **все** записи `corpus_urls.yaml` (миграция, §7.5),
   чтобы старые продукты не потеряли seed'ы.
2. `domain_profiles.yaml` — как сегодня (`write_domain_profiles_with_new_keys`,
   `package_add_product.py:1192-1226`). Каждый seed обязан резолвиться в существующий или
   только что добавленный profile-key.
3. `package.yaml` — `package_version` bump (minor) через существующий bump-механизм;
   иначе diff не увидит авторское изменение пакета. Bump выполняется **до** записи
   `cohort.yaml`/`run.yaml`: если bump упал, когорта ещё не изменилась.
4. `cohort.yaml` — через `apply_candidate` (`package_add_product.py:1264-1282`), **плюс**
   новая ревизия когорты: `frozen_at = snapshot` (дата онбординга) и новый `cohort_ref`
   (например `cohort-{case}-{snapshot}-r2`) в `run.yaml`. Без новой ревизии
   `run` отказывает `cohort_expansion` как нарушению §13.
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
**Проекция `run.yaml`.** `patch_run_projection` (`package_io.py`) должен уметь патчить не
только `anchor`/`products`, но и `cohort` (и `cycle_id`, если он появляется в `run.yaml`):
иначе `update_cohort` оставит в `run.yaml` старый `cohort_ref`, и следующий `run` откажет
`cohort_expansion`. Патч — построчный, с сохранением комментариев; полная запись —
`exclude_none=True`.

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

Следующий `hound run --update` в фазе `onboarding` возвращает `blocked` с
`reason = "product_not_ready"`, перечисляя продукт и что именно отсутствует
(`no_seeds`, `profile_unbound`, `seed_invalid`). Отчёта/ячеек для такого прогона нет.

> Почему не «отказать и не добавлять»: требование — «следующий run должен блокироваться».
> Если продукта нет в когорте, `run` о нём не знает и не заблокируется. `pending_seeds`
> делает неготовность **явной и durable**.

### 2.3. Управление seed'ами

Новые подкоманды `hound package seeds`:

| Команда | Назначение |
|---|---|
| `seeds list --package DIR [--product ID] [--json]` | read-only: URL, role, source, enabled, last_verified, profile_key |
| `seeds add --package DIR --product ID --url URL [--role ...] [--profile-key ...] --actor ...` | авторская запись; валидация SSRF/scope/дедуп |
| `seeds remove --package DIR --product ID --url URL --actor ...` | soft-disable (`enabled: false`), не физическое удаление |
| `seeds validate --package DIR [--json]` | SSRF, схема, дубликаты, привязка к `domain_profiles.yaml` |
| `seeds status --package DIR [--json]` | onboarding-состояния продуктов (`ready`/`pending_seeds`/`blocked`) |

`seeds add` без `--profile-key` привязывает seed к самому длинному подходящему ключу
`host[+path]`; если ключа нет — создаёт минимальный профиль (`extract: auto`, `fetch: auto`,
без `wait_for`/`rate_limit`) и пишет warning. Синтез `wait_for`/`rate_limit` запрещён
(они измеряются, A3).

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
  `--cycle-id` **обязателен**, если в `run.yaml` нет `cycle_id`: иначе `date.today()` сделал
  бы retry следующего дня новым прогоном. Эффективный `cycle_id` фиксируется в снимке
  прогона (`runs/{run_id}/run.yaml` + `run_inputs`), но **не** переписывает
  `packages/{case}/run.yaml` (авторский артефакт не мутируется прогоном).
- **`--since-run`** — явный predecessor для carry-forward. По умолчанию — последний
  **завершённый** прогон пакета из `state.db` (`run_lineage.status in (complete, partial)`).
  Для resume текущего прогона predecessor — он сам, carry-forward не выполняется.
- **`--refresh`**:
  - `auto` (дефолт): `new|full` для новых/неполных продуктов, `revalidate` для существующих;
  - `revalidate`: условный GET по сохранённым `ETag`/`Last-Modified`; 304 или тот же
    `content_hash` → переиспользование документа;
  - `refresh`: безусловный GET, content-hash дедуп сохраняется;
  - `offline`: без сети, только retained raw; `freshness = stale`, `❌` запрещён (см. §11.4).
- **`--only-failed`** — теперь **межпрогонный**: читает `run_product`/`url_state`
  последнего прогона (или `--since-run`) и повторяет продукты/URL со статусом
  `failed|partial`. В отличие от текущей реализации (`context.py:1118-1136`), не ограничен
  текущим сеансом. Семантика зависит от контекста:
  - если последний прогон **не завершён** (есть `running`/`aborted`) — это resume того же
    `run_id`; `Runner` пропускает `ok`-фазы, `--only-failed` сужает work-set;
  - если последний прогон **завершён** — это новый scoped-прогон: `run_id` включает
    `run_scope=only_failed` (см. §6.2), `previous_run_id` = завершённый прогон;
    иначе тот же `run_id` короткозамкнул бы все фазы.
- **`--only-product`** — фильтр объёма; прогон помечается `scope = product_only` и **не**
  обновляет `last_completed_run_id` продукта (частичное измерение не становится baseline).
  `run_id` включает `run_scope=product:<id>`; без этого scoped-прогон совпал бы с полным
  `run_id` и resume пропустил бы фазы.
- **`--reset`** — сбрасывает **прогон** (сессию/фазы), но **не** durable state. Для
  разрушительного сброса — отдельные команды `hound package state forget-product` /
  `state rebuild` (позже; в MVP — ручное удаление `state/{package}/state.db` с бэкапом).

> **`--update` vs идентичность прогона.** `run_id` меняется при любом изменении входов
> (`cohort_digest`/`seeds_digest`/`focus`/`cycle_id`), даже без `--update`: иначе нельзя
> гарантировать провенанс. `--update` — это семантическое объявление нового **цикла**
> (новый `cycle_id`, `previous_run_id` = последний завершённый прогон, `change_class`).
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

### 3.1. Новые перечисления (`core/domain/enums.py`, `StrEnum`)

| Enum | Значения | Смысл |
|---|---|---|
| `ProductMode` | `new`, `full`, `incremental` | режим обработки продукта в прогоне |
| `OnboardingState` | `pending_seeds`, `ready`, `blocked`, `retired` | готовность продукта к измерению |
| `SeedRole` | `primary`, `docs`, `satellite`, `host_root` | роль seed'а в корпусе продукта |
| `SeedState` | `enabled`, `disabled`, `invalid` | операционное состояние seed'а |
| `ReuseKind` | `live`, `revalidated`, `reused`, `stale` | как получен документ в этом цикле |
| `CoverageFreshness` | `live`, `revalidated`, `stale` | свежесть наблюдения корпуса |
| `ChangeClass` | `none`, `content_change`, `cohort_expansion`, `cohort_reduction`, `execution_change`, `methodology_change` | класс изменения между прогонами |
| `RefreshPolicy` | `auto`, `revalidate`, `refresh`, `offline` | политика обновления |
| `MeasurementOrigin` | `observed`, `carried` | ячейка измерена или перенесена |
| `RunProductStatus` | `pending`, `running`, `complete`, `partial`, `failed`, `blocked` | статус продукта в прогоне |

Источник истины — `docs/status-dictionary.yaml`; перечисления добавляются туда и в схему
зеркала. Все — `StrEnum`, без строковых литералов в коде.

### 3.2. Новые доменные модели (`core/domain/models.py`, `extra="forbid"`)

- `SeedEntry(url, normalized_url, role, source, source_ref, profile_key, added_at, added_by,
  enabled, last_verified_at, state)`.
- `SeedsConfig(schema, products: dict[str, ProductSeeds])`.
- `ProductPlan(product_id, mode, onboarding_state, seed_count, last_completed_run_id,
  last_corpus_run_id, last_evaluated_run_id, refresh_policy, scope, reasons: list[str])`.
- `ProductPlanSet(cycle_id, previous_run_id, change_class, products, blocked)`.
- `DocumentVersion(doc_version_id, doc_id, product_id, url, content_hash, extractor_version,
  level, raw_path, chunker_version, chunk_ids_digest, first_seen_run_id, last_seen_run_id,
  status)`.
- `UrlState(product_id, url, normalized_url, doc_version_id, etag, last_modified,
  last_checked_at, last_changed_at, next_check_at, consecutive_failures, last_error_kind,
  reuse_kind, seed_id)`.
- `ProductState(product_id, canonical_name, vendor, onboarding_state, onboarding_reason,
  added_at, added_by, re_added_at, retired_at, last_corpus_run_id, last_evaluated_run_id,
  last_completed_run_id, last_completed_cycle_id, last_attempt_run_id, last_attempt_status)`.
- `ProductSnapshot(snapshot_id, product_id, cycle_id, run_id, status, scope,
  coverage_complete, coverage_digest, doc_set_digest, partition_build_id, docs_total,
  docs_reused, docs_new, cells_total, cells_carried, cells_measured, cells_pending,
  baseline_complete, created_at)`.
- `CellMeasurement(run_id, product_id, param_id, state, status, prompt_hash, model_id,
  llm_scenario, index_build_id, partition_build_id, doc_set_digest, coverage_digest,
  catalog_row_digest, cell_reuse_key, evidence_digest, measured_at)`.
- `CompositionMember(product_id, partition_build_id, content_digest, chunks, dim)`.
- `IndexComposition(composition_id, package_id, members, created_at)`.
- `RunProduct(run_id, product_id, mode, status, phase, coverage_complete, docs_total,
  docs_reused, docs_new, cells_total, cells_carried, cells_measured, cells_pending,
  error_kind, error)`.

### 3.3. Новые инварианты (O-серия)

| ID | Инвариант | Где обеспечивается |
|---|---|---|
| **O1** | Каждый продукт когорты имеет ≥ 1 `enabled` seed **или** `onboarding_state=pending_seeds`; `run` с `pending_seeds` блокируется | `onboarding` фаза + `lint` |
| **O2** | Продукт с `mode in (new, full)` не может завершить corpus с `expected_sources = 0`; это `failed`, не пустой `ok`. Сильнее: `complete` требует `observed_sources > 0` — корпус без единого наблюдения не доказывает отсутствия (2.3.0) | `build_corpus` |
| **O3** | Для каждого продукта плана в прогоне существует строка `corpus_coverage`; отсутствие — аномалия, не «неизвестно» | `corpus` + `evaluate` |
| **O4** | `doc_version_id` детерминирован от `(product_id, content_hash, extractor_version, level)`; `chunk_id` — от `(product_id, doc_version_id, chunker_version, chunk_no)` | `corpus` |
| **O5** | Любая evidence-строка несёт `doc_version_id`, и этот документ входит в doc-set продукта текущего прогона (I19/I17) | `evaluate`/`carry_forward` |
| **O6** | `carried`-ячейка допустима только при совпадении `cell_reuse_key`, полном покрытии и неизменном doc-set | `carry_forward` |
| **O7** | `❌` публикуется только при `coverage.complete and freshness != stale` (или человеческой аттестации) | `evaluate` |
| **O8** | `index_build_id` прогона = `composition_id`; каждая партиция иммутабельна и адресуется `partition_build_id` | `composition`/`index_npy` |
| **O9** | Добавление/удаление продукта меняет `cohort_digest` и `change_class ∈ {cohort_expansion, cohort_reduction}` | `plan`/`report` |
| **O10** | `last_completed_run_id` обновляется только при `baseline_complete=True` (coverage complete, eval прошёл, `cells_pending == 0`) и `scope=full`; `last_corpus_run_id`/`last_evaluated_run_id` обновляются и для scoped-прогона (корпус/оценка продукта действительно выполнены) | `report`/`state` |
| **O11** | `run_inputs` прогона иммутабельны: повторная запись с другим `payload_sha256` — ошибка, не `REPLACE` | `context.py` |
| **O12** | Продукт без `last_corpus_run_id` — `new`; с `last_corpus_run_id`, но без `last_evaluated_run_id` — `full`; с `last_evaluated_run_id` и полным snapshot `coverage_complete=True` — `incremental` (pending-ячейки не мешают) | `onboarding` |
| **O13** | Человеческие решения (`reviewed`, `N/A`, `scope_attested_by`) не переносятся неявно: carry-forward разрешён только при совпадении `attestation_digest`, а `reviewed`/`N/A` без действующей аттестации переоцениваются | `evaluate` |

Связь с регламентом: O1–O3 закрывают «тихий пустой продукт» (I5/I11-честность); O4–O6
усиливают I17/I19; O7 сохраняет I2c; O8 — §13/иммутабельность; O9–O10 — §13; O11 —
провенанс R7; O12 — A3 (не выдавать частичное за полное).

---

## 4. Durable per-product state

### 4.1. Где хранить

**`state/{package_id}/state.db`** — отдельная SQLite-БД на пакет, вне `packages/` (авторские
данные не смешиваются с операционным состоянием) и вне `runs/{run_id}/` (переживает прогоны).

Раскладка (`core/config/paths.py`): новое поле `state: Path = Path("state")`, env
`COHOUND_STATE_DIR`, приоритет как у остальных полей. Файл:
`state/{package_id}/state.db` (+ `state.db-wal`, `state.db-shm`).

**Почему per-package, а не глобальный `history.db`:**

- blast radius: повреждение/миграция одного пакета не трогает другие;
- нет cross-package блокировок и «общего» `product_id`-пространства (I17: продукт-скоупед);
- бэкап/restore — вместе с пакетом (`backup.py` расширяется);
- категорийная нейтральность: `core/` не знает имён пакетов, путь — данные.

**Глобальными остаются:** `corpus_raw/{product_id}/` (content-addressed байты),
`cache/http/`, `extract_cache/`, `eval_cache/`, `index_store/{package}/`.

### 4.2. Схема `state.db`

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

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
    error             TEXT
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

CREATE INDEX idx_seed_product ON seed_set(product_id, state);
CREATE INDEX idx_doc_product  ON document_version(product_id, status);
CREATE INDEX idx_snapshot_product ON product_snapshot(product_id, created_at);
CREATE INDEX idx_cell_measure_product ON cell_measurement(product_id, run_id);
CREATE INDEX idx_partition_product ON index_partition(package_id, product_id);
```

### 4.3. Что остаётся в `runs/{run_id}/cohound.db`

- `runs` (+ новые поля `cycle_id`, `previous_run_id`, `cohort_digest`, `seeds_digest`,
  `execution_contract_digest`, `change_class`, `product_plan_json`).
- `run_product` — per-product статус прогона.
- `corpus_docs` / `corpus_chunks` — **самодостаточный срез** документов/чанков, реально
  измеренных в этом прогоне (копируются из state.db для reused-документов; текст — из
  `chunk_store`). Это сохраняет self-containment прогона и работу `iter_chunks`.
- `index_manifest` — по строке на продукт: `(index_build_id=composition_id, product_id,
  partition_build_id, ...)`.
- `cells`, `cell_evidence`, `corpus_coverage`, `search_log`, `gate_decisions`, `run_inputs`.

> Дублирование текста чанков в run DB — осознанная плата за self-containment (бэкап/restore,
> пост-хок команды). Экономия достигается не на записи в БД, а на сети/экстракции/эмбеддингах/LLM.

### 4.4. Транзакционность и восстановление

- `document_version`, `chunk_meta`, `chunk_store`, `url_state` пишутся **до** фиксации
  партиции (сначала диск, потом БД — как сейчас для индекса).
- `product_snapshot` и `last_corpus_run_id`/`last_evaluated_run_id`/`last_completed_run_id`
  пишутся **после** соответствующего этапа:
  - `last_corpus_run_id` — после успешной сборки/регистрации партиции;
  - `last_evaluated_run_id` — после `evaluate` продукта (даже если часть ячеек `pending`);
  - `last_completed_run_id` — после `evaluate` + `coverage` + `report` продукта **и**
    `cells_pending == 0` (полный baseline).
- Падение между этапами: state.db может содержать «осиротевшие» версии/партиции — они
  content-addressed и безопасны; следующий прогон либо переиспользует их, либо создаст новые.
  `run_lineage.status = aborted` фиксируется при старте следующего прогона (recovery).
- **Concurrency:** файловый lock `state/{package_id}/.lock` (exclusive) на время прогона;
  второй `run` на тот же пакет завершается `EXIT_USAGE` с понятной ошибкой. SQLite WAL —
  дополнительная защита на уровне БД, но lock нужен, чтобы не гонять два краула.

### 4.5. Retention

| Данные | Политика |
|---|---|
| `corpus_raw/{product}/{sha256}` | retention по `crawl.retention_days`/бюджету; при вытеснении `document_version.raw_path = NULL`, `status = evicted` |
| `document_version`, `chunk_store`, `chunk_meta` | бессрочно (метаданные и текст — субстрат evidence; I19) |
| `product_snapshot`, `cell_measurement`, `run_lineage` | бессрочно (история измерений) |
| `index_partition` / `index_store` | иммутабельно; GC только явным вызовом `gc_index_builds` (проверяет, что сборка не входит ни в одну композицию пакета) и `gc_index_compositions` (§8.6) |
| `eval_cache`/`extract_cache`/`cache/http` | существующие бюджеты/eviction |

Если raw вытеснен, а документ нужен для carry-forward: evidence переносится только если
`chunk_store` содержит текст и `content_hash` совпадает; иначе ячейка **не** переносится,
продукт получает `full`-режим (нельзя честно утверждать, что документ не изменился).

### 4.6. Bootstrap: принятие уже существующих прогонов

Без этого шага первый `run --update` после появления `state.db` увидит у всех старых
продуктов `last_corpus_run_id = NULL` и сделает их `new` — то есть полный краул всей
когорты. Это прямо противоречит требованию «старые продукты обновляются инкрементально».

**Правило.** `state.db` — производное хранилище; его можно построить из существующих
`runs/{run_id}/cohound.db` + `index_store/`. Принятие (**adoption**) выполняется один раз:

```
hound package state adopt --package <pkg> [--from-run <run_id>] [--actor <human>] [--dry-run]
```

- По умолчанию выбирается **последний прогон пакета**, у которого `runs.status` (или
  отчёт) = `complete` **и** для продукта есть `corpus_coverage.complete = true`.
  Частичный/убитый прогон (p7) не принимается: его `corpus_docs` могут быть неполными,
  а `corpus_chunks`/`index_manifest` — отсутствовать.
- Принятие **per-product**: продукт без подходящего завершённого прогона остаётся
  `new`. Один плохой прогон не блокирует adoption остальных.
- Из прогона переносятся:
  - `corpus_docs` → `document_version` (`doc_version_id = H(product_id, content_hash,
    extractor_version, level)`) + `url_state` (`etag`/`last_modified`, если сохранены;
    иначе `NULL` — первый incremental сделает безусловный GET и сравнит `content_hash`);
  - `corpus_chunks` → `chunk_store`/`chunk_meta` (текст есть только у прогонов, дошедших
    до Pass B; иначе чанки будут пересобраны из `document_version`);
  - `index_store/{package}/{build}/{product}` → `index_partition` (content-addressed,
    путь и `content_digest` пересчитываются и сверяются);
  - `cells`/`cell_evidence` → `cell_measurement` **только если**
    `runs.methodology_version == current METHODOLOGY_VERSION`; иначе ячейки не
    принимаются (корпус переиспользуется, оценка — заново). Это защищает от переноса
    вердиктов, посчитанных по старым правилам;
  - `product_snapshot` (`status=complete`, `scope=full`) и `run_lineage`.
- `attestation` переносится только при совпадении `doc_set_digest`/`coverage_digest`;
  иначе `superseded_at` и `❌` → `pending`.
- Adoption идемпотентен: повторный запуск обновляет/дополняет, не дублирует.
- В `run --update` без adoption: если `state.db` пуст, а завершённые прогоны есть, —
  **warning** `state_not_adopted` и явная подсказка команды; молчаливый полный краул
  допустим, но должен быть виден в отчёте (`mode=new`, `reason=no_state`).
- `add-product` **не** пишет `state.db` (это операционное состояние, а не авторский
  артефакт). Продукт материализуется в state при первом `run`, который его увидел.

> Альтернатива «автоматически принимать последний complete-прогон при первом `--update`»
> удобнее, но скрывает выбор baseline. Явная команда + warning — честнее; автоматизацию
> можно добавить флагом `--adopt-latest` позже.

---

## 5. Определение «новый продукт»

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
Добавьте seed: hound package seeds add --package ... --product acme-db --url ... --actor ...
```

---

## 6. Алгоритм run

### 6.1. Порядок фаз

```
normalize → lint → plan → onboarding → g1 → discovery → g2 → registry
→ inheritance → corpus → evaluate → review → g3 → compute → g4 → report
```

`onboarding` добавлена между `plan` и `g1`:
- `plan` строит `RunPlan` (чистый, без I/O) и `cohort_digest`/`seeds_digest`;
- `onboarding` читает `state.db` + `seeds.yaml`, вычисляет `ProductPlanSet`, пишет
  `run_product` и `runs.product_plan_json`, блокирует неготовые продукты;
- `g1` утверждает уже готовую когорту (человек видит, какие продукты new/full/incremental).

`Runner.run` (`runner.py:402-421`) не меняется: `blocked` останавливает прогон.

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
- `seeds_digest` = `H(sorted (product_id, normalized_url, role, profile_key, enabled))`.
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

### 6.3. Corpus: new/full vs incremental

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

        if all_docs_reused(product) and partition_exists(product):
            partition_id = state.last_snapshot(product, scope="full").partition_build_id
        else:
            partition_id = build_partition(product)    # embed + write
        state.register_partition(product, partition_id)
        state.set_last_corpus_run_id(product, run_id)

    composition = compose(plan.products, state)         # cb_<hash>
    save_composition(composition)
    return PhaseResult(status="ok", ...)
```

**`plan_incremental`.**

```python
def plan_incremental(product, roots, state, policy):
    work = []
    # 1. Seeds всегда в работе (условный GET).
    for seed in roots:
        work.append(CrawlWorkItem(product, seed.url, depth=0, root=seed))
    # 2. Известные URL: revalidate, если не истёк next_check_at.
    for u in state.iter_url_state(product):
        if policy == "offline":
            work.append(Reuse(u)); continue
        if u.next_check_at and now < u.next_check_at and u.doc_version_id:
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

**Фильтры work-set.** К known-URL применяются текущие `exclude_paths`/`exclude_documents`
и `max_documents` (per seed-root): исключённый URL не входит в `expected_sources`, а не
считается «потерянным». Для очень больших продуктов — опциональный
`crawl.revalidate_max_documents` (потолок числа условных GET на продукт за цикл);
при достижении потолка остальные URL помечаются `reuse_kind=stale` и **не** могут
поддерживать `❌` (coverage `freshness=stale`), пока не будут ревалидированы в следующем
цикле.

**Гарантия «новый продукт не потерялся».** `onboarding` уже проверил seed'ы; `corpus`
дополнительно (O2) не возвращает `ok` продукту с `expected_sources = 0`. `evaluate` (O3)
не оценивает продукт без строки `corpus_coverage`.

### 6.4. Evaluate

```python
def evaluate_phase(ctx):
    for product in plan.products:
        rp = run_product(product)
        if rp.status in ("failed", "blocked"):
            continue
        coverage = store.read_corpus_coverage(run_id, product)
        if coverage is None:
            # O3: аномалия, не «неизвестно»
            emit_failed_cells(product, failure_kind="coverage_missing")
            continue
        if product_plan[product].mode == "incremental" and coverage.complete:
            carried = carry_forward_cells(product, coverage)
        else:
            carried = []
        evaluate_missing_cells(product, skip={c.param_id for c in carried})
```

- Новый продукт: `carried = []`, полный набор ячеек `plan.params × product`.
- Существующий: carry-forward (см. §10), затем — только оставшиеся ячейки.
- `failed`-ячейки (I4) — `status = null`, не статус.
- Продукт с `coverage is None` или `expected_sources = 0` — `failed`-ячейки с
  `failure_kind ∈ {coverage_missing, no_sources}`, **не** `❌`/`❓`.

### 6.5. Статус прогона

`run_product.status`:
- `complete` — corpus + coverage complete и eval-фаза прошла; **`pending`-ячейки
  допустимы** (они в очереди человеческого ревью, I6/I7, и не являются провалом
  измерения). Отдельный флаг `baseline_complete = (cells_pending == 0)` определяет,
  можно ли обновлять `last_completed_run_id`;
- `partial` — часть URL/ячеек `failed`/`preliminary`, либо coverage неполон, но продукт
  измерялся;
- `failed` — corpus/eval не состоялись (нет seed'ов, все origin'ы недоступны, coverage missing);
- `blocked` — `pending_seeds`/человеческий гейт.

Итоговый `run`:
- `blocked` — если `onboarding`/`g1`/`g3`/`g4` не пройдены;
- `partial` — если есть `run_product.status ∈ {partial, failed}`, но прогон дошёл до report;
- `complete` — все продукты `complete`.

CLI: `EXIT_BLOCKED` для `blocked`, `EXIT_ERROR` для `partial` с блокирующими сбоями,
`EXIT_OK` для `complete` (как сейчас для сеанса).

---

## 7. Seeds

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
  `hound package seeds export --format corpus-urls`.
- `build_package.py` должен либо генерировать `seeds.yaml`, либо выдавать
  `generator_drift` warning (как сейчас для `domain_profiles.yaml`,
  `package_add_product.py:1153-1154`). В MVP: `add-product` пишет `seeds.yaml`, а
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
  (`seeds validate` warning), чтобы не плодить дубликаты пагинации.

`seed_id = short_hash(product_id + "\x1f" + normalized_url, 24)`.

### 7.3. Валидация

1. `core.util.security.validate_url` (SSRF: private IP, metadata endpoints, file://, и т.п.).
2. Схема/хост/порт.
3. `profile_key` существует в `domain_profiles.yaml` **или** создаётся `add-product`.
4. Seed не выходит за scope продукта: host/path должен покрываться profile-key;
   host-root seed допустим только с `max_depth: 0` (иначе `I17`-риск: затянет чужие продукты).
5. Дубликат `(product_id, normalized_url)` — merge, не ошибка (idempotent re-add).
6. Seed не должен указывать на sitemap/feed URL — для них есть `crawl_sources.yaml`
   (`kind: sitemap|feed|channel`). `seeds validate` предупреждает.

### 7.4. Привязка к `domain_profiles.yaml`

- `profile_key` — самый длинный ключ `host[+path]`, чей host совпадает и path является
  префиксом URL.
- `add-product` уже пишет профили для обнаруженных сайтов; `plan_seeds` проставляет
  `profile_key` после `write_domain_profiles_with_new_keys`.
- `seeds add` без ключа: autobind; если нет — создать минимальный профиль с
  `extract: auto`, `fetch: auto`, без `wait_for`/`rate_limit`.
- Профиль, у которого `max_depth: 0` (host-root), не должен получать seed с глубиной > 0:
  `seeds validate` это проверяет.

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
  затем добавляет новый продукт. Отдельная команда:
  `hound package seeds migrate --from sources/corpus_urls.yaml --actor ...`.
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
- `run` блокируется в `onboarding` (O1);
- `corpus` (defense-in-depth) при пустом seed-наборе пишет coverage с
  `expected_sources=0, complete=False, reason=no_seeds` и `failed`-ячейки, если
  preflight был обойдён (unit-тест/прямой вызов).

---

## 8. Index architecture

### 8.1. Два идентификатора вместо одного

| ID | Формула | Смысл |
|---|---|---|
| `partition_build_id` (`pb_...`) | `H(_PARTITION_SCHEMA, package_id, product_id, chunker_version, embed_model_id, extractor_version, lexical_engine_version, vector_store, product_content_token)` | иммутабельная сборка **одной** продукт-партиции |
| `composition_id` (`cb_...`) | `H(_COMPOSITION_SCHEMA, package_id, sorted[(product_id, partition_build_id, content_digest)])` | композиция партиций когорты; это `runs.index_build_id` |

`product_content_token` — существующий `_product_content_token`
(`corpus.py:839-851`): sorted `(chunk_id, content_hash)`.

**Что убирается из `partition_build_id`:** `params_token` и `snapshot_date`.
Каталог/фокус не меняют чанки/векторы; `snapshot_date` — провенанс, не содержимое.
Это позволяет переиспользовать партицию при смене фокуса/каталога. (Текущий
`_derive_index_build_id` включает и то, и другое — `corpus.py:793-836`; это заменяется.)

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
  (`core/adapters/vector_npy.py:75-160`): в `search` для каждого `product_id` читает
  `self._view.build_id_for(product_id)`; `RetrievedChunk.build_id` = partition id;
  `NpyVectorIndex.build_id` = composition id (для провенанса/экспериментов).
- `seed_lexical_index` (`core/cli/wiring.py:283-312`) итерирует `view.members` и
  `store.iter_chunks(partition_id, product_id=product_id)`; `index.index(records,
  build_id=composition_id)`.
- `iter_index_manifest(composition_id)` возвращает строки с `partition_build_id`.
- I17 сохраняется: `search` фильтрует по `q.product_ids`; партиция продукта не содержит
  чужих чанков.

### 8.5. Эмбеддинги

- Партиция хранит векторы; переиспользование партиции = нет эмбеддингов.
- Для изменённого продукта: `embed_cache` (опционально, later) с ключом
  `H(embed_model_id, content_hash)`; при хите вектор берётся из кэша, эмбеддится только
  новый текст. Кэш не входит в MVP, потому что пересборка одной партиции — приемлемая цена.
- `embed_model_id` обязан включать ревизию/снапшот модели; иначе смена весов при том же
  id даст ложное переиспользование. Это требование к composition root, не к `core/`.

### 8.6. Иммутабельность и GC

- Партиция и композиция — только создание; правка на месте запрещена (K2/§13).
- **Партиции**: `gc_index_builds` (`core/store/backup.py`) удаляет сборки, не входящие ни в
  одну композицию пакета; вызывается явно из кода (портируемой CLI-поверхности у него пока
  нет — `hound package index-gc` чистит только композиции).
- **Композиции**: `hound package index-gc --package DIR [--runs-dir runs] [--apply --actor]`
  (`gc_index_compositions`). Живое множество — `runs.index_build_id` по **всем** run DB
  каталога (`runs.package_id` фильтрует; `NULL`-пакет консервативно защищает любой пакет;
  `NULL`-id — не ссылка). Отказ (`ValueError`/`code=state`) при недоказуемости: нет
  `runs_dir`/ни одной run DB, DB не читается, ссылка пакета висит в воздухе, манифест не
  разбирается. По умолчанию dry-run; удаление — `--apply --actor`.
- Операции **не сцеплены**: удаление композиции не трогает партиции (их соберёт отдельный
  `gc_index_builds`), и наоборот. Реестр `state.db.index_composition` — durable-лог
  существовавшего (и `last_used_run_id`), **не** зеркало диска; GC его не чистит.
- **Рабочая последовательность** (владелец: все прогоны, кроме новейшего, расходуемы —
  дрейф Capability делает старые прогоны нерелевантными):
  1. удалить устаревшие каталоги `runs/{run_id}/` — run DB и есть **единственная** заявка
     на композицию;
  2. `hound package index-gc --package <DIR> [--runs-dir runs] --apply --actor <NAME>` —
     прунятся композиции, чья заявка исчезла вместе с прогоном;
  3. при желании — `gc_index_builds` для партиций, ставших недостижимыми после шага 2.
  Отказ при нуле run DB — именно то, что делает переход (1)→(2) безопасным: опорожнённый
  каталог прогонов не читается как «удалить все композиции». Ретенция прогонов — **вне**
  области этой функции: GC композиций не удаляет `runs/`, он лишь читает его.
  Отдельного флага `--keep-latest`/`--ignore-runs-before` нет: у артефакта один источник
  заявки (run DB), второй критерий создал бы конкурирующий авторитет.

---

## 9. Reuse matrix

| Артефакт | Ключ | Условие переиспользования | Инвалидация |
|---|---|---|---|
| HTTP-байты | `http:{url_hash}` (`cache/http`) | URL тот же; условный GET вернул 304 или контент тот же | `--refresh`, eviction, `--from-scratch` |
| ETag/Last-Modified | `url_state(product, normalized_url)` | тот же URL | 200 с новыми валидаторами; 404 |
| Retained raw | `corpus_raw/{product}/{sha256}` | `content_hash` тот же; файл не вытеснен | retention eviction |
| Extraction | `(sha256(raw), extractor_version, level)` (`extract_cache`) | точное совпадение | смена `extractor_version`/`level` |
| DocumentVersion | `(product_id, content_hash, extractor_version, level)` | точное совпадение | новый контент/экстрактор/уровень |
| Chunks | `(doc_version_id, chunker_version)` | точное совпадение | смена `chunker_version`; новый doc version |
| Embeddings | `(content_hash, embed_model_id)` (кэш) или партиция | точное совпадение | смена `embed_model_id`/ревизии |
| Index partition | `partition_build_id` | content token + все версии совпали | любой вход сборки |
| Evaluator verdict | `eval_cache` key: rendered prompt + model config + **methodology/scoring/catalog digest** | байт-в-байт | смена промпта/модели/правил/каталога |
| Cell/evidence | `cell_reuse_key` (см. §10) | ключ совпал + coverage complete + doc-set тот же | см. §10.3 |
| Coverage | не переиспользуется | всегда пересчитывается из текущих crawl-исходов | — |
| Golden/retrieval experiments | не переиспользуется | — | — |

### 9.1. Что **нельзя** переиспользовать без пере-измерения

- Ячейки при смене `methodology_version`, `scoring_policy_version`, `status_dict_version`,
  `capability_policy`, `unknown_policy`, `partial_score`.
- Ячейки при изменении строки каталога, влияющей на оценку: `rule`, `absence_rule`,
  `param_type`, `role_binding`, `concept_terms`, `search_langs`, `importance`.
- Ячейки при смене модели/слота/декодера/температуры/seed (I21).
- Ячейки при смене `retrieval_profile`, `cutoff_K`, `retrieval_params_json` (I20).
- Ячейки при изменении doc-set продукта (даже если изменился один документ — MVP:
  переоцениваем весь продукт; later — per-cell).
- `❌` при неполном покрытии или `freshness=stale` без аттестации.
- `scope_attested_by` при изменении doc-set/coverage (аттестация привязана к корпусу).
- Coverage `complete=True` — никогда не переносится; пересчитывается.

---

## 10. Evaluation / carry-forward

### 10.1. Ключ переноса ячейки

```python
CELL_REUSE_SCHEMA = "cell-reuse-1"

def cell_reuse_key(param, product_id, plan, prev: CellMeasurement,
                   composition: CompositionView, coverage: CoverageSnapshot,
                   attestation_digest: str, state: ProductStatePort,
                   prompt_template_digest: str, model_config_digest: str) -> str:
    return short_hash("\x1f".join((
        CELL_REUSE_SCHEMA,
        product_id, param.param_id,
        catalog_row_digest(param),                 # canonical JSON строки каталога
        prompt_template_digest,                    # ТЕКУЩИЙ шаблон+rule, не prev.prompt_hash
        plan.model_id, plan.llm_scenario,          # ТЕКУЩАЯ маршрутизация (I21)
        model_config_digest,                       # decoder/temperature/seed/endpoint_kind
        plan.retrieval_profile, str(plan.cutoff_k), plan.retrieval_params_json,
        current_doc_set_digest(product_id, state), # ТЕКУЩИЙ doc-set, не prev
        coverage.digest(),                         # complete + blocked + unreadable + truncated
        plan.methodology_version, plan.status_dict_version, plan.scoring_policy_version,
        plan.capability_policy, plan.unknown_policy, str(plan.partial_score),
        attestation_digest,
        composition.build_id_for(product_id),      # partition id
    )), 24)
```

`prompt_template_digest` — отпечаток **текущего** шаблона/правила (`compiled.template_version`
+ текст rule), а не `prev.prompt_hash`: иначе смена шаблона не инвалидировала бы перенос.
`model_config_digest` — текущая конфигурация модели, а не `prev.model_id`. `doc_set_digest`
и `coverage.digest()` — текущие; предыдущие значения используются только как данные
`prev`-строки для сравнения.

`doc_set_digest` — отпечаток **всех** документов продукта, а не только retrieved-подмножества.
Это консервативно: изменение любого документа продукта инвалидирует все его ячейки.
MVP-компромисс; later — per-cell `retrieved_doc_set_digest` (после retrieval) и повторная
проверка `prompt_hash`.

> **Важно:** `coverage.digest()` и `doc_set_digest` обязаны быть **стабильными между
> прогонами** при неизменном наблюдении. В `coverage.digest()` **не входят** `computed_at`,
> `run_id`, `product_id`, `freshness` и счётчики `reused/revalidated/new` (они меняются
> от того, был ли ответ 304 или 200 с тем же телом, — это не семантическое изменение).
> Входят: `complete`, `expected`, `observed`, `blocked`, `unreadable_origins`,
> `partial_blocks`, `truncated`, `reason`. `freshness` проверяется отдельно в
> `carry_forward_cells` (`stale` ⇒ запрет переноса). Тест: «два прогона без изменений
> контента, один live и один 304 → одинаковый `cell_reuse_key`».

### 10.2. Перенос

```python
def carry_forward_cells(product_id, coverage, plan, state, store, run_id, prev_run_id,
                        composition, *, prompt_template_digest, model_config_digest,
                        attestation_digest):
    if coverage is None or not coverage.complete:
        return []
    if coverage.freshness == "stale":            # offline: нельзя публиковать ❌
        return []
    out = []
    for param in plan.params:
        prev = state.get_cell_measurement(prev_run_id, product_id, param.param_id)
        # pending переносится как pending (очередь ревью), resolved — как carried.
        if prev is None or prev.state not in ("evaluated", "reviewed", "pending"):
            continue
        key = cell_reuse_key(param, product_id, plan, prev, composition, coverage,
                             attestation_digest, state, prompt_template_digest,
                             model_config_digest)
        if key != prev.cell_reuse_key:
            continue
        cell = store.get_cell(prev_run_id, product_id, param.param_id)
        evidence = list(store.iter_evidence(prev_run_id, product_id, param.param_id))
        if not evidence_chain_valid(evidence, product_id, composition, state):
            continue
        new_cell = cell.model_copy(update={
            "run_id": run_id,
            "measurement_origin": "carried",
            "carried_from_run_id": prev_run_id,
            "cell_reuse_key": key,
            "doc_set_digest": prev.doc_set_digest,
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
        out.append(new_cell)
    return out
```

`evidence_chain_valid`:
- `ev.product_id == product_id` (I17);
- `ev.doc_version_id` входит в `document_version` продукта со `status=active`;
- `ev.content_hash == document_version.content_hash`;
- `ev.source_id` резолвится в URL (`store.resolve_source_url`);
- `ev.source_scope`/`scope_attested_by` не противоречат O7;
- `ev.index_build_id` — предыдущая композиция, но `partition_build_id` совпадает с текущей
  (партиция не менялась).

### 10.3. Когда обязательна переоценка

| Изменение | Переоценка |
|---|---|
| Новый продукт | все ячейки |
| Продукт `full` (corpus был неполным) | все ячейки |
| Изменён doc-set продукта | все ячейки продукта (MVP); later — per-cell |
| `coverage.complete = False` | все ячейки продукта (carry-forward запрещён) |
| Смена `methodology_version`/`scoring_policy_version`/`status_dict_version` | все ячейки всех продуктов |
| Смена модели/слота/декодера | все ячейки (eval cache тоже miss) |
| Смена каталога (rule/absence_rule/…) | затронутые параметры |
| Смена `retrieval_profile`/`cutoff_K`/`rrf_k` | все ячейки (I20) |
| Смена `scope_attested_by`/doc-set для `❌` | absence-ячейки |
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

### 10.5. Eval cache: исправление ключа

Текущий `EvalVerdictCache` (`core/pipeline/eval_cache.py:81-89`) ключует по отрендеренному
промпту + конфигурации модели. Промпт может не содержать всех методологических правил
(scoring policy, unknown policy, absence-правила). Дизайн: расширить `echo` полями
`methodology_version`, `scoring_policy_version`, `status_dict_version`,
`catalog_row_digest`, `capability_policy`, `unknown_policy`, `partial_score` и поднять
`EVAL_CACHE_FORMAT` до 2. Иначе кэш может вернуть вердикт, посчитанный по старым правилам.

---

## 11. Coverage / честность

### 11.1. Новые поля `corpus_coverage`

```sql
ALTER TABLE corpus_coverage ADD COLUMN mode TEXT;              -- new|full|incremental
ALTER TABLE corpus_coverage ADD COLUMN freshness TEXT;         -- live|revalidated|stale
ALTER TABLE corpus_coverage ADD COLUMN reused_sources INTEGER DEFAULT 0;
ALTER TABLE corpus_coverage ADD COLUMN revalidated_sources INTEGER DEFAULT 0;
ALTER TABLE corpus_coverage ADD COLUMN new_sources INTEGER DEFAULT 0;
ALTER TABLE corpus_coverage ADD COLUMN removed_sources INTEGER DEFAULT 0;
ALTER TABLE corpus_coverage ADD COLUMN reason TEXT;            -- no_seeds|no_sources|...
ALTER TABLE corpus_coverage ADD COLUMN doc_set_digest TEXT;
```

- `expected_sources` — все URL work-set продукта (seed'ы + известные + обнаруженные).
- `observed_sources` — URL с терминальным исходом `FETCHED` (включая 304/same-hash).
- `reused_sources` — из `observed_sources`, контент переиспользован (`revalidated`).
- `revalidated_sources` — 304 или 200 с тем же `content_hash`.
- `new_sources` — 200 с новым `content_hash`.
- `removed_sources` — 404/410/исчез из sitemap.
- `freshness` — худшая свежесть по продукту: `live` (был живой 200 с новым контентом),
  `revalidated` (все наблюдения — 304/same-hash), `stale` (offline/reuse без сети).
- `complete` — не truncated, нет unreadable origins, нет blocking failures,
  `expected_sources > 0` **и** `observed_sources > 0` (есть хотя бы один читаемый документ).
  Продукт, у которого все seed'ы вернули 404/410, — `complete=False`: отсутствие нельзя
  доказывать по исчезнувшему сайту без человеческой аттестации. Это усиливает определение
  §4.4, а не ослабляет его. `reason` при этом остаётся `null` (причина видна по счётчикам
  и `blocked`), поэтому ячейки получают `failure_kind=coverage_missing`, а не отдельный
  класс: введение нового значения `failure_kind` — самостоятельная методологическая
  граница (словарь статусов), в 2.3.0 её нет.

**Ключевое различие:** `reused` в coverage — это **переиспользование контента**, а не
«не наблюдали». Все reused-URL были реально опрошены (304) в режиме `revalidate`.
Поэтому `complete=True` остаётся честным, и семантика §4.4 не меняется.

### 11.2. `❌` и неполный корпус

O7: `❌` публикуется только если:
- `coverage.complete == True` **и** `freshness != stale`; **или**
- есть человеческая аттестация `scope_attested_by`, привязанная к текущему
  `doc_set_digest`/`coverage_digest`.

Иначе машинный `❌` понижается до `pending` (как сейчас `evaluate.py:743-787`), а
`_record_corpus_coverage` фиксирует причину.

`freshness=stale` (режим `offline`) — `❌` **всегда** понижается: нельзя утверждать
отсутствие по невалидированному корпусу. Это не новая методология, а более строгое
операционное правило; в отчёте — `coverage.freshness=stale` и `reason=offline`.

### 11.3. Ноль seed'ов / ноль источников

- `onboarding` блокирует продукт с `pending_seeds` (O1).
- Если preflight обойдён: `corpus` пишет `expected_sources=0, complete=False,
  reason=no_seeds`, `evaluate` — `failed`-ячейки с `failure_kind=no_sources`, **не** `❌`.
- В отчёте — аномалия `corpus_coverage_anomalies` с продуктом и причиной.

### 11.4. `reused` vs `observed` в отчёте

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
Человек видит, что 38 ячеек не пересчитывались, а 4 — пересчитаны; это не «тихая»
экономия.

### 11.5. `scope_attested_by`

- Аттестация хранится в `state.db.attestation` с `doc_set_digest`/`coverage_digest`.
- Переносится только если digest'ы совпали; иначе `superseded_at` и `❌` → `pending`.
- Новый продукт не получает аттестацию автоматически (I6/I7).

### 11.6. `pending`/`N/A`

- Новый продукт: `N/A` — только человеком (`reviewed_by`), не автоматически.
- `pending` из-за неполного покрытия — с `review_reason=CORPUS_INCOMPLETE_REVIEW_REASON`
  (как сейчас); в отчёте — аномалия.
- `failed` — `status=null` (I4), входит в знаменатель Coverage (I5).

---

## 12. Versioning §13

### 12.1. Классы изменений

| Класс | Что меняется | Diff | Golden |
|---|---|---|---|
| `none` | ничего значимого | разрешён | — |
| `content_change` | только состав/контент документов (новая композиция, тот же execution contract) | разрешён с баннером `content_change` | — |
| `cohort_expansion` | добавлен продукт в когорту | **запрещён** (`execution_change`) | **`golden-full`** |
| `cohort_reduction` | продукт удалён/retired | **запрещён** (`execution_change`) | **`golden-full`** |
| `execution_change` | версии/модели/ретривал/краул/seed-набор | **запрещён** | `golden-full` при смене модели/каталога/когорты; иначе `golden-fast` |
| `methodology_change` | правила/scoring/absence/coverage/Comparability | **запрещён** | `golden-full` + пере-аннотация golden |

### 12.2. `cohort_digest` и `seeds_digest`

- `cohort_digest` входит в `run_id`, `runs`, `RunManifest`, `execution_contract_digest`.
- `seeds_digest` — тоже: смена seed-набора меняет корпус и coverage.
- `selection_reason`/`canonical_name` — **не** входят (авторский текст/отображение).

### 12.3. Lineage

`runs`/`RunManifest`:
- `cycle_id`;
- `previous_run_id`;
- `cohort_digest`, `seeds_digest`;
- `execution_contract_digest`;
- `change_class`;
- `golden_required` (`none|fast|full`);
- `state_schema_version`.

`state.db.run_lineage` — граф: `previous_run_id` → текущий. `previous_run_id` выбирается:
1. `--since-run`, если задан;
2. иначе последний `run_lineage` пакета со `status ∈ {complete, partial}` и
   `completed_at < now`, исключая текущий `run_id`;
3. для resume текущего `run_id` — `None` (carry-forward не выполняется).

### 12.4. Когда bump'ить

- **`methodology_version`** — только смена семантики правил: словарь статусов, scoring,
  absence/сателлитная/evidence-модель, определения Coverage/Comparability/sensitivity.
  Требует `golden-full` + пере-аннотацию golden.
- **`package_version`/`catalog_version`** — авторские изменения пакета; `execution_change`.
- **`cohort_ref`** — новое значение при изменении состава когорты (например,
  `cohort-ru-pg-dbms-2026-10-19-r2`); `execution_change` + `golden-full`.
- **`cycle_id`** — каждый update-цикл; не версия, а идентификатор.
- **`index_build_id`/`composition_id`** — вычисляемый; не «bump», а следствие.

### 12.5. Межцикловый diff

`diff_boundary_violations` (`report.py`) сравнивал `EXECUTION_FIELDS`, включая
`index_build_id`. Любое изменение контента (а значит, и каждый недельный update) блокировало
diff — инкрементальность была полезна только для стоимости, не для сравнения.

**Реализовано (методология 2.3.0).**

- `execution_contract_digest` — отпечаток **условий измерения**, без контента:
  `methodology_version`, `scoring_policy_version`, `status_dict_version`,
  `catalog_version`, `package_version`, `cohort_digest`, `seeds_digest`,
  `llm_scenarios`, `retrieval_profile`, `cutoff_K`, `retrieval_params_json`,
  `chunker_version`, `extractor_version`, `embed_model_id`, `lexical_engine_version`,
  `vector_store`, `crawl_strategy`, `crawl_rate_limit`, `crawl_config_digest`,
  `registry_spec_digest`, `deploy_profile`, `edge_schema_version`
  (`core/pipeline/digests.py:EXECUTION_CONTRACT_FIELDS`). Переносится в `runs`
  (аддитивная колонка) и в `RunManifest`, чтобы `hound diff` читал его из БД прогона,
  а не пересчитывал из другого набора значений.
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
> `golden-full` + пере-аннотация golden. **Реализовано в 2.3.0** (`METHODOLOGY_VERSION`
> = `2.3.0`).

### 12.6. Открытые решения человека

**Принято владельцем 19.10:**

1. **Comparability/diff** — вводим `execution_contract_digest` + `content_change`
   (`methodology_version` bump + golden re-annotation).
2. **`❌` на reused-корпусе** — только `live`/`revalidated` в текущем цикле; `offline`/
   `stale` не публикует `❌` (семантика §4.4 не меняется).
3. **`add-product` при нуле seed'ов** — писать `pending_seeds` + cohort, возвращать
   `EXIT_BLOCKED`; следующий `run` блокируется в `onboarding`.
4. **Partial-run policy** — `--update` с неготовым продуктом блокирует весь прогон
   (не публиковать неполный срез); `--only-product` обрабатывает только выбранный
   продукт.

**Ещё открыто:**

5. **Seed-файл** — **принято:** новый `sources/seeds.yaml`; `corpus_urls.yaml` остаётся
   производным экспортом (`hound package seeds export --format corpus-urls`); миграция —
   `ensure_seeds_file` (§7.5).
6. **Retention текста чанков:** бессрочно (рекомендуется, это субстрат evidence) vs
   бюджет/вытеснение. Второе делает часть evidence неверифицируемой.
7. **Per-cell vs per-product invalidation** (MVP — per-product).
8. **Golden для нового продукта** — **принято:** явный waiver
   (`golden_exempt: true` + причина в `cohort.yaml`/`run.yaml`), отчёт показывает
   аномалию `golden_exempt_product`; аннотация golden — цель на потом.
9. **Паспорт/сателлиты нового продукта** — **принято:** `add-product` генерирует
   draft-паспорт из scout-артефакта (identity, vendor, сайты, satellites) и помечает
   `needs_human`; eval получает authored-контекст, но draft не считается полным
   паспортом.
10. **Bootstrap** (явный `state adopt` vs `--adopt-latest`).

---

## 13. Failure / partial semantics

### 13.1. Статусы продукта в прогоне

`run_product.status`:
- `pending` — ещё не начат;
- `running`;
- `complete` — corpus + coverage complete и eval-фаза прошла; `pending`-ячейки
  допустимы (очередь ревью, не провал); `baseline_complete = (cells_pending == 0)`
  управляет обновлением `last_completed_run_id`;
- `partial` — часть URL/ячеек `failed`/`preliminary`, либо coverage неполон;
- `failed` — corpus/eval не состоялись;
- `blocked` — `pending_seeds`/гейт.

### 13.2. Новый продукт недоступен

- robots deny all / DNS / timeout / все seed'ы `FAILED`:
  - `coverage.complete=False`, `expected_sources>0`, `unreadable_origins` непуст;
  - `evaluate` → `failed`-ячейки (`status=null`), **не** `❌`;
  - `run_product.status=failed`;
  - итог `run` — `partial`/`failed`, `EXIT_ERROR`; в отчёте — аномалия с классами
    (`robots_disallow=N`, `source_unavailable=M`).
- Один из нескольких seed'ов недоступен, остальные читаемы: `partial`, `❌` demoted,
  `✅`/`⚠️` по доступным документам; продукт не становится `complete`.

### 13.3. Старый продукт не изменился

- Все URL → 304/same-hash; doc-set тот же; coverage complete.
- Партиция не пересобирается; ячейки carry-forward; LLM не вызывается.
- `run_product.status=complete`, `docs_reused=all`, `cells_carried=all`.
- `last_completed_run_id` обновляется (измерение подтверждено).

### 13.4. Старый продукт изменился частично

- Изменённые документы → новые `doc_version`; doc-set digest меняется.
- Партиция продукта пересобирается (эмбеддинги — только для новых чанков, если
  `embed_cache`; иначе все).
- MVP: все ячейки продукта переоцениваются; eval cache может дать хиты для ячеек, чей
  prompt не изменился.
- `run_product.status=complete` (если coverage complete), `docs_new=N`, `cells_measured=M`.

### 13.5. Повтор и resume

- `--only-failed` (межпрогонный): выбирает `(product, url)` со статусом `failed` в
  последнем прогоне/`--since-run`; `url_state.consecutive_failures` сохраняется;
  `max_attempts` — потолок; `attempts_exhausted` — терминально, не тихо.
- `run` без `--update` и с тем же `run_id` — resume: `Runner` пропускает `ok`-фазы
  (`runner.py:402-421`); `corpus` переиспользует doc-версии, добытые в этом прогоне.
- `--reset` — сбрасывает сессию/фазы текущего прогона, но не state.db.
- Падение между corpus и evaluate: `last_corpus_run_id` установлен, `last_evaluated_run_id`
  — нет → следующий прогон `full` (O12), но doc-версии/партиция переиспользуются.

### 13.6. `--only-product`

- `scope=product_only`; `last_completed_run_id` **не** обновляется (O10).
- В отчёте — `scope=product_only`, `partial=true`; продукт нельзя считать baseline.
- Следующий полный прогон увидит `last_completed_run_id` от предыдущего полного прогона
  и сделает `incremental` (не `full`), если тот был complete. Это корректно: частичный
  прогон не должен ломать baseline, но и не должен считаться полным измерением.

---

## 14. Schema / ports / migrations

### 14.1. Run DB: новые поля/таблицы

```sql
ALTER TABLE runs ADD COLUMN cycle_id TEXT;
ALTER TABLE runs ADD COLUMN previous_run_id TEXT;
ALTER TABLE runs ADD COLUMN cohort_digest TEXT;
ALTER TABLE runs ADD COLUMN seeds_digest TEXT;
ALTER TABLE runs ADD COLUMN execution_contract_digest TEXT;
ALTER TABLE runs ADD COLUMN change_class TEXT;
ALTER TABLE runs ADD COLUMN product_plan_json TEXT;

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

ALTER TABLE corpus_docs ADD COLUMN doc_version_id TEXT;
ALTER TABLE corpus_docs ADD COLUMN level TEXT;
ALTER TABLE corpus_docs ADD COLUMN doc_status TEXT;        -- active|superseded|evicted
ALTER TABLE corpus_docs ADD COLUMN reused_from_run_id TEXT;

ALTER TABLE corpus_chunks ADD COLUMN doc_version_id TEXT;
ALTER TABLE corpus_chunks ADD COLUMN partition_build_id TEXT;

ALTER TABLE index_manifest ADD COLUMN partition_build_id TEXT;

ALTER TABLE cells ADD COLUMN measurement_origin TEXT DEFAULT 'observed';
ALTER TABLE cells ADD COLUMN carried_from_run_id TEXT;
ALTER TABLE cells ADD COLUMN cell_reuse_key TEXT;
ALTER TABLE cells ADD COLUMN doc_set_digest TEXT;
ALTER TABLE cells ADD COLUMN coverage_digest TEXT;
ALTER TABLE cells ADD COLUMN catalog_row_digest TEXT;
ALTER TABLE cells ADD COLUMN partition_build_id TEXT;

ALTER TABLE cell_evidence ADD COLUMN doc_version_id TEXT;
ALTER TABLE cell_evidence ADD COLUMN partition_build_id TEXT;
ALTER TABLE cell_evidence ADD COLUMN origin_evidence_id TEXT;
```

`index_manifest` PK остаётся `(index_build_id, product_id)`, где `index_build_id` =
`composition_id`; `partition_build_id` — новое поле.

### 14.2. `run_inputs` — без `REPLACE`

`save_plan_inputs` (`sqlite_store.py:738-761`) меняется:

```sql
INSERT INTO run_inputs (run_id, snapshot_version, payload_json, payload_sha256, created_at)
VALUES (?, ?, ?, ?, ?);
-- при конфликте: если payload_sha256 совпал — no-op (resume);
-- иначе — ошибка run_inputs_conflict, не перезапись.
```

Payload дополняется `cycle_id`, `cohort_digest`, `seeds_digest`, `product_plan`,
`previous_run_id`, `state_schema_version`.

### 14.3. Порты

Новый `ProductStatePort` (`core/ports/product_state.py`, Protocol):

```python
class ProductStatePort(Protocol):
    # product
    def get_product_state(self, product_id: str) -> ProductState | None: ...
    def upsert_product_state(self, state: ProductState) -> None: ...
    def set_onboarding_state(self, product_id, state, reason=None) -> None: ...
    def set_last_corpus_run_id(self, product_id, run_id) -> None: ...
    def set_last_evaluated(self, product_id, run_id) -> None: ...
    def set_last_completed(self, product_id, run_id, cycle_id) -> None: ...
    # seeds
    def iter_seeds(self, product_id: str) -> Iterator[SeedEntry]: ...
    def upsert_seed(self, seed: SeedEntry) -> None: ...
    def disable_seed(self, product_id, normalized_url, actor) -> None: ...
    # url/document state
    def get_url_state(self, product_id, normalized_url) -> UrlState | None: ...
    def iter_url_state(self, product_id) -> Iterator[UrlState]: ...
    def upsert_url_state(self, state: UrlState) -> None: ...
    def get_document_version(self, doc_version_id) -> DocumentVersion | None: ...
    def find_document_version(self, product_id, content_hash, extractor_version, level) -> DocumentVersion | None: ...
    def upsert_document_version(self, dv: DocumentVersion) -> None: ...
    def iter_chunks(self, doc_version_id) -> Iterator[ChunkMeta]: ...
    def get_chunk_text(self, content_hash) -> str | None: ...
    # snapshots/measurements
    def last_snapshot(self, product_id, *, scope: str = "full") -> ProductSnapshot | None: ...
    def save_snapshot(self, snap: ProductSnapshot) -> None: ...
    def save_cell_measurement(self, cm: CellMeasurement) -> None: ...
    def get_cell_measurement(self, run_id, product_id, param_id) -> CellMeasurement | None: ...
    # lineage/index
    def record_run(self, lineage: RunLineage) -> None: ...
    def update_run_status(self, run_id, status, error=None) -> None: ...
    def previous_run(self, package_id, *, before: str) -> RunLineage | None: ...
    def register_partition(self, part: IndexPartition) -> None: ...
    def get_partition(self, package_id, product_id, partition_build_id) -> IndexPartition | None: ...
    def save_composition(self, comp: IndexComposition) -> None: ...
    def get_composition(self, composition_id) -> IndexComposition | None: ...
    # attestation
    def get_attestation(self, product_id, param_id) -> Attestation | None: ...
```

Реализация: `core/store/state_sqlite.py` (`SqliteProductState`), DDL — §4.2.
`pipeline` зависит только от Protocol; composition root (`core/cli/wiring.py`) строит
`SqliteProductState(paths.state / package_id / "state.db")`.

`StorePort` (run DB) расширяется:
- `save_run_product`, `iter_run_products`, `read_run_product`;
- `iter_evidence(run_id, product_id, param_id)`;
- `save_plan_inputs` — семантика без `REPLACE` (см. §14.2);
- `iter_chunks(build_id, product_id=...)` — `build_id` теперь `partition_build_id`
  (для lexical/vector); добавить `iter_chunks_by_composition(composition_id, product_id)`
  или оставить `iter_chunks` с `partition_build_id` и вызывать по member'ам.

`IndexBuilderPort`:
- `build(chunks, vectors, meta)` — без изменения сигнатуры; `meta.extra["partition_build_id"]`
  обязателен; `meta.product_id` — один продукт;
- `build_id` возвращается = `partition_build_id`;
- новый `IndexCatalogPort` (или методы state) — регистрация/чтение композиций. Не смешивать
  с `IndexBuilderPort` (запись vs чтение).

`VectorIndexPort`/`LexicalIndexPort`:
- конструктор/фабрика принимает `CompositionView`;
- `build_id` = `composition_id`; per-chunk `build_id` = `partition_build_id`.

`CrawlerPort` — без изменений: условный GET уже выражен в `FetchPlan`
(`core/ports/types.py:33-67`: `if_none_match`, `if_modified_since`). Меняется только
источник этих значений: `url_state`, а не HTTP-кэш.

### 14.4. Артефакты и раскладка

```
packages/{case_id}/
  sources/seeds.yaml                     # новый канонический seed-набор
  sources/corpus_urls.yaml               # производный экспорт (legacy)
  sources/add_product/{date}-{product_id}.json  # + seed_plan/onboarding

state/{package_id}/state.db              # новый durable per-product state
state/{package_id}/state.db-wal
state/{package_id}/.lock

runs/{run_id}/
  cohound.db                             # + run_product, новые поля
  run.yaml                               # снимок (эффективный cycle_id)
  plan_inputs.json                       # + product_plan/lineage
  logs/, prompts/, outputs/

index_store/{package}/
  {partition_build_id}/{product_id}/     # иммутабельная партиция
  _compositions/{composition_id}.json    # иммутабельная композиция
```

`config/paths.py`: новое поле `state`; `PathsConfig` + `HARNESS_FIELDS` +
`ENV_BY_FIELD` + `config/paths.example.yaml`.

`backup.py`: бэкап прогона дополняется `state/{package}/state.db` (или документируется,
что state — отдельный объект бэкапа). Рекомендуется: `hound backup --include-state`.

### 14.5. Layer matrix / категорийная нейтральность

| Модуль | Слой | Зависимости |
|---|---|---|
| `core/domain/enums.py`, `models.py`, `invariants.py` | domain | только stdlib/pydantic |
| `core/ports/product_state.py` | ports | domain |
| `core/config/seeds.py` | config | domain/config/util |
| `core/pipeline/onboarding.py`, `incremental.py`, `composition.py` | pipeline | domain/ports/util |
| `core/store/state_sqlite.py` | store | domain/ports, sqlite3 |
| `core/cli/wiring.py`, `commands/*` | cli | всё (composition root) |
| `core/adapters/index_npy.py`, `vector_npy.py` | adapters | ports/util |

`pipeline` **не** импортирует `store`/`adapters`/`cli`; `state` инжектится как
`ProductStatePort`. Грепп-тест §1.3: в `core/domain`, `core/ports`, `core/pipeline` нет
названий продуктов/вендоров; `seeds.yaml` — данные пакета, не код.

### 14.6. Что придётся заменить, а не надстроить

1. `derive_run_id` (`plan.py:72-90`) — убрать `products`/`snapshot_date`, добавить
   `cycle_id`/`cohort_digest`/`seeds_digest`/`execution_contract_digest`/`run_scope`/
   `refresh_policy`.
2. `_derive_index_build_id`/`_content_token` (`corpus.py:793-865`) — заменить на
   `derive_partition_build_id` + `derive_composition_id`.
3. `_persist_corpus_coverage` (`corpus.py:355-373`) — писать строку для **каждого**
   продукта плана, включая zero-source.
4. `_coverage_for` (`evaluate.py:355-370`) — `None` для продукта плана = аномалия, не
   «неизвестно».
5. `_load_corpus_urls`/`_resolve_phases` (`context.py:1067-1115`, `1387-1391`) —
   приоритет `--urls > seeds.yaml > corpus_urls.yaml`.
6. `NpyVectorIndex` (`vector_npy.py:75-160`) — один `build_id` → `CompositionView`.
7. `seed_lexical_index` (`wiring.py:283-312`) — итерация по member'ам композиции.
8. `--only-failed` (`context.py:1118-1136`) — сессионный → межпрогонный.
9. `save_plan_inputs` (`sqlite_store.py:738-761`) — `REPLACE` → insert-once/conflict.
10. `EvalVerdictCache` key (`eval_cache.py:81-89`) — добавить методологические поля.
11. `chunk_id` (`corpus.py:887-895`) — включить `doc_version_id` + `chunker_version`
    (иначе смена чанкера даёт тот же id с другим текстом).
12. `corpus_docs.doc_id`/`corpus_chunks.doc_id` — перейти на `doc_version_id` (старые
    прогоны остаются читаемыми; новые используют новый формат).

---

## 15. Tests / acceptance

### 15.1. Функциональные сценарии

| # | Сценарий | Ожидание |
|---|---|---|
| T1 | `add-product` без `--url`, SERP недоступен | plan-only: `needs_human=["identity"]`, ничего не записано |
| T2 | `add-product --url <hint>` | seeds.yaml содержит hint + все обнаруженные сайты; `--url` не единственный seed |
| T3 | `add-product --apply`, discovery дал 0 seed'ов | cohort + seeds `pending_seeds`; exit `EXIT_BLOCKED`; artifact `needs_human=["no_seeds"]` |
| T4 | `run --update` после T3 | `onboarding` → `blocked`, `reason=product_not_ready`, сообщение с продуктом; нет cells/report |
| T5 | Новый продукт с 2 seed'ами | full crawl обоих; coverage `expected>0`; все ячейки `observed`; `last_completed_run_id` обновлён |
| T6 | Два новых продукта за один update | оба `new`; старые `incremental`; композиция содержит все; старые партиции не пересобраны |
| T7 | Старый продукт, все URL → 304 | `docs_reused=all`; `cells_carried=all`; LLM/embed не вызваны; partition id не изменился |
| T8 | Старый продукт, один документ изменился | новый `doc_version`; новая партиция только этого продукта; ячейки продукта переоценены; остальные carried |
| T9 | Новый продукт недоступен (robots/timeout) | `run_product=failed`; coverage `complete=False`; cells `failed`/`status=null`; нет `❌`; exit error |
| T10 | Повторный `run` после падения | resume того же `run_id`; `ok`-фазы пропущены; добытые doc-версии переиспользованы; нет дублей |
| T11 | `run --update` с расширением когорты | `change_class=cohort_expansion`; `golden_required=full`; diff запрещён с `execution_change: cohort_digest`; `previous_run_id` корректен |
| T12 | `--only-product` | `scope=product_only`; `last_completed_run_id` не изменён; отчёт помечен partial |
| T13 | `--only-failed` после T9 | повторяются только failed URL/продукты; успешные не фетчатся |
| T14 | Пустые seed'ы, preflight обойдён (unit) | coverage `expected=0, complete=False, reason=no_seeds`; cells `failed`; нет «тихого ok» |
| T15 | Продукт retired, затем re-add | режим `new`; полный краул; старые партиции сохранены, но не в композиции |
| T16 | Переименование `canonical_name` | режим `incremental`; краул не запускается; меняется только `product_state` |
| T17 | Смена `cutoff_K` | `cell_reuse_key` не совпал; все ячейки переоценены (I20) |
| T18 | Смена `methodology_version` | carry-forward запрещён; eval cache miss; golden-full требуется |
| T19 | Смена модели/слота | carry-forward запрещён; eval cache miss (I21) |
| T20 | `offline` refresh | `freshness=stale`; `❌` → pending; отчёт явно показывает stale |

### 15.2. Инвариантные тесты

- **I17:** retrieval по `product_ids=[A]` не возвращает чанки B; carried evidence
  `product_id=A`; композиция не смешивает партиции.
- **I18:** evaluator без web-tool; carry-forward не делает сетевых вызовов (mock
  crawler/search не вызывается).
- **I19:** каждая evidence-строка имеет `doc_version_id` + `content_hash`; doc-версия
  входит в doc-set; reused-документ прошёл `raw→hash→index` в прошлом цикле и
  ревалидирован в текущем.
- **I20:** `cutoff_K` в `cell_reuse_key`; golden и pipeline используют один K.
- **I21:** `llm_scenario` в `cell_reuse_key`; смена слота → miss.
- **O1–O13:** property-тесты на `resolve_product_modes` (все комбинации
  `last_corpus_run_id`/`last_evaluated_run_id`/`last_completed_run_id`/`re_added_at`/`scope`).

### 15.3. Регрессии

- `tests/selfcheck.py` — добавить проверку DDL `state.db` и инвариантов O1–O13.
- `tests/domain/` — property-тесты `ProductMode`, `CellReuseKey`, `composition_id`
  (детерминизм/порядко-независимость).
- `tests/packages/test_build_package.py` — `seeds.yaml` vs `corpus_urls.yaml` деривация
  (generator drift).
- `scripts/ci.sh` — без изменений; новые тесты входят в pytest.

---

## 16. Rollout

### 16.1. Этапы

| Этап | Что | Риск | Ценность |
|---|---|---|---|
| **0. Контракты** | `seeds.yaml` schema, `ProductStatePort`, enum'ы, `state.db` DDL, `composition` формулы | низкий | фиксирует границы |
| **1. Seeds + preflight (MVP-1)** | `add-product` пишет seeds; `run` читает seeds; `onboarding` блокирует `pending_seeds`; coverage zero-source; `_coverage_for` anomaly | низкий | закрывает «тихий пустой продукт» |
| **2. Run identity + state (MVP-2)** | `derive_run_id` без products; `cycle_id`/`--update`; `state.db` (product/seed/snapshot/lineage); new/full/incremental | средний | продукт не теряется между прогонами |
| **3. Incremental corpus (MVP-3)** | `url_state`/`document_version`; conditional GET; reuse doc/chunks; per-product doc-set | средний | экономия сети/экстракции |
| **4. Partitions + composition (MVP-4)** | `partition_build_id`; `composition_id`; `CompositionView`; retrieval/lexical | высокий (индексный контур) | добавление продукта не пересобирает старые |
| **5. Carry-forward + coverage honesty (MVP-5)** | `cell_reuse_key`; `measurement_origin`; coverage freshness; eval cache key; tests | высокий (методология) | экономия LLM + честный отчёт |
| **Later** | per-cell invalidation, embed cache, sitemap `lastmod`, `seeds` UX, state GC, split/DMZ parity | — | оптимизация |

### 16.2. MVP (3–5 шагов)

1. **Seeds + preflight.** `sources/seeds.yaml` + `add-product` запись + `run` чтение +
   `onboarding` блокировка + coverage zero-source. Это самостоятельная ценность: продукт
   больше не «тихий пустой».
2. **Run identity + durable state.** `derive_run_id` (cycle/cohort/seeds), `--update`,
   `state.db` с `product_state`/`seed_set`/`product_snapshot`/`run_lineage`,
   `run_product`, new/full/incremental.
3. **Incremental corpus.** `url_state`/`document_version`/`chunk_store`, conditional GET,
   reuse doc/chunks; `--refresh`.
4. **Per-product index + composition.** `partition_build_id`/`composition_id`,
   `CompositionView`, retrieval/lexical, reuse старых партиций.
5. **Carry-forward + coverage.** `cell_reuse_key`, `measurement_origin`, coverage
   `reused/revalidated/freshness`, eval cache key, acceptance-тесты, golden-full gate.

### 16.3. Риски и компромиссы

| Риск | Митигация |
|---|---|
| Diff-семантика (`index_build_id`) ломает недельный diff | решение человека (§12.5); рекомендуемый `execution_contract_digest` + `content_change` |
| Две БД (run/state) рассинхронизируются при падении | content-addressed state; snapshot после eval; recovery `aborted`; идемпотентные upsert'ы |
| State DB растёт (chunk text, snapshots) | retention/GC; текст — бессрочно (evidence), остальное — по бюджету |
| Конкурентные прогоны одного пакета | file lock + WAL; второй run — `EXIT_USAGE` |
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

1. **Seed'ы — первоклассный пакетный артефакт** `sources/seeds.yaml` (роль, source,
   profile_key, enabled); `--url` — только hint, не единственный seed.
2. **`pending_seeds` — durable состояние**, а не молчаливый пропуск: `add-product`
   добавляет продукт в когорту, пишет пустой seed-набор и возвращает `EXIT_BLOCKED`;
   следующий `run` блокируется в `onboarding`.
3. **`run_id` больше не зависит от списка продуктов**: `cycle_id + cohort_digest +
   seeds_digest + execution_contract_digest + run_scope + refresh_policy + focus`.
   Добавление продукта — новый прогон, не мутация.
4. **Durable per-product state — `state/{package}/state.db`** (не глобальный `history.db`):
   `product_state`, `seed_set`, `url_state`, `document_version`, `chunk_store`,
   `product_snapshot`, `cell_measurement`, `run_lineage`, `index_partition`,
   `index_composition`, `attestation`.
5. **«Новый» = нет `last_corpus_run_id`**, а не diff когорты; `full` = corpus был, eval
   нет; `incremental` = есть complete snapshot.
6. **Новый/неполный продукт — полный краул; существующий — conditional GET + reuse**
   документов/чанков/эмбеддингов/партиций; `offline` — только для debug, `❌` запрещён.
7. **Индекс: per-product immutable partitions + cohort composition manifest**;
   `partition_build_id` не включает `params_token`/`snapshot_date`; `composition_id` —
   `runs.index_build_id`; добавление продукта не пересобирает старые партиции.
8. **Retrieval через `CompositionView`** (product_id → partition_build_id); I17
   сохраняется; lexical/vector читают member'ы композиции.
9. **Carry-forward ячеек по `cell_reuse_key`** (каталог, промпт, модель, retrieval,
   doc-set, coverage, методология, аттестация, partition); `measurement_origin`
   `observed|carried`; evidence привязан к `doc_version_id` (I19).
10. **Coverage пересчитывается всегда**; `reused` — переиспользование контента, не
    отсутствие наблюдения; `complete` требует `expected>0`; `❌` требует complete +
    non-stale (или аттестации).
11. **Eval cache key расширяется методологическими полями** — иначе кэш может вернуть
    вердикт по старым правилам.
12. **§13: расширение когорты = `cohort_expansion` (execution_change) + `golden-full`**;
    lineage (`previous_run_id`, `cycle_id`, `execution_contract_digest`) фиксируется в
    `runs`/`state.db`.
13. **`--update` явный**; `--since-run` — override predecessor; `--only-failed` —
    межпрогонный; `--only-product` не обновляет baseline.
14. **O1–O13** — новые инварианты, закрывающие тихий пустой продукт, пустой coverage,
    ложный carry-forward и потерю провенанса.
15. **Заменяем, а не надстраиваем:** `derive_run_id`, batch `index_build_id`,
    `_persist_corpus_coverage`, `_coverage_for`, `NpyVectorIndex` single-build,
    `--only-failed`, `save_plan_inputs` REPLACE, `chunk_id`.

---

## 18. Открытые вопросы / риски для решения человека

1. **Comparability/diff.** ~~Вводить ли `execution_contract_digest` и класс
   `content_change`…~~ **Решено и реализовано (2.3.0):** `hound diff` сравнивает
   `execution_contract_digest`, `content_change` — разрешённый diff с баннером и doc-delta
   (§12.5).
2. **`❌` на reused-корпусе.** Требовать ли live-revalidation (304) в текущем цикле
   (рекомендуется, не меняет семантику §4.4) или разрешать `❌` на `stale` с аттестацией
   (смена absence-семантики → `methodology_version`)?
3. **Поведение `add-product` при нуле seed'ов.** Писать `pending_seeds` + cohort и
   возвращать `EXIT_BLOCKED` (рекомендуется, run блокируется) или не добавлять продукт
   вовсе (тогда run о нём не знает)?
4. **Seed-файл и миграция.** Новый `sources/seeds.yaml` (рекомендуется) vs расширение
   `corpus_urls.yaml`; как обновлять `packages/{case}/sources/build_package.py`, чтобы
   не было generator drift.
5. **Retention текста чанков.** Бессрочно (рекомендуется: это субстрат evidence) vs
   бюджет/вытеснение (тогда часть evidence становится неверифицируемой).
6. **Per-cell vs per-product invalidation.** MVP переоценивает весь продукт при изменении
   любого документа (консервативно, дороже по LLM); per-cell требует retrieval до
   проверки ключа и сложнее. Нужен порог/политика.
7. **Golden для нового продукта.** `golden.csv` сейчас содержит строки только для
   `pangolin`/`postgrespro`; `evaluate_golden` проверяет лишь присутствующие строки, поэтому
   новый продукт молча оказывается вне golden. Варианты: (а) требовать golden-строки для
   каждого продукта когорты (расширяет `golden-full`, нужна аннотация); (б) явный waiver в
   `cohort.yaml`/`run.yaml` (`golden_exempt: true` + причина) с отчётом-аномалией;
   (в) считать golden только для «эталонной» подкогорты и фиксировать это в methodology.
   Рекомендуется (б) для MVP + (а) как цель.
8. **Паспорт/сателлиты нового продукта.** Если `--passport` не задан, у продукта нет
   паспорта и сателлитов; retrieval работает только по корпусу. Варианты: (а) требовать
   draft-паспорт (хотя бы `product.yaml` с identity) для допуска к eval; (б) разрешать
   eval без паспорта с `needs_human` и понижением `✅` (нет authored-контекста);
   (в) генерировать draft-паспорт из scout-артефакта. Рекомендуется (в) + `needs_human`.
9. **Bootstrap/adoption.** Явная `state adopt` (рекомендуется, §4.6) vs авто-принятие
   последнего complete-прогона при первом `--update` (`--adopt-latest`). Явная команда
   сохраняет контроль над baseline, но требует шага в runbook.
10. **Partial-run policy.** Текущий дизайн блокирует весь run, если есть продукт с
    `pending_seeds` (O1). Альтернатива — обрабатывать готовые продукты, а неготовые
    помечать `blocked` в отчёте (частичный результат). Первое честнее для когорты
    (не публиковать неполный срез), второе — операционно удобнее. Рекомендуется первое
    для `--update` и второе для `--only-product`.

---

## 19. Приложение: псевдокод `run --update`

```python
def cmd_run_update(args):
    inputs = load_inputs(args.run_yaml, args.package)          # pure
    state = SqliteProductState(paths.state / inputs.run.package / "state.db")
    with state.lock():                                         # .lock
        prev = state.previous_run(inputs.run.package) if not args.resume else None
        cycle_id = resolve_cycle_id(args, inputs, prev)        # без неявного today()
        seeds = load_effective_seeds(args, inputs.package)      # --urls > seeds.yaml > corpus_urls
        reconcile_seed_set(state, seeds, actor=args.actor)
        cohort_digest = derive_cohort_digest(inputs.cohort)
        seeds_digest = derive_seeds_digest(seeds)
        contract_digest = execution_contract_digest(inputs, plan_axes=..., cohort_digest=cohort_digest,
                                                    seeds_digest=seeds_digest)
        run_scope = resolve_run_scope(args, prev)               # full|only_failed|product:<id>
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
        state.record_run(RunLineage(run_id, ..., previous_run_id=prev.run_id if prev else None,
                                    change_class=change_class, status="running"))
        phases = build_phases(
            inputs, plan, product_plan=product_plan, state=state,
            refresh=args.refresh, urls=effective_urls(seeds),
            composition_factory=CompositionView,
            previous_run_store=open_run_store(prev.run_id) if prev else None,
        )
        report = Runner(store, phases).run(run_id)
        finalize_products(store, state, run_id, product_plan, report)
        state.update_run_status(run_id, report.status)
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

*Документ design-only. Реализация — отдельными атомарными коммитами по этапам §16;
изменения §13/§4.4 — только после явного решения владельца методологии.*
