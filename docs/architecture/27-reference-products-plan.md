# 27. Reference-продукты профиля: разведка, факты и обновление по версиям

**Статус:** план (14.11.2026). Не реализовано. Заменяет собой «черновик наследования из
эвристик профиля» как основной путь: `compose_baselines` остаётся, но перестаёт быть
единственным источником строк.

Решение владельца, на котором держится весь документ:

1. baseline-продукты — **часть профиля**, но **не часть когорты**;
2. профиль обязан знать не только актуальную версию reference-продукта, но и **историю
   мажорных версий** (для PostgreSQL — начиная с 11);
3. reference-продукты **по умолчанию не входят** в результаты сравнения когорты — только
   по отдельному запросу;
4. обратная совместимость не требуется: формат, схема БД и раскладка переделываются
   свободно.

---

## 1. Что строится и где граница

**Строится:** инструмент, который (а) измеряет reference-продукт по объявленному каталогу
профиля на объявленной версии его документации, (б) публикует результат как **объявленный
артефакт пакета**, который читает волна наследования Ш4, и (в) умеет перемерять его при
выходе новой мажорной версии, показывая diff, а не переписывая числа молча.

**Не строится в этом плане:** полноценная карта возможностей reference-продукта
(`Capability`/`Coverage` по 181 параметру) — §12, отложено с триггером.

Причина разделения — в том, что́ нужно конвейеру. Ш4 не нужна карта upstream: ей нужен
**факт** «в версии V параметр P у reference-продукта есть, вот цитата и её хеш». Карта —
другой предмет (сравнение), и она входит в когорту, чего владелец как раз не хочет.

### 1.1. Ключевое свойство: наследование — ускоритель, а не ворота

Если факта нет, `pending` он или `rejected` — **ячейка не наследуется, и параметр
оценивается обычным путём** (Ш5/Ш6 по корпусу продукта). Ни `❓`, ни `❌` из этого не
следует: отсутствие ускорения не является наблюдением о продукте.

Отсюда — риск дефекта в этой подсистеме ограничен: худшее, что даёт неверный факт, — это
лишняя работа и неверная провенанс-запись, а не пропущенная ячейка. Именно поэтому факты
можно вводить постепенно, не блокируя прогоны.

---

## 2. Что уже есть и чего нет (проверено по коду 14.11.2026)

| деталь | состояние |
|---|---|
| `PackageManifest.baselines: list[str]` (`core/domain/models.py:236`) | ✅ объявлено (`ru-pg-dbms: [upstream-pg]`) |
| `BaselineRow` (`models.py:372`), `load_baselines` (`core/config/run_config.py:287`) | ✅ формат `(baseline_id, param_id, min_version, inheritance_mode, note)` |
| `upstream.baseline_id` в паспортах (`models.py:410`) | ✅ есть у 4 из 5 продуктов `ru-pg-dbms`; у `pangolin` — `null` |
| режимы `direct/conditional/blocked` + I8 (`invariants.py:527`) | ✅ |
| порог `min_version` (`inheritance.py:127`) | ✅ (нераспознанная версия → пропуск, не «считаем подходящей») |
| базис «объявлено/выпущено» (`inheritance.py:378`) | ✅ |
| CRUD строк с линтом (`package_admin.py:1394,1440`) | ✅ `hound package baseline add/update/remove/list/get` |
| кандидаты `kind="baseline"` → `package apply` (`package_candidates.py:274`) | ✅ |
| черновик из эвристик профиля (`package_discovery.py:241`) | ✅ `hound package compose-baselines` (ничего не пишет) |
| **`baselines.csv` в `run_id`** | ❌ **нет `baselines_digest`** в `EXECUTION_CONTRACT_FIELDS` (32 поля) |
| **per-param evidence наследования** | ❌ `_derived_evidence` (`inheritance.py:152`) берёт **одну** цитату из `passport.upstream.evidence` |
| **reference-продукт как сущность** | ❌ `baseline_id` — строка, за ней нет ни продукта, ни версии, ни корпуса |
| **история версий** | ❌ `upstream.version` — одна строка в паспорте |
| **измерение upstream** | ❌ нет: наследование *предполагает* upstream-факт, а не измеряет его |

Замер эталона `ru-pg-dbms-2a5b51af5778` (08.11.2026): `baselines.csv` — **0 строк**,
`inheritance.jsonl` → `cells: 0`, `cells.source_kind` = `documentation: 905`,
`cell_evidence(source_kind=inheritance)` = **0**. Механизм построен и не используется.

**Спека это предусмотрела, код — нет** (`cohound-spec-v2.md:338`):

> носитель — запись `cell_evidence` со ссылкой на upstream-паспорт/строку baselines и,
> **где возможно, на корпусный чанк upstream-продукта** (`source_id`); если чанк
> недоступен — на паспортный `Citation` upstream

---

## 3. Модель

### 3.1. Reference-продукт

```yaml
# packages/{case_id}/sources/reference_products.yaml   (schema reference_products/1)
schema: reference_products/1
products:
  - baseline_id: upstream-pg          # связь с package.yaml:baselines и passport.upstream
    product_id: postgresql            # slug reference-продукта (namespace отдельный)
    kind: upstream                    # upstream | sibling
    canonical_name: PostgreSQL
    versions:
      - version: "11"
        docs_roots: ["https://www.postgresql.org/docs/11/"]
      - version: "18"
        docs_roots: ["https://www.postgresql.org/docs/18/"]
    current_version: "18"             # какая считается актуальной для наследования
```

`kind`:
* `upstream` — предок, от которого наследуют (архетип A);
* `sibling` — параллельная OSS-линия (архетип B): наследование от неё запрещено
  (`direct` недопустим), но факты полезны для сопоставимости.

`product_id` reference-продукта **не пересекается** с `product_id` когорты — линтер это
проверяет. Namespace общий намеренно: два разных смысла одного slug'а в одном пакете —
источник молчаливых ошибок.

### 3.2. Факт

```csv
# packages/{case_id}/sources/baseline_facts.csv   (форма baseline_facts/1)
baseline_id,param_id,version,status,url,quote,content_hash,observed_at,corpus_build_id,review,reviewed_by,note
upstream-pg,pg_json_type,18,present,https://www.postgresql.org/docs/18/datatype-json.html,"...jsonb...",sha256:…,2026-11-14T09:12:00Z,ref_pg_18_a1b2c3d4,accepted,roger,
```

* `status` — `present` | `absent` | `unclear`. Не `Status` ячейки: это наблюдение о
  документации upstream, а не вердикт о продукте. `absent` — «в документации этой версии
  параметр не описан», а не «функции нет» (разница — та же, что между `❓` и `❌`).
* `quote` — **дословная** подстрока текста чанка (проверка — как I22: `quote ∈ chunk.text`).
* `content_hash` — `content_hash` того чанка, откуда цитата (I19: хеш текста, не URL).
* `review` — `pending` | `accepted` | `rejected`. **Наследует только `accepted`.**
* `reviewed_by` / `observed_at` / `corpus_build_id` — провенанс, в дайджест не входят.

### 3.3. Разделение «политика / наблюдение»

`baselines.csv` остаётся **политикой**: какие параметры наследуем, в каком режиме, с каким
полом версии. `baseline_facts.csv` — **наблюдение**: что у upstream реально есть в каждой
версии. Это разные носители, и смешивать их нельзя: политика — авторское решение,
наблюдение — замер с датой.

Практическое следствие: `min_version` в `baselines.csv` может быть **выведен** из фактов,
но никогда не пишется туда автоматически (§9).

---

## 4. Раскладка и носители

```
packages/{case_id}/sources/
  reference_products.yaml          # авторская декларация (в git)
  baseline_facts.csv               # объявленный вход прогона (в git, ~300 КБ на 181×8)
  reference/{product_id}/{version}/
    corpus_urls.yaml               # корни документации этой версии (авторские, в git)
```

Корпус reference-продукта — **данные хоста**, не пакет:

```
COHOUND_HOME/reference/{package_id}/{product_id}/{version}/
  reference.db        # corpus_docs + corpus_chunks (та же схема, отдельная БД)
  lexical/            # tantivy-индекс по чанкам
  manifest.json       # build_id, extractor_version, chunker_version, content_digest, docs, chunks
```

Свойства:
* **иммутабельно**, как `index_store/{package}/{build}/{product}`: новая сборка — новый
  `build_id`, правка на месте запрещена;
* **content-addressed**: `build_id = ref_{product}_{version}_{hash(content_digest)[:8]}`;
* **не в `runs/`**: reference-корпус живёт дольше прогона и переиспользуется между
  циклами (документация PostgreSQL 11 не меняется никогда);
* **вне когорты**: не участвует в `corpus_coverage`, `cells`, `Capability`.

Почему не в run-DB: старые версии документации не перемеряются еженедельно. Связать их с
прогоном значит либо перекрауливать неизменяемое каждую неделю, либо блокировать refresh
продуктов на краул upstream. Ни то, ни другое не оправдано.

---

## 5. CLI

Группа `hound baseline`, отдельно от `hound package` (у той — авторские CRUD-операции над
пакетом; здесь — измерение).

```bash
# что объявлено в профиле и в каком состоянии
hound baseline list --package {case_id} [--json]
#   upstream-pg → postgresql (upstream), версии 11…18, фактов 1284 (accepted 1102, pending 182)

# разведка: построить/дополнить reference-корпус и снять факты по каталогу
hound baseline recon --package {case_id} --baseline upstream-pg \
  [--version 18] [--param pg_json_type] [--only-changed] [--dry-run] [--json]

# что измерено: чтение фактов с фильтрами (ничего не пишет)
hound baseline facts --package {case_id} --baseline upstream-pg \
  [--version 18] [--param …] [--review pending] [--json]

# человеческая корректировка — по одному или пачкой
hound baseline review --package {case_id} --baseline upstream-pg \
  --param pg_json_type --version 18 --accept|--reject --actor {кто} [--note "…"]
hound baseline review --package {case_id} --baseline upstream-pg \
  --version 18 --accept-all --actor {кто} [--dry-run]   # печатает, что принимает

# новая мажорная версия: перемер + diff, без записи
hound baseline refresh --package {case_id} --baseline upstream-pg --version 19 [--apply]

# что из этого унаследовано и кем (read-only, прогон не нужен)
hound baseline report --package {case_id} [--baseline upstream-pg] [--json]
```

Правила, общие для всех подкоманд:

* `--actor` обязателен для любой записи (`recon --apply`, `review`, `refresh --apply`) —
  тот же класс, что `package apply --actor` и `scope attest --actor`;
* `--dry-run` ничего не пишет и **говорит**, что записал бы;
* запись идёт через журнал `.authoring/apply_log.jsonl` (существующий контур I1);
* ни одна подкоманда не пишет в `runs/` и не трогает прогон.

---

## 6. Изменения в конвейере: волна Ш4

### 6.1. Сегодня

```
для продукта P с passport.upstream = {baseline_id: B, version: V}:
  для строки row из baselines.csv, где row.baseline_id == B:
    direct: если V >= row.min_version → ячейка с evidence из passport.upstream.evidence
```

Одна цитата (`_derived_evidence`, `inheritance.py:156`) обслуживает все параметры. У
`postgrespro` это «Российская СУБД корпоративного уровня на базе PostgreSQL с расширенной
функциональностью». Формально I1/I8 проходят, содержательно — `support_not_about_subject`
(`inheritance.py:194`) почти на всех.

### 6.2. Станет

```
ref = reference_product_by_baseline(B)                    # может быть None
V = major_version(passport.upstream.version)               # "18.4.1" → 18

для строки row из baselines.csv, где row.baseline_id == B:
  if row.inheritance_mode is not DIRECT:  как раньше (conditional → pending, blocked → пропуск)
  if not V >= row.min_version:            пропуск (порог), причина в базис
  fact = facts[(B, row.param_id, V)]
  if fact is None:                        не наследуем; причина `baseline_fact_missing`
  elif fact.review is not ACCEPTED:       не наследуем; причина `baseline_fact_{review}`
  elif fact.status is not PRESENT:        не наследуем; причина `baseline_fact_{status}`
  else:                                   ячейка с evidence из факта (url + quote + content_hash)
```

Три исхода «не наследовали» публикуются **по продукту и параметру** в
`inheritance_basis` (расширение существующего отчёта, не новый носитель). Это важно:
«объявлено 181, выпущено 140» без причин читается как дефект конвейера, а не как «41 факт
ещё не разведан».

### 6.3. Evidence из факта

```python
CellEvidence(
    source_kind=SourceKind.INHERITANCE,
    evidence_type=EvidenceType.DERIVED,
    source_id=f"upstream://{baseline_id}#{param_id}@{version}",   # версия теперь в адресе
    source_url=fact.url,
    citation_quote=fact.quote,          # I22: непустая и дословная
    content_hash=fact.content_hash,     # I19: хеш текста чанка, а не URL
)
```

`@version` в `source_id` — не косметика: один параметр в v11 и v18 — два разных факта, и
адрес обязан их различать (I15: детерминированный lookup).

### 6.4. Фолбэк на паспортную цитату — убирается

`direct` без reference-продукта **не производится**. Линтер блокирует пакет, где
`baselines.csv` непуст, а `reference_products.yaml` не объявляет нужный `baseline_id`:

```
baseline-without-reference-product: строка (upstream-pg, pg_json_type) наследует от
baseline_id 'upstream-pg', но reference-продукт для него не объявлен (§27.6.4)
```

Это осознанно жёстко: фолбэк на одну цитату — тот самый способ, которым 181 ✅
превращаются в фикцию. Пустой `baselines.csv` (`ru-pg-dbms` сегодня) правило не задевает.

---

## 7. Идентичность и версии

### 7.1. Два новых поля контракта

`EXECUTION_CONTRACT_FIELDS`: 32 → **34**.

| поле | материал дайджеста | почему в контракте |
|---|---|---|
| `baselines_digest` | отсортированные `(baseline_id, param_id, min_version, inheritance_mode)` | **дефект сегодня**: правка `baselines.csv` меняет вердикты под тем же `run_id` |
| `baseline_facts_digest` | отсортированные `(baseline_id, param_id, version, status, review, sha256(quote))` | определяет, какие ячейки наследуются |

Не входят в дайджесты (провенанс, не измерение): `observed_at`, `corpus_build_id`,
`reviewed_by`, `note`, `url`. `url` исключён намеренно: переезд документации на другой
адрес при том же тексте и той же цитате вердикт не меняет, а `content_hash` уже
фиксирует текст. `review` **входит**: снятие `accepted` меняет набор унаследованных
ячеек.

Оба поля — `execution_change` (блокируют межцикловый diff с прежним `run_id`, требуют
`golden-full` при первом прогоне). Разово и явно, не «под капотом».

### 7.2. Версии

* `upstream.version` в паспорте приводится к **мажорной** версии для поиска факта:
  `"18.4.1"` → `18`. Нечисловая версия — **линт-ошибка**, а не «пропустим»: молчаливый
  пропуск вернул бы ровно ту проблему, из-за которой `_satisfies_min_version` написан
  мягко.
* Факт ищется **ровно на объявленной версии**, не «на любой ≤ V». Разница
  содержательная: если в v18 параметр исчез, а в v17 был, наследовать нельзя — это
  регрессия upstream, и она должна быть видна, а не замазана поиском «где-нибудь было».
* `min_version` — **пол применимости строки** («параметр существует начиная с v11»), а не
  диапазон поиска. Семантика не меняется.

### 7.3. Схема БД

`SCHEMA_VERSION` → **3.21.0** (аддитивно, без `ALTER` при открытии — версия читается до
DDL):

```sql
-- пер-продуктовый учёт наследования: почему не наследовали
ALTER TABLE run_product ADD COLUMN inheritance_planned   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE run_product ADD COLUMN inheritance_released  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE run_product ADD COLUMN inheritance_skipped_json TEXT;
```

(Объявлено в `CREATE TABLE`; слово `ALTER` здесь — описание смысла для читателя, не
команда миграции.)

`baseline_facts` **не** попадает в run-DB: это вход, а не результат прогона. Прогон читает
CSV и публикует счётчики.

---

## 8. Разведка: поток и граница LLM

### 8.1. Шаги

```
1. Декларация      reference_products.yaml + corpus_urls.yaml версии (автор)
2. Краул           те же краулер/SSRF/robots/scope, что у продукта, но корни — свои
3. Экстракция      тот же extractor_factory(level, pdf_pipeline) — объявленный, не дефолт
4. Чанкинг         тот же чанкер (chunker_version в manifest)
5. Лексический     tantivy по чанкам (BM25); векторы НЕ нужны (§8.4)
6. Кандидаты       для каждого param_id каталога: запрос из title + rule + concept_terms
7. Отбор цитаты    (a) детерминированно: чанк, где встречаются все термины правила;
                   (b) опционально LLM — выбрать чанк и вернуть дословную цитату
8. Проверка        quote ∈ chunk.text (как I22); иначе факт = unclear + причина
9. Запись          sources/baseline_facts.csv со status=pending, review=pending
10. Человек        hound baseline review --accept|--reject
```

### 8.2. Граница LLM — та же асимметрия, что у разведки

LLM здесь — **разведочный** контур: он выбирает, *что* цитировать, но не выносит вердикт о
продукте. Поэтому:

* модель читается из `COHOUND_LLM_*`/слота, как у `package scout` (args > env > slot), и
  **не** входит в `run_id` — ровно как у discovery (AGENTS §8, намеренная асимметрия);
* выход — кандидат с `review=pending`; цитата проверяется механически;
* без LLM шаги 1–9 работают (детерминированный отбор), просто качественнее промахи;
* LLM **никогда** не пишет `status=present` без цитаты: нет цитаты → `unclear`.

### 8.3. Что считается «present»

`present` = в документации объявленной версии есть место, дословно описывающее параметр,
и это место цитируется. Это **не** утверждение «функция работает» — ровно как у
`direct`-наследования по регламенту («переносит возможность проверить применимость»).

`absent` = проход по корпусу версии не нашёл такого места **и** охват корпуса полный
(иначе `unclear` — та же дисциплина, что I2/I2c: неполнота не даёт отсутствия).

### 8.4. Почему без векторов

Задача — «найти в документации одного продукта одной версии место про параметр P». Это
классический BM25 по корпусу ~1–2 тыс. страниц на версию. Векторы здесь дали бы
приблизительный recall, зависящий от сборки, — то есть ровно то, чего не должно быть в
провенансе факта. Эмбеддинг reference-корпуса — отложено (§12), триггер — измеренная
доля `unclear` выше приемлемой.

### 8.5. Цена (экстраполяция от измеренного)

Замеренный темп краула: **1207 док / 209 с = 5.78 req/s** (`docs.tantorlabs.ru`, 07.11.2026).
Документация PostgreSQL — порядка 800–1200 страниц на мажорную версию; 8 версий ≈
7–10 тыс. страниц ≈ **20–30 мин краула** одноразово, плюс экстракция HTML (дёшево: HTML —
не PDF). Это экстраполяция, а не замер: фактический объём версий не считался.

---

## 9. Обновление: `refresh` и вывод `min_version`

`hound baseline refresh --version 19`:

```
1. recon v19 (если корпуса нет) → факты v19
2. diff против принятых фактов предыдущей версии:
     added      present@19, нет@18          → кандидат строки; предложить min_version=19
     removed    present@18, absent@19       → РЕГРЕССИЯ: не применять, показать человеку
     changed    present в обеих, другая цитата → кандидат на замену цитаты
     unchanged  → ничего
     unclear    → в очередь review
3. печать diff (числа + список параметров), --apply пишет только `added`/`changed`
4. `baselines.csv` НЕ трогается: предложенные `min_version` выводятся отдельным списком
```

Почему `min_version` не пишется автоматически: это авторское решение о применимости
строки, и неверный пол молча **пере**наследует (порог ниже реального) или **недо**наследует
(выше). Оба исхода меняют карту без наблюдения. Предложение + подтверждение человеком —
единственная форма, которая оставляет решение за владельцем профиля (A3).

`removed` — не автоприменение: исчезновение описания может быть (а) реальной регрессией
upstream, (б) переездом документации, (в) дефектом извлечения. Различить это может только
человек, и различие определяет, снимать ли строку из `baselines.csv`.

---

## 10. Исключение из сравнения

Reference-продукт **не попадает** в `cohort.yaml`, `run.yaml:products`,
`sources/corpus_urls.yaml` — поэтому он по построению не входит ни в `cells`, ни в
`Capability`, ни в `Coverage`, ни в `Comparability`. Фильтровать нечего: исключение
обеспечено тем, что reference-корпус — отдельная подсистема, а не продукт прогона.

Линтер (блокирующий): `product_id` из `reference_products.yaml` не может встречаться в
`cohort.yaml` или `run.yaml:products`; `baseline_id` из `baselines.csv` обязан быть
объявлен в `reference_products.yaml`, если строки есть.

Посмотреть на reference-продукт — `hound baseline report` (read-only, прогон не нужен):
покрытие каталога по версиям, распределение `present/absent/unclear`, принято/на ревью,
кто из продуктов когорты что унаследовал и с какой цитатой.

---

## 11. Тесты

**Ядро (без сети):**
* `baselines_digest` / `baseline_facts_digest`: смена `min_version` → новый `run_id`;
  смена `reviewed_by`/`observed_at` → **тот же** `run_id`; смена `review` → новый.
* Ш4: `direct` + `present` + `accepted` → ячейка с цитатой факта; `pending`/`rejected`/
  `absent`/отсутствие факта → ячейки нет, причина в базисе; `conditional`/`blocked` —
  как раньше (пин).
* I22: цитата факта — подстрока текста чанка; выдуманная цитата отвергается.
* Версии: `"18.4.1"` → факт ищется на 18; `"abc"` → линт-ошибка; факт v17 при объявленной
  v18 не наследуется (регрессия не замазана).
* Линтер: непустой `baselines.csv` без reference-продукта → блок; reference-продукт в
  когорте → блок.
* `refresh`: `added`/`removed`/`changed` классифицируются верно; `removed` не пишется;
  `min_version` предлагается, но `baselines.csv` не меняется.
* `review --accept-all --dry-run` ничего не пишет и печатает список.

**Без сети, на фейковых портах:** recon на 3-страничном корпусе → факт с цитатой;
`absent` при полном охвате; `unclear` при неполном.

**Живой (по разрешению владельца):** recon PostgreSQL 18 → факты, `baseline report`,
и прогон с `direct`-строками: сравнить число унаследованных ячеек и долю
`support_not_about_subject` до/после.

---

## 12. Отложено (с триггером)

| что | почему отложено | триггер пересмотра |
|---|---|---|
| Полная карта reference-продукта (`Capability`/`Coverage`) | другой предмет: сравнение, а не наследование; владелец просил не включать в сравнение по умолчанию | запрос «покажи карту PostgreSQL» как отдельный отчёт |
| Эмбеддинги reference-корпуса | BM25 достаточно для «найти место про параметр»; векторы добавили бы recall-зависимость в провенанс | измеренная доля `unclear` выше приемлемой |
| `sibling`-наследование (архетип B) | `direct` от параллельной линии методологически не обоснован | отдельное решение владельца |
| Автообновление `min_version` | см. §9 | нет: решение остаётся человеческим |
| Инкрементальный recon (только изменённые страницы) | документация версий почти неизменна; полный пересбор 20–30 мин не оправдывает сложности | ≥3 мажорных версий в неделю |

---

## 13. Этапы

Порядок не случаен: пункты 1–2 устраняют дефекты, которые делают любой генератор опасным.

| # | что | класс | проверка |
|---|---|---|---|
| **1** | `baselines_digest` в контракте (32 → 33) | `execution_change` | пин: правка `baselines.csv` меняет `run_id` |
| **2** | `reference_products.yaml` + модель `ReferenceProduct` + линтер (namespace, связь с `baselines.csv`, версия-мажор) | формат пакета | линт-тесты; `ru-pg-dbms` проходит (0 строк) |
| **3** | `baseline_facts.csv` + `baseline_facts_digest` (33 → 34) + `FactStatus`/`FactReview` в словаре | `execution_change` | пин дайджеста; selfcheck словаря |
| **4** | Ш4 читает факты; фолбэк на паспортную цитату снят; причины «не наследовали» в `inheritance_basis` | меняет доказательства | ядро-тесты §11 |
| **5** | `hound baseline list|facts|report` (read-only) | — | CLI-тесты |
| **6** | reference-корпус: `reference.db` + `manifest.json` + лексический индекс; `hound baseline recon` | новая подсистема | recon на фейковом корпусе |
| **7** | `hound baseline review` + `apply_log` | — | тесты записи/`--dry-run` |
| **8** | `hound baseline refresh` (diff, `min_version` как предложение) | — | тесты diff |
| **9** | Разово для `ru-pg-dbms`: 8 версий PostgreSQL, факты, разметка, замер доли наследования | данные | отчёт с числами |
| **10** | Документация: этот план → справочник; `13-manual-artifacts` — соседний механизм; таблица команд в `03` §E | — | якоря 0 |

Пункты 1–3 — `execution_change`; после них прежние `run_id` несравнимы, и это надо
объявить до, а не после.

---

## 14. Вопросы владельцу

1. **`absent` в документации — это факт или отсутствие факта?** Я исхожу из того, что это
   наблюдение (`absent`), но оно **не** даёт `❌` и не подавляет оценку продукта. Если
   нужно «upstream не имеет P ⇒ downstream тоже» — это другой инвариант (отрицание по
   upstream), и он методологический.
2. **Сколько версий хранить?** «Начиная с 11» для PostgreSQL — 8. Общий ли это принцип
   («все мажорные, поддерживаемые вендором») или объявляется профилем явно? Я исхожу из
   второго: список версий — авторский, в `reference_products.yaml`.
3. **`sibling`-линии (архетип B) входят в эту же подсистему или это отдельный план?** Я
   заложил поле `kind`, но наследование от `sibling` запретил.
4. **Нужна ли карта reference-продукта (§12, п.1) сразу?** Она даёт ответ «PG покрывает
   150 из 181», но требует прогона оценки на reference-корпусе — это ещё ~60 мин LLM на
   версию.
