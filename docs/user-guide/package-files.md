# Справочник файлов и полей пакета

Этот справочник дополняет [обзор и жизненный цикл пакета](package-profile.md): там описаны запуск и границы обычного прогона, здесь — назначение файлов, подтверждённые поля и примеры. Шаблон-источник: [`packages/_template`](../../packages/_template/README.md). Не добавляйте поля, которых нет в текущей схеме; YAML-модели обычно запрещают неизвестные ключи.

## Состав и назначение

| Файл | Назначение и загрузка |
|---|---|
| `package.yaml` | Обязательный манифест категории: модули, роли и методологические метаданные. |
| `catalog.csv` | Обязательный каталог измеряемых параметров; один параметр на строку. |
| `baselines.csv` | Обязательный заголовок и, когда применимо, правила наследования baseline. Может быть без строк данных. |
| `cohort.yaml` | Обязательный замороженный состав продуктов и единственный anchor. |
| `run.yaml` | Обязательный мандат конкретного прогона; products проецируют когорту. |
| `golden.csv` | Проверочные эталонные примеры; необязателен для обычного run, хотя присутствует в шаблоне. |
| `scope_attestation.yaml` | Необязательная процедура и per-product подписи принятия риска охвата для ограниченных выводов об отсутствии. |
| `domain_profiles.yaml` | Необязательные профили хостов и path-prefix для получения и обработки. |
| `crawl_sources.yaml` | Необязательная маршрутизация типа/движка источника; шаблон не содержит. |
| `sources/seeds.yaml` | Канонические корни краула (schema `seeds/1`); legacy fallback — `sources/corpus_urls.yaml`. |
| `sources/golden_status.yaml` | Необязательное явное состояние golden-эталона. |
| `registries/{registry_id}.yaml` | Необязательные package-scoped спецификации публичных реестров, перечисленные в манифесте. |
| `sources/manual_artifacts.yaml` | В шаблоне имеется, но обычный load_inputs/plan/run сейчас его не загружает. Не считать входом обычного прогона. |

Обязательные для нормализации/планирования/обычного запуска пакетные файлы: манифест, каталог, baselines и cohort; кроме того нужен `run.yaml`. Не путайте наличие файла в шаблоне с тем, что команда его реально читает.

## `package.yaml`

Пример — сокращённая форма схемы (значения иллюстративны):

```yaml
case_id: example_case
package_version: "0.1.0"
archetype: B
modules:
  - {module_id: core, weight: 70}
  - {module_id: ecosystem, weight: 30}
roles:
  - role_id: primary
    definition: "Роль, заданная проверяемыми признаками"
    markers: ["публичный признак"]
    query_patterns: {ru: ["{product} документация"], en: ["{product} documentation"]}
    core_coverage_allowed: true
    multi_instance: false
baselines: []
baseline_participant: null
jurisdictions: [XX]
registries: []
languages: [ru, en]
```

* `case_id` — slug пакета; должен совпадать с идентификаторами в связанных файлах.
* `package_version` — версия данных пакета.
* `archetype` — `A`, `B` или `C`; ограничения baseline зависят от архетипа.
* `modules[]`: `module_id` — стабильный lowercase slug, `weight` — вес модуля; сумма весов ровно 100.
* `roles[]`: `role_id` — стабильный slug; `definition` — содержательное определение; `markers` — публичные ориентиры роли; `query_patterns` — поисковые шаблоны по языковым ключам; `core_coverage_allowed` и `multi_instance` — булевы правила роли. Пустые авторские поля — incomplete, а не повод сочинять значения.
* `baselines` — объявленные baseline-идентификаторы; `baseline_participant` — участие пакета в baseline либо `null`. Архетип B обычно использует пустой список; архетип C не допускает непустой `baselines.csv`.
* `jurisdictions` — коды юрисдикций, относящиеся к профилю категории.
* `registries` — идентификаторы файлов `registries/{id}.yaml`.
* `languages` — языки пакета.

Идентификаторы, уже использованные в измерении, не переименовывайте и не переиспользуйте: отменённые сущности выводятся из обращения установленным процессом, а не маскируются новым смыслом.

## `catalog.csv`

Заголовок и порядок колонок фиксированы:

```csv
param_id,title,module,group,importance,param_type,rule,absence_rule,role_binding,concept_terms,search_langs
feature_alpha,Функция Alpha,core,capabilities,Кр,fact,"Засчитывается только подтверждённое наличие.",explicit,primary,"{""ru"":[""функция alpha""],""en"":[""alpha feature""]}",ru|en
```

* `param_id` — иммутабельный lowercase slug; `title` — отображаемое имя.
* `module` ссылается на `module_id`; `group` — группировка параметров.
* `importance` — одно из `Кр`, `Вж`, `Баз`, `Опц`; `param_type` — `fact` или `showcase`.
* `rule` точно задаёт условие зачёта; для `showcase` называет публичную величину/метрику.
* `absence_rule` — один из `explicit`, `parameter_reference`, `distribution_absence`, `registry_absence`; это класс обоснования отрицания, а не формулировка для удобства.
* `role_binding` ссылается на роль из манифеста.
* `concept_terms` — JSON-объект с массивами терминов по языковым кодам; кавычки JSON удваиваются внутри CSV.
* `search_langs` — языки через вертикальную черту.

## `baselines.csv`

Фиксированный заголовок: `baseline_id,param_id,min_version,inheritance_mode,note`. Каждая строка связывает известный baseline с параметром. `min_version` задаёт нижнюю версию источника baseline, `inheritance_mode` — `direct`, `conditional` или `blocked`, `note` — пояснение применения. Для архетипа B допустим только заголовок без строк.

## `cohort.yaml`

```yaml
case_id: example_case
frozen_at: '2026-10-28'
entries:
  - product_id: product_a
    role_overlap: 1.0
    selection_reason: "Обоснование включения по критериям когорты"
    anchor_relation: anchor
  - product_id: product_b
    role_overlap: 0.8
    selection_reason: "Сопоставимый аналог"
    anchor_relation: analog
```

`case_id` связывает когорту с пакетом; `frozen_at` — дата фиксации; `entries` — уникальные продукты. У каждой записи `product_id` — slug паспорта, `role_overlap` — численная оценка перекрытия ролей, `selection_reason` — содержательное обоснование (null означает incomplete), `anchor_relation` — `anchor` или `analog`. Ровно одна запись должна быть anchor.

## `run.yaml`

```yaml
case_id: example_case
anchor: product_a
cohort: example-cohort-2026
products: [product_a, product_b]
package: example_case
focus: {modules: [core], params: [feature_alpha]}
languages: [ru, en]
snapshot_date: '2026-10-28'
mode: strict
ranking_policy: capability_ranked
gates: {g3: external, g4: external}
```

* `case_id` и `package` связывают конфигурацию с пакетом; `cohort` — идентификатор когорты.
* `anchor` обязан быть одним из `products`; `products` должно совпадать как множество с `cohort.yaml`.
* `focus.modules` и `focus.params` ограничивают рассматриваемые модули и параметры.
* `languages`, `snapshot_date`, `mode`, `ranking_policy` задают язык, дату среза, режим и политику ранжирования.
* `gates.g3` и `gates.g4` — `external` или `waived` (по умолчанию external). G1/G2/G3 без действующего решения не блокируют: пишется policy-строка `not_required`; G4 блокирует без действующего одобрения, а явный `rejected` по любому гейту блокирует. Waiver G3/G4 видимо отражается в следе прогона.

Конфигурация может содержать и другие поля, например `cycle_id`, `cohort_ref`, `refresh_policy`, `llm_slot`, `settings_provenance`, `serp`, `crawl`; используйте только поля фактической схемы. Смена контрактных входов создаёт другой `run_id`.

## Файлы корпуса и дополнительные настройки

* `sources/seeds.yaml`: канонические записи корней с URL и метаданными; `schema: seeds/1`. Legacy `corpus_urls.yaml` — отображение `product_id` в список URL. Операторский `--urls FILE` заменяет пакетные seeds целиком. Корни должны принадлежать публичным объявленным хостам; seed сам по себе не разрешает произвольные поддомены.
* `domain_profiles.yaml`: словарь ключей `host` либо `host/path` к профилям. Поддерживаемые оси включают `fetch` (`http|browser`), `extract` (`auto|html|document`), `strategy` (`auto|honest|hybrid`), `wait_for`, `rate_limit` и объявленные в схеме per-site лимиты. Пример и полный список сверяйте с [текущим загрузчиком](../../core/config/domain_profiles.py); неизвестные ключи запрещены.
* `crawl_sources.yaml`: `defaults`, `products`, `sites` задают наследуемую маршрутизацию; поля `kind`, `engine`, `fetch`, `extract`, `strategy`, `wait_for`, `rate_limit`, `params`. `params` сейчас свободный словарь; routing не расширяет host scope.
* `scope_attestation.yaml`: `procedure` описывает процедуру принятия риска, `attestations[]` содержит `product_id`, `attested_by`, `attested_at`, `snapshot_date`, `notes`. Подпись нужна для ограниченного отрицания классов `parameter_reference`/`distribution_absence`; `registry_absence` требует механического exhaustive scope без подписи, `explicit` не требует scope-подписи. При неполном корпусе `explicit`/`authoritative` не теряют силу, bounded требует принятия риска, `unobserved`/`not_stated` не дают `❌`.
* `golden.csv`: колонки `product_id,param_id,expected_status,expected_evidence_kind,case_class,tier,note`; tiers `fast` и `full`. Golden — проверочный набор, не источник вердикта.
* `sources/golden_status.yaml`: `schema: golden_status/1`, `state: absent|present|declined`; для declined обязательны `reason`, `declined_by`, `declined_at` необязателен. `present` требует golden-файл, `absent` — его отсутствие. Необязательный `exempt_products` объявляет waiver по продукту (`product_id`, `reason`, `declared_by`): эталона для продукта нет — и это решение, а не пропуск.
* `registries/{id}.yaml`: только публичные реестровые спецификации; идентификатор включается в `package.yaml: registries`. Схему берите из [`docs/templates/registries`](../templates/registries/), не помещайте секреты в YAML.

### Полная схема `run.yaml`

`RunConfig` запрещает неизвестные ключи. Поля обязательного ядра и дополнительные поля:

| Поле | Семантика и ограничения |
|---|---|
| `case_id` | Идентификатор пакета. |
| `anchor` | Продукт-якорь; обязан входить в `products`. |
| `cohort` | Объявленный идентификатор когорты. Состав проверяется отдельно по `cohort.yaml`. |
| `products` | Уникальные `product_id`; множество должно совпадать с `cohort.yaml`. |
| `package` | Каталог/идентификатор пакета, используемый прогоном. |
| `methodology_version` | Версия методологии; по умолчанию версия проекта. Меняется при изменении семантики методологии, не ради операционных настроек. |
| `focus` | Объект `Focus`: `modules` и `params` задают выбранные модули и параметры; при пропуске применяется пустой объект. Имена должны ссылаться на пакетный каталог. |
| `languages` | Перечень языков, используемых прогоном; по умолчанию пустой список модели. |
| `snapshot_date` | Дата снимка, обязательна. |
| `cycle_id` | Необязательный идентификатор цикла (ISO-дата/неделя); иначе эффективный цикл выводится из даты снимка/состояния. |
| `cohort_ref` | Необязательная авторская ссылка на редакцию/состав когорты. |
| `refresh_policy` | Необязательный режим обновления корпуса: `auto`, `revalidate`, `refresh`, `offline` (корпус из **чанков**), `frozen` (корпус из сохранённых **байтов** `corpus_raw` с текущим извлечением и нарезкой; источник — флаг `--frozen-source bytes\|chunks`, вне `frozen` канонизируется в `n/a`); CLI `--refresh` может переопределять. Входит в `run_id`: смена режима — `execution_change`. |
| `mode` | `strict` или `preliminary`. Для preliminary запрещён `capability_ranked`. |
| `ranking_policy` | Политика ранжирования; значение проверяйте по текущему enum/словарю статусов. По умолчанию `capability_ranked`. |
| `llm_slot` | Необязательный слот оценщика; разрешается композиционным слоем относительно deploy. Модель оценщика должна быть объявлена CLI или deploy, env-дефолта модели нет. |
| `ranking_epsilon` | Необязательный порог неразличимости ранжирования; сохраняется как provenance-настройка. |
| `settings_provenance` | Необязательная карта `setting: {source, experiment_id?, value?, by_level?}`. `source` описывает `default`/`measured`; остальные поля документируют измеренное значение/эксперимент и разрез по уровням. Это провенанс настроек, не произвольный набор осей. |
| `crawl` | Необязательная секция краулера, см. таблицу ниже. Отсутствующая секция сохраняет дефолтное поведение. |
| `serp` | Необязательная секция внешнего поиска, см. ниже. Без неё SERP выключен. |
| `gates` | Политика только для `g3` и `g4`: `external` или `waived`; по умолчанию `external`. G1/G2 нельзя делегировать через конфиг. Политика не входит в execution contract. |

`crawl` принимает следующие поля (неизвестные поля запрещены):

| Поле | Значение |
|---|---|
| `strategy` | `auto`, `honest`, `hybrid`; стратегия браузерного исполнения. Отличается от `fetch` в доменном профиле. |
| `rate_limit` | Необязательный диапазон `{min_s, max_s}` задержки запросов. |
| `max_workers` | Параллелизм сетевого краула, целое ≥1. |
| `extract_workers` | Параллелизм экстракции, целое ≥1; независим от сетевых workers. |
| `extract_max_tasks_per_child` | Рециклинг воркеров экстракции после N задач, целое ≥1. |
| `max_response_bytes` | Максимальный размер одного ответа, положительные байты. |
| `max_artifact_bytes` | Максимальный размер одного принимаемого контейнера, положительные байты. |
| `attachment_formats` | Поддерживаемые форматы контейнеров; нормализуются загрузчиком. |
| `archive_max_member_bytes`, `archive_max_uncompressed_bytes`, `archive_max_members` | Положительные границы на один контейнер/его члены. |
| `archive_max_total_uncompressed_bytes` | Положительные кумулятивные байты на корень спуска. |
| `archive_descent` | Необязательная политика спуска в контейнеры; допустимые значения определяет enum в текущем коде. |
| `max_depth` | Глубина обхода, целое ≥0; 0 означает только seed-корни. |
| `max_documents` | Положительное число документов на seed-root. |
| `revalidate_max_documents` | Положительный предел ревалидации известных URL на продукт; неисполненный хвост видим как stale. |
| `exclude_paths`, `exclude_documents` | Списки непустых glob для путей URL и имён документов. |
| `document_level` | `off`, `text`, `layout`, `layout_tables`, `full`. |
| `pdf_pipeline` | `structured` или `text_layer`. |
| `pdf_device` | `cpu` или `cuda`; per-site аналога нет намеренно. `auto` не поддерживается. |
| `document_timeout_s` | Положительное число секунд на извлечение одного документа. |

Все лимитные оси могут наследоваться с application/deploy слоя, а site-профиль имеет более высокий приоритет, кроме устройства PDF. Смена объявленных осей исполнения может менять `run_id`.

`serp` содержит `rounds` (целое ≥0, по умолчанию 0 — контур выключен), `budget` (целое ≥1 — запросы на продукт на раунд, обязательно при `rounds > 0`) и `mode` (`sync`/`async`; если не задан, используется объявленный кодовый default). Раунды и режим определяют состав внешних находок и относятся к условиям исполнения.

### Seeds: канонический и legacy-форматы

Канонический `sources/seeds.yaml` имеет корень `schema: seeds/1` и `products` — отображение `product_id` к продуктовой записи:

```yaml
schema: seeds/1
products:
  product_a:
    state: ready
    seeds:
      - url: https://docs.example.org/
        normalized_url: https://docs.example.org/
        role: docs
        source: operator
        added_at: '2026-10-28'
        added_by: editor
        enabled: true
        state: enabled
```

Продуктовая запись имеет `state` (`ready`, `pending_seeds`, `blocked`, `retired`) и `seeds[]`; `ready` требует хотя бы один активный seed. Seed-запись: `url` — исходный публичный HTTP(S) URL без userinfo; `normalized_url` — нормализованный URL для дедупликации; `role` — `primary`, `docs`, `satellite` или `host_root`; `source` — `add_product`, `discovery`, `migration`, `operator`; `source_ref` — необязательная ссылка на происхождение; `profile_key` — необязательный ключ `host[/path]`; `added_at` — дата; `added_by` — непустой автор; `enabled` — bool; `last_verified_at` — необязательная дата; `state` — `enabled`, `disabled`, `invalid`. Согласованность обязательна: enabled=true ровно при state=enabled. URL нормализуется (host lowercase, default port и fragment убираются, path сохраняет регистр и slash, query сохраняется с предупреждением); seed-id вычисляется, не задаётся автором.

Legacy `sources/corpus_urls.yaml` — простой mapping `{product_id: ["https://…", ...]}`. Пустое/отсутствующее содержимое даёт пустой набор; каждое значение должно быть списком строк. При миграции в seeds значения получают `role: primary`, `source: migration`; не добавляйте в legacy-формат предполагаемые поля.

### Все поля `domain_profiles.yaml`

Файл — mapping `host[/path]` → строгая модель профиля (`extra=forbid`). Неизвестные ключи ошибочны; `null` означает «ось не задана здесь» и наследуется. Приоритет осей лимитов: deploy < run < site.

| Поле | Семантика / проверка |
|---|---|
| `fetch` | `http` или `browser`; транспорт получения. |
| `extract` | `auto`, `html`, `document`; маршрут извлечения. |
| `wait_for` | Необязательная строка условия готовности для браузерного рендера. |
| `strategy` | Необязательная `auto`, `honest`, `hybrid`; стратегия браузера. Ненулевая стратегия требует browser fetch. |
| `rate_limit` | Необязательный диапазон задержек `{min_s, max_s}`. |
| `max_workers`, `extract_workers`, `extract_max_tasks_per_child` | Необязательные целые ≥1: сетевой параллелизм, параллелизм извлечения и рециклинг воркера соответственно. |
| `max_response_bytes`, `max_artifact_bytes` | Необязательные положительные байты одного ответа и одного контейнера. |
| `attachment_formats` | Необязательный нормализуемый список форматов контейнеров. |
| `archive_max_member_bytes`, `archive_max_uncompressed_bytes`, `archive_max_members` | Необязательные положительные границы одного контейнера. |
| `archive_max_total_uncompressed_bytes` | Положительная кумулятивная граница на корень спуска. |
| `archive_descent` | Необязательная стратегия допуска/спуска; enum из кода. |
| `max_depth` | Целое ≥0, глубина обхода in-site ссылок. |
| `max_documents` | Положительное число документов на корень. |
| `revalidate_max_documents` | Положительный предел ревалидации на продукт. |
| `exclude_paths`, `exclude_documents` | Необязательные списки непустых glob для URL path и basename документа. |
| `document_level` | Уровень `off|text|layout|layout_tables|full`. |
| `pdf_pipeline` | `structured|text_layer`. |
| `document_timeout_s` | Положительное число секунд на документ. |
| `sitemap` | Необязательный непустой HTTP(S) URL sitemap; `<loc>` становятся дополнительными depth-0 seeds, sitemap не индексируется как страница. |

Устройство (`pdf_device`) не входит в профиль домена. Полный источник валидации — [`DomainProfile`](../../core/config/domain_profiles.py).

### `crawl_sources.yaml`: routing-схема

Корневые поля — `defaults` (один источник по умолчанию), `products` (`product_id` → источник) и `sites` (`product_id` → `host[/path]` → источник). Порядок наложения: defaults → product → site. Site-key — host или host/path без URL-схемы. `CrawlSource` принимает только `kind`, `engine`, `fetch`, `extract`, `strategy`, `wait_for`, `rate_limit`, `params`; все прочие ключи отвергаются. `kind` и `engine` — свободные строки, а не перечисления текущего загрузчика; fetch/extract/strategy валидируются enum; rate_limit — диапазон; `params` — свободный словарь, схема конкретного движка пока не проверяется. Routing выбирает обработчик, но не даёт права краулить необъявленный хост.

### Golden: поля строки и статус файла

`golden.csv` требует `product_id`, `param_id`, `expected_status`, `expected_evidence_kind`, `case_class`; `tier` и `note` опциональны. `expected_status` — статусный enum; `expected_evidence_kind` — список SourceKind, который можно записать через `|`/запятые; `case_class` — авторский класс тестового случая; `tier` — `fast` или `full` (по умолчанию `full`); `note` — пояснение. Семантическая проверка состава full-набора (наличие ожидаемого ❌) выполняется lint-ом, не CSV-загрузчиком. Golden — проверка, не источник статуса.

`sources/golden_status.yaml` имеет строгие ключи: `schema` (обязательно `golden_status/1`), `state` (`absent|present|declined`), `reason`, `declined_by`, `declined_at`, `exempt_products`. Если файл объявления отсутствует или содержит только комментарии, состояние выводится из `golden.csv`. `present` требует CSV; `absent` требует отсутствия CSV; `declined` требует непустые `reason` и `declined_by`, сохраняет сам CSV и отключает сравнение; `declined_at` необязательна и должна быть ISO-дата. Лишние ключи запрещены. `exempt_products` — необязательный список waiver'ов по продуктам (`product_id`, `reason`, `declared_by` обязательны, `declared_at` опционален) при любом `state`: у продукта нет ни одной golden-строки, и это решение с причиной и автором, а не пропуск. Без waiver линтер предупреждает, но прогон не останавливает (правило `golden-coverage`, уровень `warning`: эталон не входит в `run_id` и не влияет на вердикты, поэтому гейт на нём запрещал бы первый замер продукта), waiver для продукта с golden-строками или вне когорты — `warning` (`golden-exempt-sync`), отчёт показывает аномалию `golden_exempt_product`. См. [актуальную реализацию статуса](../../core/pipeline/golden_status.py).

### `scope_attestation.yaml` и `manual_artifacts.yaml`

`scope_attestation.yaml` загружается частично совместимым YAML-парсером, а не строгой моделью корня. `procedure` — необязательный текст описания процедуры; `attestations` — необязательный список объектов `ScopeAttestation` со строгими полями `product_id` и `attested_by` (обязательны), `attested_at`, `snapshot_date`, `notes` (необязательны). Даты сохраняются как ISO-текст. В списке нельзя повторять product_id; если когорта читается — продукт должен быть в ней. `procedure` и подписи — отдельные значения; наличие процедуры само по себе не подписывает продукт. CLI для добавления подписи: `./hound.py package scope attest --help`.

`sources/manual_artifacts.yaml` — schema `manual_artifacts/1`, ключ `provide` — список. У каждой подачи обязательны `product_id`, `url`, `path`; опциональны `sha256`, `reason`, `provided_by`, `provided_at`. `product_id` должен быть в когорте, пара product+URL уникальна; URL проходит ту же SSRF-валидацию, хост должен быть объявлен. `path` — существующий файл относительно корня пакета, абсолютный путь и выход за пакет запрещены. `sha256`, если задан, сверяется; хеш байтов всё равно вычисляется и входит в идентичность прогона. Reason и предоставивший/дата — аудит-провенанс. Этот вход сейчас читается загрузчиком ручных артефактов; он не обходит robots/CAPTCHA/авторизацию и не удаляет факт отказа краулера. Байты проходят обычный путь retention→extract→index.

### Registry: пределы схемы

Спецификации живут в `packages/{case_id}/registries/{registry_id}.yaml`; список `registry_id` объявлен в `package.yaml:registries`. В отличие от некоторых остальных полей, полный справочник уже поддерживается в [документации шаблонов реестров](../templates/registries/README.md): обязательные `registry_id`, `jurisdiction`, `endpoint`, `format`, `mapping`; опциональные `method` (только GET), `query_param`, `records_path`, `headers`, `headers_env`, `timeout`. Схема строгая; `params` и предполагаемые поля не переносите из crawl routing. Секретные значения не записывают в YAML — `headers_env` ссылается на имя переменной окружения.

Решения G1/G2 и интеграционные G3/G4 — не содержимое файлов пакета: они записываются в БД конкретного прогона и привязаны к предмету решения. Подробнее — [обзор пакета](package-profile.md#быстрый-старт) и [инструкции репозитория](../../AGENTS.md#7-gates-g1g4).
