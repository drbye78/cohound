# Руководство пользователя: пакет категории

Пакет категории задаёт сравниваемые свойства, роли продуктов, когорту, правила оценки и входы измерения. Это авторские данные, а не код ядра: пакет должен описывать только проверяемые утверждения. Не заполняйте пробелы правдоподобными догадками. Ядро остаётся категорийно-нейтральным; реальные названия, термины и URL предоставляет эксперт домена.

## Назначение и границы

Пакет — версионируемый профиль категории, из которого coHound строит сравнение публично подтверждаемой функциональности. Он не является паспортом отдельного продукта и не доказывает истинность введённых автором сведений. Паспорт продукта хранится отдельно в `passports/{product_id}/`.

Пакет определяет: архетип и веса модулей; роли; строки каталога и их правила зачёта; состав когорты и якорь; конфигурацию конкретного прогона; при необходимости профили сайтов, маршрутизацию источников, реестры и человеческие аттестации. Пакет не должен обходить CAPTCHA, авторизацию, платные барьеры или ограничения `robots.txt`; работают только с публичными источниками.

Важно различать спецификацию и исполняемый путь. Нормализация текущей реализации требует `run.yaml`, `package.yaml`, `catalog.csv`, `baselines.csv`, `cohort.yaml`. `golden.csv` необязателен для обычных `normalize`/`plan`/`run`, хотя шаблон его содержит; пакетные golden-проверки — отдельный контур. `sources/manual_artifacts.yaml` — дополнительный вход прогона: если файл заполнен, объявленные байты проходят обычный путь retention → extraction → index, а их отпечаток входит в идентичность прогона. Пустой шаблонный файл ничего не добавляет.

## Быстрый старт

Команды ниже используют репозиторный launcher `./hound.py`; фактические опции сверяйте через `./hound.py <command> --help`.

```bash
# Создать пакет из шаблона; допустимые архетипы A, B, C.
./hound.py package new --case-id example_case --archetype B

# Вручную отредактировать packages/example_case/ (см. разделы ниже).
$EDITOR packages/example_case/package.yaml
$EDITOR packages/example_case/catalog.csv
$EDITOR packages/example_case/cohort.yaml
$EDITOR packages/example_case/run.yaml

# Проверить содержимое (incomplete блокирует готовность к прогону).
./hound.py package validate --package packages/example_case

# План ничего не записывает; run.yaml и каталог пакета обязательны.
./hound.py plan --run-yaml packages/example_case/run.yaml \
  --package packages/example_case

./hound.py run --run-yaml packages/example_case/run.yaml \
  --package packages/example_case

./hound.py report --runs-dir runs --run-id RUN_ID
./hound.py review --runs-dir runs --run-id RUN_ID
```

G1, G2 и G3 не блокируют прогон при отсутствии действующего решения: фиксируется политика `not_required`. Явный отказ по любому гейту блокирует; G4 также блокирует при отсутствии действующего одобрения. G1/G2 — внутренние точки аттестации; G3/G4 — интеграционные точки. Для внешнего решения `gate` требует `--source external --external-ref REF --notes TEXT`. Предмет решения привязан `subject_digest`; устаревшее одобрение не проходит. Решение о waiver G3/G4 задаётся в `run.yaml:gates` (`external` по умолчанию или `waived`) и явно отражается в записи/манифесте. Параметры `--until`, `--run-id` и режимы команд смотрите через `--help`.

`plan` — предварительный план без записи. В текущей справке `./hound.py run --help` также перечисляет `run --dry-run`: этот флаг показывает план фаз без записи и исполнения; обычный `run` исполняет конвейер. `report` генерирует артефакты в `runs/{run_id}/outputs/`; `review` открывает очередь ручного ревью, а не заменяет его.

## Жизненный цикл пакета

Подробный перечень файлов, полей и примеров см. в [справочнике состава пакета](package-files.md).

1. Создайте пакет из шаблона или скопируйте `packages/_template/`; зафиксируйте идентификатор и владельцев авторских полей.
2. Определите архетип, модули и их веса, роли и каталог измеримых параметров.
3. Зафиксируйте когорту: ровно один якорь, прочие участники — аналоги. Обоснуйте включение каждого.
4. Согласуйте `run.yaml` как проекцию когорты и выбранный фокус. Объявите источники/домены, необходимые для корпуса.
5. Добавьте при необходимости правила baseline, golden-случаи, профиль сайта, source routing и спецификации реестров.
6. Запустите `package validate`; устраните blocking и incomplete. Предупреждения требуют осознанной оценки.
7. Выполните `plan`, затем прогон и человеческие решения гейтов. После изменений правил, каталога, когорты или условий измерения соблюдайте версионные границы §13; не переиспользуйте slug.

Команды `package module`, `role`, `param`, `cohort`, `baseline`, `scope` помогают редактировать отдельные сущности. Мутирующие операции поддерживают `--dry-run`; используйте их справку и проверяйте diff. `package new` создаёт из шаблона по умолчанию; `--no-from-template` создаёт минимальный каркас.

## Дерево и обязательность файлов

```text
packages/example_case/
  package.yaml                   обязательно
  catalog.csv                    обязательно
  baselines.csv                  обязательно (может быть только заголовок)
  cohort.yaml                    обязательно
  run.yaml                       обязательно для normalize/plan/run
  golden.csv                     опционально для обычного прогона; поставляется шаблоном
  scope_attestation.yaml         опционально; нужно для механизма absence-аттестации
  domain_profiles.yaml           опционально
  crawl_sources.yaml             опционально; в шаблоне отсутствует
  registries/                    опционально; в шаблоне отсутствует
    {registry_id}.yaml
  sources/
    seeds.yaml                   канонический seed-вход, если используется
    corpus_urls.yaml             legacy-вход/fallback
    golden_status.yaml           опциональное объявление состояния golden
    manual_artifacts.yaml        шаблонный файл, не читается обычным load_inputs/run
```

`catalog.lock.csv`, `derived_fields.csv`, `adversarial/`, `sources/render_modes.yaml`, `sources/scout/` и `sources/crawl_probe/` упоминаются в архитектурной раскладке или связаны с отдельными процедурами; это не обязательные входы обычного запуска и не все присутствуют в шаблоне. `sources/seeds.yaml` — канонический формат по архитектуре; загрузчик сохраняет legacy fallback `sources/corpus_urls.yaml`. В актуальных пакетах ещё встречается legacy-файл. Не считайте архитектурно описанный артефакт автоматически подключённым: сверяйте фактическую команду и loader.

## Профильные файлы

### `package.yaml`

Манифест идентифицирует пакет, его методологический профиль и версии авторских данных. Ключевые поля: `case_id` (slug пакета), `package_version`, `archetype` (`A|B|C`), `modules`, `roles`, `baselines`, `baseline_participant`, `jurisdictions`, `registries`, `languages`.

```yaml
case_id: example_case
package_version: "0.1.0"
archetype: B
modules:
  - {module_id: core, weight: 70}
  - {module_id: extensions, weight: 30}
roles:
  - role_id: primary
    definition: "Роль, определённая проверяемыми признаками"
    markers: ["публичный маркер роли"]
    query_patterns: {ru: ["{product} документация"], en: ["{product} documentation"]}
    core_coverage_allowed: true
    multi_instance: false
baselines: []
baseline_participant: null
jurisdictions: [XX]
registries: []
languages: [ru, en]
```

Сумма весов модулей должна быть ровно 100. Роль без `definition` (или иного обязательного авторского содержания) отмечается как incomplete, а не маскируется текстом-заглушкой. `query_patterns` и маркеры помогают описать роль, но не являются доказательством. В архетипе B набор baseline обычно пуст; архетип C несовместим с непустым `baselines.csv`. Идентификаторы модулей/ролей — lowercase slug; использованные идентификаторы иммутабельны: отмена через `deprecated`/`replaced_by`, а не переименование с повторным использованием.

### `catalog.csv`

CSV с ровно 11 колонками в указанном порядке. Заголовок нельзя менять:

```csv
param_id,title,module,group,importance,param_type,rule,absence_rule,role_binding,concept_terms,search_langs
capability_alpha,Функция Alpha,core,capabilities,Кр,fact,"Засчитывается только явно подтверждённое наличие.",explicit,primary,"{""ru"": [""функция alpha""], ""en"": [""alpha feature""]}",ru|en
```

`param_id` — lowercase slug, иммутабельный; `title` — название; `module` должен ссылаться на модуль; `group` — подгруппа; `importance` ∈ `Кр|Вж|Баз|Опц`; `param_type` — `fact|showcase`; `rule` точно определяет границу зачёта; `absence_rule` — `explicit|parameter_reference|distribution_absence|registry_absence`; `role_binding` — роль пакета; `concept_terms` — JSON-объект с массивами терминов RU/EN (в CSV внутренние кавычки удваиваются); `search_langs` — языки через `|`.

Для `showcase` правило должно называть публично проверяемую величину/метрику. Пустые `concept_terms` вызывают warning. Не подменяйте конкретное правило расплывчатой формулировкой: линтер может проверить структуру, не истинность методологического решения.

### `baselines.csv`

Пять колонок: `baseline_id,param_id,min_version,inheritance_mode,note`. Пустой файл с заголовком допустим, в частности для архетипа B. Режимы: `direct`, `conditional`, `blocked`. Строки должны ссылаться на известные baseline и параметр. Наследование помогает проверить применимость upstream-факта, но само по себе не доказывает наличие функции у сравниваемого продукта.

### `cohort.yaml`

Когорта — канонический состав сравнения. В записях используются `product_id`, `role_overlap`, `selection_reason`, `anchor_relation`; у когорты — `case_id`, `frozen_at`, `entries`.

```yaml
case_id: example_case
frozen_at: '2026-10-28'
entries:
  - product_id: product_a
    role_overlap: 1.0
    selection_reason: "Покрывает определённую роль и публично проверяемый профиль"
    anchor_relation: anchor
  - product_id: product_b
    role_overlap: 0.8
    selection_reason: "Сопоставимый аналог по критериям отбора"
    anchor_relation: analog
```

Должна быть ровно одна запись `anchor`; `product_id` уникальны. `selection_reason: null` означает incomplete и блокирует готовность к прогону. После фиксации состава изменение когорты — граница цикла/сопоставимости, не косметическая правка.

### `run.yaml`: проекция когорты и мандат

Минимально задаются `case_id`, `anchor`, `cohort`, `products`, `package`, `focus`, `languages`, `snapshot_date`, `mode`, `ranking_policy`. `products` обязано совпадать как множество со всеми `product_id` из `cohort.yaml`, а `anchor` обязан входить в `products`. Один источник состава — cohort; список products в run лишь проекция.

```yaml
case_id: example_case
anchor: product_a
cohort: example-cohort-2026
products: [product_a, product_b]
package: example_case
focus:
  modules: [core]
  params: [capability_alpha]
languages: [ru, en]
snapshot_date: '2026-10-28'
mode: strict
ranking_policy: capability_ranked
gates: {g3: external, g4: external}
```

`focus` ограничивает модули/параметры рассматриваемого прогона. Дополнительные поля включают `cycle_id`, `cohort_ref`, `refresh_policy`, `llm_slot`, `settings_provenance`, `serp`, `crawl` и политику `gates`. Не переносите сюда настройки развёртывания. Смена некоторых объявленных осей меняет `run_id`; подробности — §13 и архитектура 03 §C/D. Режимы обновления корпуса — раздел «Режимы обновления корпуса» ниже.

### `domain_profiles.yaml`

Необязательный YAML-словарь `host[/path]` → профиль. Строгая схема (`extra=forbid`): неизвестные ключи отвергаются, не добавляйте предполагаемые поля вроде `render` или `index`.

```yaml
docs.example.org:
  fetch: browser       # http | browser
  extract: html        # auto | html | document
  strategy: honest     # auto | honest | hybrid
  wait_for: "main article"
  rate_limit: {min_s: 1.0, max_s: 2.0}
  sitemap: https://docs.example.org/sitemap.xml
```

Также поддерживаются per-site оси лимитов краула, например `max_depth`, `max_documents`, `max_workers`, `extract_workers`, `document_level`, `exclude_paths`, `exclude_documents`, `attachment_formats` и `archive_*` (точные имена см. `core/config/domain_profiles.py`). Настраивайте только измеренно необходимые значения. Приоритет лимитов: deploy < run < сайт. `strategy != auto` требует браузерного fetch. `sitemap` должен быть публичным HTTP(S) sitemap.

Host scope не равен наследованию профиля: URL допускается только для объявленного хоста (корень или объявленный профиль/seed), с соблюдением сегментного path-prefix. Поддомен сам по себе не разрешён; `www.` эквивалентен базовому хосту. Не используйте профиль, чтобы подразумевать не объявленные корни.

### Seeds и `crawl_sources.yaml`

Для корпуса задайте явные корни. Современный канонический источник — `sources/seeds.yaml` (`schema: seeds/1`) с записями URL и метаданными роли/профиля/enabled/state. Legacy `sources/corpus_urls.yaml` имеет форму `{product_id: [url, ...]}` и читается как fallback. Флаг `--urls FILE` полностью заменяет пакетный seed-набор, а не дополняет его; продукт без URL блокирует onboarding. Порядок seed-корней внутри продукта значим.

`crawl_sources.yaml` опционален и отсутствует в шаблоне. Он задаёт маршрутизацию источника по слоям:

```yaml
defaults:
  kind: web
  engine: http
products:
  product_a: {kind: web, engine: scrapy}
sites:
  product_a:
    docs.example.org: {kind: web, engine: http, fetch: http}
```

Иерархия `defaults` → `products` → `sites`; site-key — `host` или `host/path` без схемы URL. Возможны `kind`, `engine`, `fetch`, `extract`, `strategy`, `wait_for`, `rate_limit`, `params`. `params` пока свободный словарь, его схема по движку не валидируется. Это source routing, не разрешение на расширение host scope.

### Режимы обновления корпуса

По умолчанию все корни обновляются автоматически (`auto`). Ограничить обновление можно
командой `hound crawl policy` — запись живёт в `state/{package_id}/state.db` оператора
(append-only), пакет может задавать дефолты в необязательном `crawl_policy.yaml`:

```bash
hound crawl policy set --package example_case --scope host --id docs.example.com \
  --mode scheduled --interval-hours 168 --actor {кто}
hound crawl policy list --package example_case          # эффективный режим по корням
```

- `on_request` — первичный обход автоматический; повторное обновление — только если корень
  назван во `--refresh-scope` прогона.
- `scheduled` — обновление, когда `now − observed_at ≥ interval_hours`.
- `network_denied` — запросов нет вовсе; корпус собирается из уже сохранённого, покрытие
  неполное, и это видно в отчёте (продукт не выпадает).

Разовый выбор без правки политики — флаги прогона: `--refresh offline` (переиспользовать
чанки, ноль запросов) или `--refresh frozen --frozen-source bytes` (корпус из сохранённых
байтов `corpus_raw`, но с текущей экстракцией и нарезкой). Резолв по областям — «наиболее
специфичная выигрывает» (`path` > `root` > `host` > `product`); смена политики или режима
обновления меняет `run_id`. Подробности — `docs/architecture/03-data-config-cli.md` §D.2.2.

### Отсутствие и `scope_attestation.yaml`

Классы absence нельзя считать взаимозаменяемыми:

* `explicit` — явное отрицание вендора; `source_scope` не требуется, но действует проверка покрытия корпуса I2c.
* `registry_absence` — отсутствие в авторитетном реестре; exhaustive фиксируется механически, подпись полноты не требуется.
* `parameter_reference` и `distribution_absence` требуют exhaustive scope и человеческой подписи охвата; без них отрицательный вердикт не публикуется.
* `unobserved`/`not_stated` не дают `❌`.

Файл `scope_attestation.yaml` задаёт процедуру и подписи **per product**:

```yaml
procedure: "Как эксперт проверяет полноту указанного справочника"
attestations:
  - product_id: product_a
    attested_by: reviewer
    attested_at: '2026-10-28T12:00:00+00:00'
    snapshot_date: '2026-10-28'
    notes: "Сверено с полным публичным перечнем"
```

Команда `./hound.py package scope attest --help` показывает параметры; типичный вызов: `./hound.py package scope attest --package packages/example_case --product product_a --actor reviewer --notes "Сверено"`. Для такой подписи нужна объявленная процедура; следите за датой снимка и актуальностью. Durable `profile_attestation` в state DB — design-only: работающего производителя в прогоне нет. Не путайте эту подпись профиля с решениями G1/G2, которые записываются отдельно per-run.

### `golden.csv` и золотые проверки

Шаблон предоставляет `golden.csv` для регрессионного сравнения. Формат: `product_id,param_id,expected_status,expected_evidence_kind,case_class,tier,note`. Используйте tier `fast` и `full`; полный набор должен покрывать ожидаемый `❌`. Golden — проверочная сеть, а не вход, определяющий измеряемый вердикт, и не обязательное условие обычного `plan`/`run`. Пустой/отсутствующий golden означает отсутствие сверки.

`sources/golden_status.yaml` в шаблоне содержит только комментарии, поэтому состояние выводится из наличия `golden.csv`. Для явного объявления обязательно укажите схему:

```yaml
schema: golden_status/1
state: present  # absent | present | declined
```

Если эталон объявляется непригодным, файл `golden.csv` сохраняется:

```yaml
schema: golden_status/1
state: declined
reason: "Эталон размечен для прежнего состава когорты"
declined_by: reviewer
declined_at: '2026-10-28'  # необязательно
```

`state: absent` допустим только при отсутствии `golden.csv`; `present` требует существующий файл. Для `declined` обязательны `reason` и `declined_by`. Не объявляйте противоречивое состояние относительно наличия файла. Golden-проверки запускайте отдельной командой, сверяясь с `./hound.py golden --help`.

### Package-scoped registries

Архитектурно предусмотрены, но не включены в шаблон и пока отсутствуют в пакетах: `registries/{registry_id}.yaml`. Идентификаторы перечисляются в `package.yaml:registries`. Спецификации задают публичный источник-реестр и извлечение фактов; точную схему и поддерживаемые поля сверяйте с `docs/templates/registries/` и загрузчиком текущей версии. Не помещайте секреты в YAML: используйте предусмотренные переменные окружения для секретных заголовков. Реестр — не произвольный локальный список, а источник доказательств.

### Ручные артефакты

`sources/manual_artifacts.yaml` — обычный вход прогона для ручной подачи байтов, которые краулер не смог получить. Заполнение `provide[]` добавляет материалы в корпус через retention → extraction → chunking → index; URL и хост всё равно проходят SSRF/scope-проверки, а ручная подача не является обходом robots, CAPTCHA, авторизации или платного барьера. Подробная схема полей приведена в [справочнике файлов](package-files.md#scope_attestationyaml-и-manual_artifactsyaml).

## `config/deploy.yaml` и `run.yaml`

`run.yaml` — декларация конкретного измерения: когорта, фокус, дата/цикл, метод оценщика, SERP и операционные оси краула. Его контрактные входы могут менять `run_id`. `config/deploy.yaml` — профиль развёртывания: local/split, адаптеры, endpoint-ы, allowlist и app-level настройки. Это не ещё один файл пакета и не место для состава категории. Для краула действует слоёный резолв app (`deploy`) < run < per-site, но часть транспортных настроек остаётся операционной и не является осью измерения.

Модель оценщика должна быть объявлена CLI или слотом `deploy`; `COHOUND_LLM_MODEL` не является входом модели оценщика. Разведка имеет отдельную политику и не превращается в evidence. См. AGENTS.md, 03 §D.5–D.6 и архитектуру deployment.

## Линт, ошибки и диагностика

Линт имеет уровни `blocking`, `warning`, `incomplete`. Blocking — нарушение правил/ссылок/схемы; warning не блокирует сам по себе; incomplete — ещё не заполнено обязательное авторское поле. Создание/редактирование допускает incomplete, но `package validate`, `plan` и `run` не допускают незавершённый пакет. Шаблонные `definition: null` и `selection_reason: null` намеренно требуют заполнения.

Проверки готовности включают уникальные slug и разрешимость ссылок; валидные enum; веса модулей ровно 100; showcase с величиной в `rule`; `products` равные множеству когорты и включение anchor; ровно один anchor; условия absence и процедуры scope; для архетипа C пустые baselines; заполненность авторских полей. `concept_terms` без содержимого обычно warning.

| Симптом | Что проверить |
|---|---|
| `incomplete` | В `package.yaml` заполнены определения ролей; в `cohort.yaml` — обоснования выбора. Не заменяйте `null` текстом `TODO`. |
| веса не равны 100 | Пересчитайте все веса; при изменении модулей проверьте распределение параметров и `focus`. |
| несоответствие products | Сравните `run.yaml:products` с полным множеством когорты; порядок значения не имеет. |
| неизвестный ключ профиля | Удалите ключ либо сверьте точное имя в `core/config/domain_profiles.py`; схема строгая. |
| URL не попал в краул | Проверьте seed, объявленный хост, path-prefix, SSRF/blacklist, затем доменный профиль. Поддомен не наследует право на crawl. |
| `profile_unbound` / `no_seeds` | Проверьте seed-файл, product_id, enabled/state и `profile_key`; операторский `--urls` заменяет весь набор. |
| absence понижен до `❓` | Уточните absence-класс, полноту корпуса и действующую per-product scope-подпись; процедура не заменяет подпись. |
| gate снова блокирует | Проверьте run_id, gate, `subject_digest` и решение именно в БД этого прогона; решение прошлого запуска не переносится. |
| golden не применяется | Проверьте `golden_status.yaml` и формат; обычный run без golden допустим, но это не означает, что проверка прошла. |

Полезные read-only команды: `package show`, `package artifact list`, `package artifact get`, `package run show`; `package validate --json` даёт машинный отчёт. Полная справка: `./hound.py package --help`, `./hound.py package scope --help`, а также `plan/run/gate/report/review --help`.

## Источники и ссылки

* Регламент: [`cohound-spec-v2.md` §4.2](../../cohound-spec-v2.md#42-пакет-категории), [§4.5](../../cohound-spec-v2.md#45-конфигурация-прогона-runyaml), [§5](../../cohound-spec-v2.md#5-конвейер), [§7](../../cohound-spec-v2.md#7-llm-контуры), [§10](../../cohound-spec-v2.md#10-безопасность-и-правовые-рамки), [§11.3](../../cohound-spec-v2.md#113-acceptance-mvp), [§13](../../cohound-spec-v2.md#13-версионирование).
* Архитектура конфигурации и CLI: [`03-data-config-cli.md`](../architecture/03-data-config-cli.md) — особенно §A.1, §C, §D.1–D.6, §E.1.
* [Шаблон пакета](../../packages/_template/README.md) и его файлы — минимальный рабочий образец раскладки, не реальные данные.
* `core/pipeline/normalize.py`, `core/pipeline/lint.py`, `core/config/run_config.py`, `core/config/domain_profiles.py`, `core/config/crawl_sources.py` — актуальное поведение загрузки и валидации.
* [Словарь статусов](../status-dictionary.yaml), [шаблоны реестров](../templates/registries/).
