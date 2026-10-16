# Шаблоны реестровых конфигов (`docs/templates/registries/`)

**Назначение (W6/T6.2).** Здесь лежат **шаблоны-документация** для реестровых конфигов,
которые интерпретирует generic-адаптер `core/adapters/registry_generic.py`. Ядро
категорийно-нейтрально (§1.3): названия реестров, вендоров и предметные термины в
`core/` не попадают — вся специфика внешнего источника живёт в данных.

> **Где живут рабочие спецификации.** Реестровые спецификации — **package-scoped**:
> `packages/{case_id}/registries/{registry_id}.yaml`, перечисленные в `package.yaml`
> (`PackageManifest.registries`). Прежний корневой каталог `registries/` больше не
> является операционным путём — он содержал только шаблоны и был перенесён сюда.
> Загрузка: `core.adapters.registry_generic.load_package_registry_configs(package_dir,
> registry_ids)`; диагностика: `hound doctor --package packages/{case_id}`.

> **Секреты.** Значения токенов/ключей в спецификации **не хранятся** (AGENTS: секреты
> не коммитятся). Для секретных заголовков используйте `headers_env` — отображение
> `имя_заголовка → имя env-переменной`; значение подставляется из окружения в момент
> запроса (переменная не задана → заголовок опускается). Не-секретные заголовки
> (`Accept` и т.п.) остаются в `headers`.

> **Статус примеров.** Файлы `example_*.yaml` — шаблоны с заведомо несуществующим TLD
> `.invalid`; они проходят валидацию схемы, но не являются рабочими конфигами и не
> должны использоваться в прогоне.

## Схема файла `packages/{case_id}/registries/{registry_id}.yaml`

Обязательные и опциональные поля полностью совпадают с pydantic-моделью
`RegistryConfig` в `core/adapters/registry_generic.py` (`extra="forbid"` — лишние ключи
считаются ошибкой; строки обрезаются по краям).

| Поле | Тип | Обяз. | Дефолт | Смысл и как используется движком |
|---|---|---|---|---|
| `registry_id` | str | да | — | Идентификатор реестра; непустой. Совпадает с именем файла. Уходит в `RegistryFact.registry_id`. |
| `jurisdiction` | str | да | — | Юрисдикция реестра (например код региона); непустая. Метаданные/подстановка. |
| `endpoint` | str | да | — | URL-шаблон. Подстановка через Python `str.format`: `{query}`, `{product_id}`, `{canonical_name}`, `{vendor}`, `{jurisdiction}`, ключи `product.extra` и `scope`. Если `query` не задан явно — берётся `canonical_name` либо `product_id`. |
| `format` | `json` \| `html` | да | — | Выбор парсера ответа. Другие значения отклоняются. |
| `method` | str | нет | `GET` | Поддерживается **только** `GET` (адаптер не шлёт тело запроса). |
| `mapping` | dict[str,str] | да | — | Непустое отображение «имя поля → путь в записи». См. ниже. |
| `query_param` | str | нет | `null` | Имя query-параметра. Если задано, к URL добавляется `?<query_param>=<query>` (когда такого параметра ещё нет). |
| `records_path` | str | нет | `null` | Как выделить записи. JSON: dot-path (напр. `items`, `data.records[*]`); при отсутствии — верхнеуровневый список либо весь объект как одна запись. HTML: имя тега (напр. `tr`); при отсутствии — все узлы. |
| `headers` | dict[str,str] | нет | `{}` | **Не-секретные** HTTP-заголовки, передаются внедрённому клиенту. |
| `headers_env` | dict[str,str] | нет | `{}` | **Секретные** заголовки: `имя_заголовка → имя env-переменной`. Значение читается из окружения в момент запроса; пусто/не задано → заголовок опускается. Приоритет над `headers`. |
| `timeout` | float > 0 | нет | `null` | Таймаут (сек). Передаётся клиенту-объекту с методом `get(url, headers, timeout)`. |

### Синтаксис путей маппинга

Пути **относительны записи** (record-relative).

* **JSON** — подмножество dot-path: ключи (`a.b`), числовые индексы (`items[0]`),
  раскрытие списка (`items[*]`). Ведущие `$` и `$.` отбрасываются, поэтому допустимы
  оба стиля: `name` и `$.name`. Не найденный путь — поле просто опускается в результате.
* **HTML** — значение относится к выбранному элементу (`records_path`):
  * `@attr` — значение атрибута (голое имя атрибута трактуется так же);
  * `#text` / `text` / `.` / пусто — нормализованный агрегированный текст узла;
  * `tag` — имя тега.

### R1 (`source_url`)

Для **каждого** возвращаемого `RegistryFact` движок проставляет `source_url` — это
фактический URL запроса (для `GET` он же — URL источника). Поле **не берётся из
маппинга**: даже если в `mapping` есть ключ `source_url`, фактический `source_url` факта
всегда равен запрошенному URL. Именно поэтому любой конфиг с шаблоном, порождающим
непустой URL, удовлетворяет R1 (100% фактов с `source_url`). Абсолютный URL-адрес
полезной ссылки на запись, если он есть в payload, можно дополнительно вынести
отдельным ключом маппинга (напр. `detail_url`).

## Пример JSON-конфига

```yaml
registry_id: example_json
jurisdiction: XX
endpoint: "https://example.invalid/api/search/{query}"
method: GET
format: json
records_path: items
headers:
  Accept: application/json
# Секретные заголовки — из окружения, никогда не значениями в файле:
# headers_env:
#   Authorization: EXAMPLE_REGISTRY_TOKEN
timeout: 30.0
mapping:
  name: "name"
  version: "version"
  registration_id: "id"
  detail_url: "detail_url"
```

## Пример HTML-конфига

```yaml
registry_id: example_html
jurisdiction: XX
endpoint: "https://example.invalid/registry/list"
method: GET
format: html
records_path: tr
query_param: name
mapping:
  name: "#text"
  version: "@data-version"
  detail_url: "@data-href"
```

## Как добавить реальный реестр

1. Создайте `packages/{case_id}/registries/{registry_id}.yaml` (по образцу шаблонов
   выше) и добавьте `registry_id` в `package.yaml: registries`.
2. Замените `registry_id`, `jurisdiction` и `endpoint` на подтверждённые владельцем
   домена значения; при необходимости заполните `headers`/`headers_env`/`timeout`.
3. Приведите `records_path` и `mapping` к реальной структуре ответа.
4. Прогоните валидацию: `hound doctor --package packages/{case_id}` (загрузка
   спецификаций через `load_registry_config`) и `tests/registries/test_registry_configs.py`.

Движок: `core/adapters/registry_generic.py`. Загрузчик package-scoped спек:
`load_package_registry_configs`. Документация: `docs/architecture/01` §7,
`docs/architecture/02` §D.1, `docs/architecture/03` §D.4, `docs/architecture/06` W6/T6.2.
