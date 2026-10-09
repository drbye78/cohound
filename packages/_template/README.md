# Шаблон пакета категории (`packages/_template/`)

**Назначение (T6.5).** Это **скелет** пакета категории со всеми обязательными
артефактами раскладки (§4.2, `03` §A.1). Его копирует владелец домена и наполняет
реальными данными. Сам шаблон данными не является: все значения — плейсхолдеры с
пометкой `# TODO`, а URL/имена в иллюстративных местах используют зарезервированный
TLD `.invalid` (RFC 2606).

> **Статус данных.** Реальные названия продуктов, вендоров и предметные термины
> **поставляет владелец домена** — мы их **не выдумываем** (см. `registries/README.md`
> и `tests/fixtures/e2e/README.md`). Ядро категорийно-нейтрально (§1.3): вся специфика
> живёт здесь, в данных пакета.

## Состав (обязательные 5 + опциональные)

| Файл | Статус | Что заполнить |
|---|---|---|
| `package.yaml` | обязателен | `case_id`, `archetype` (здесь **B** — кросс-линейный, эталона нет), модули с весами (Σ = 100), роли с `definition`/`markers`/`query_patterns`, `jurisdictions`, `languages`. `baselines: []`, `baseline_participant: null`. |
| `catalog.csv` | обязателен | 11 колонок; здесь **5 строк-примеров**, показывающих формат (факт ×4 + showcase ×1). `param_id` — slug нижнего регистра, `concept_terms` — JSON `{"ru":[…],"en":[…]}` (кавычки в CSV удваиваются), `search_langs` — через `\|`. |
| `baselines.csv` | обязателен | Архетип B: **только заголовок** (наследования базовой линии нет). |
| `cohort.yaml` | обязателен | Ровно один `anchor` + аналоги; уникальные `product_id`. |
| `golden.csv` | обязателен | Минимум по одной строке `tier=fast` и `tier=full`; в `full` обязана быть страта `❌` (иначе линтер блокирует, §11.6). |
| `run.yaml` | обязателен для прогона | `products` — **ровно** множество `product_id` когорты (S11); `anchor ∈ products`; `focus` — модули и параметры. |
| `scope_attestation.yaml` | опционален | Процедура аттестации полноты источника для `absence_rule ∈ {parameter_reference, distribution_absence}` (I2b). |
| `domain_profiles.yaml` | опционален | Профили рендера/индексации per domain; корень — словарь словарей. |

## Линтер (`core/pipeline/lint.py`)

Шаблон обязан проходить линтер **без блокирующих и без warning** — это проверяет
`tests/selfcheck.py` (проверка `_data_templates`) и `tests/config/test_templates.py`:

- сумма весов модулей = 100;
- showcase-параметр несёт публичную величину/метрику в `rule`;
- `param_id` — slug нижнего регистра; `products` = когорта; `anchor ∈ products`;
- `golden-full` содержит страту `❌`;
- `concept_terms` заполнены у всех параметров (иначе warning `empty-concept-terms`);
- присутствуют оба тира golden (`fast`/`full`) — иначе warning `golden-presence`.

## Как использовать

1. Скопируйте `packages/_template/` в `packages/{case_id}/`.
2. Замените `case_id: template` и плейсхолдеры `TODO` на подтверждённые данные.
3. Приведите `concept_terms`, `role_binding`, `module` в соответствие манифесту.
4. Прогоните валидацию: `python3 tests/selfcheck.py` и `python3 -m pytest -q`.
