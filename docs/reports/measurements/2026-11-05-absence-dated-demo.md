# Датированное отсутствие: демонстрационный замер (F3 плана 20)

**Дата:** 2026-11-05
**Срез дерева:** `88eab5c` + незакоммиченный стенд `packages/demo-absence/`
**Стенд:** изолированный `COHOUND_HOME=/tmp/opencode/demo2` (пакеты/паспорта — из рабочего
дерева, `COHOUND_NO_DOTENV=1`), `state.db` и run-DB созданы с нуля
**Пакет:** `packages/demo-absence/` — **стенд, не кейс**: один продукт (`tantor`), четыре
параметра с явными отрицаниями вендора, одна страница корпуса
**Модель:** `deepseek-v4.1-flash:purple` (слот `cheap`, `config/deploy.yaml`), эмбеддер
`BAAI/bge-m3` (1024), устройство `cpu`

Замер отвечает на один вопрос, от которого зависит F3: **что сегодня происходит с `❌`, если
тот же корпус прочитан без сети** — и число этого «что» надо получить, а не предположить.

## Стенд

Страница: `https://docs.tantorlabs.ru/tdb/ru/18_6/se/logical-replication-restrictions.html`
(«29.8. Ограничения», Tantor SE 18.6). Вендор перечисляет отсутствующее сам:

| `param_id` | цитата вендора | статус в `auto` |
|---|---|---|
| `logrep_ddl` | «Все схемы базы данных и команды DDL не реплицируются.» | `❌` |
| `logrep_sequences` | «Данные последовательности не реплицируются.» | `❌` |
| `logrep_large_objects` | «Все объекты большого размера (см. Глава 32) не реплицируются. Для этого нет обходного пути…» | `❌` |
| `logrep_relation_types` | «Репликация поддерживается только для таблиц… Попытки тиражировать другие типы отношений… приведут к ошибке.» | `❌` |

Каталог пилота (`ru-pg-dbms`) для этого замера не годится: все 181 его параметра —
`absence_rule: explicit`, и ни один не совпадает с публичным текстом, где вендор явно
говорит «не поддерживается» (проверены `tantorlabs`, `diasoft`, `jatoba`, `postgrespro`;
у `diasoft` документация отдаётся только JS-приложением и в `request`-фетче текста нет).

## Команды

```bash
export COHOUND_HOME=/tmp/opencode/demo2 COHOUND_NO_DOTENV=1
# проход 1 — сеть
./hound.py run --run-yaml packages/demo-absence/run.yaml --package packages/demo-absence \
  --urls packages/demo-absence/sources/corpus_urls.yaml --only-product tantor \
  --until evaluate --document-level text --follow
# проход 2 — те же байты, ноль запросов
./hound.py run --run-yaml packages/demo-absence/run.yaml --package packages/demo-absence \
  --urls packages/demo-absence/sources/corpus_urls.yaml --only-product tantor \
  --refresh frozen --frozen-source bytes --until evaluate --document-level text --follow
```

`run_id`: проход 1 — `demo-absence-99bc46d48b46`, проход 2 — `demo-absence-f7e5c29c0ad1`
(`refresh_policy` входит в контракт, поэтому это два прогона, а не две записи одного).

## Что получилось

**Проход 1 (`auto`).** Корпус: 1 документ (HTTP 200, `live`), 3782 символа, 5 чанков;
эмбеддинг bge-m3 **5/5 чанков за 16 с** (кэш 0 %); композиция
`cb_48000edf8daf3b79c5ce1afd`, партиция `pb_e2404da2f3f8be3db832882c`. Снятие —
`3a70722dd79b171fd666400a`, `complete=true`, `observed_how=live`.
`evaluate`: **4 из 4 ячеек `evaluated` со статусом `❌`**, `denial_strength: explicit`,
у каждой цитата вендора (таблица выше). 4 вызова оценщика, 64 663 байта промптов.
Неблокирующая заметка `denial_quote_without_markers` (ни одна цитата не содержит маркеров
отрицания — цитатой оставлена первая в evidence): статус не меняет, но видна.

**Проход 2 (`frozen --frozen-source bytes`).** Тот же документ (`reused`, `http_status=null`),
те же 5 чанков, **та же композиция и партиция**, снимок корпуса унаследовал то же снятие
`3a70722d…` (`complete=true`), `corpus_as_of` совпадает до микросекунды. Сети — ноль запросов.
`evaluate` дал:

```
error: tantor: no_sources: expected_sources=0 (frozen_no_network_observation) — оценка невозможна
note: evaluate: carry=on  |  cells_measured=0 cells_carried=0
```

и в БД — **4 ячейки `failed`, `status = null`**, `failure_kind = no_sources`,
`denial_strength: instrument_failed`. Покрытие: `complete=0`, `expected_sources=0`,
`observed_sources=0`, причина `frozen_no_network_observation`.

## Вывод — и он не тот, что записан в плане

План 20 §11.6 предполагал, что `frozen` «превращает все `❌` в `❓`». **Замер этого не
подтвердил: ячейки теряются целиком.** Тот же текст, тот же индекс, то же удостоверяющее
снятие — а карта вместо четырёх `❌` показывает четыре `failed` (`status = null`), то есть
по I4/I5 материал уходит в знаменатель Coverage как неразрешённый, а из Capability
исключается. Отказ идёт **раньше** правила `reuse_unconfirmed`: `no_sources` срабатывает на
`observed_sources = 0`, потому что покрытие считается по элементам **сеанса**
(`core/pipeline/corpus_coverage.py:31`), а у прохода без сети элементов нет. Перенос из
предыдущего прогона тоже отказывает: `carry_forward` не переносит при
`coverage.complete = False` (`core/pipeline/carry_forward.py:476`), отсюда `cells_carried=0`.

Следствия для F3 (и они шире одного инварианта):

1. **Одного I2d мало.** Датированный носитель не поможет, если покрытие объявляет
   «наблюдений не было» и оценка вообще не запускается. Ветка §7 F3 «
   `derive_corpus_coverage`: полнота считается по снятиям, а не по элементам сеанса» —
   не сопутствующая правка, а **условие работоспособности** I2d.
2. **Перенос ячеек — третье место с тем же условием.** `carry_forward` запинен на
   `coverage.complete`; без пересмотра он продолжит терять ячейки на смешанном корпусе.
3. **Правило `reuse_unconfirmed` этим замером не проверено.** Оно требует полного покрытия
   **и** `reused`-документа одновременно, то есть смешанного корпуса (часть корней
   обновлена сетью, часть взята из сохранённого) — это открытый пункт **E.6** плана 19.
   На чистом `frozen` оно недостижимо: до него не доходит исполнение.

## Что этим замером не измерено

* **Цена вызова оценщика** — не измерена (4 вызова дают только грубую нижнюю границу:
  64 663 байта промптов на 4 ячейки); `hound experiment evaluator-benchmark` по-прежнему
  не прогонялся.
* **Ветка `bounded`** (`complete`-снятие, `scope_acceptance`) — в пилоте недостижима
  (все параметры `explicit`), нужен отдельный пакет с `parameter_reference`.
* **Смешанный корпус** (E.6) — единственное место, где правило `reuse_unconfirmed`
  действительно решает, и оно не проверено.

## Воспроизведение

```bash
.venv/bin/python - <<'PY'
import sqlite3
for db in ("/tmp/opencode/demo2/runs/demo-absence-99bc46d48b46/cohound.db",
           "/tmp/opencode/demo2/runs/demo-absence-f7e5c29c0ad1/cohound.db"):
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    print(db, c.execute("select param_id, state, status, failure_kind from cells").fetchall())
    print("  coverage:", c.execute("select complete, expected_sources, observed_sources, reason from corpus_coverage").fetchall())
PY
```