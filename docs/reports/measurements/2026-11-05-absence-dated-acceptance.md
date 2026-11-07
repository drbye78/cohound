# Датированное отсутствие: приёмка F3 (план 20 §11.9)

**Дата:** 2026-11-05
**Код:** F3 в рабочем дереве от `9c77abd` (методология 0.2.0 → 0.3.0, SCHEMA 3.19.0 → 3.20.0)
**Стенд:** `COHOUND_HOME=/tmp/opencode/demo3` (пакеты/паспорта — из рабочего дерева,
`COHOUND_NO_DOTENV=1`), `state.db` и run-DB созданы с нуля
**Пакет:** `packages/demo-absence/` — стенд, не кейс: один продукт (`tantor`), четыре параметра
с дословными отрицаниями вендора, одна страница корпуса
**Модель:** `deepseek-v4.1-flash:purple` (слот `cheap`), эмбеддер `BAAI/bge-m3` (1024), `cpu`

Это **повторный** замер того же стенда: первый (`2026-11-05-absence-dated-demo.md`) выполнен
на коде **до** F3 и показал, что `frozen` теряет ячейки целиком. Здесь — что стало после.

## Команды (те же, что в первом замере)

```bash
export COHOUND_HOME=/tmp/opencode/demo3 COHOUND_NO_DOTENV=1
./hound.py run --run-yaml packages/demo-absence/run.yaml --package packages/demo-absence \
  --urls packages/demo-absence/sources/corpus_urls.yaml --only-product tantor \
  --until evaluate --document-level text --follow
./hound.py run --run-yaml packages/demo-absence/run.yaml --package packages/demo-absence \
  --urls packages/demo-absence/sources/corpus_urls.yaml --only-product tantor \
  --refresh frozen --frozen-source bytes --until evaluate --document-level text --follow
```

`run_id` детерминирован от объявленных входов и потому тот же, что в первом замере:
`demo-absence-99bc46d48b46` (сеть) и `demo-absence-f7e5c29c0ad1` (`frozen`).

## Результат

| проход | документ | покрытие | ячейки | `absence_as_of` |
|---|---|---|---|---|
| `auto` | `live`, HTTP 200, 1 док / 5 чанков | `complete=1`, `expected_sources=1`, `observed_sources=1` | **4 × `❌`**, `evaluator:explicit` | `2026-11-05T11:38:26.324196+00:00` |
| `frozen --frozen-source bytes` | `reused`, `http_status=NULL`, те же 5 чанков | `complete=1`, `expected_sources=0`, `observed_sources=0`, `reason=frozen_no_network_observation` | **4 × `❌`**, `evaluator:explicit` | **та же** до микросекунды |

Снятие корня в обоих прогонах — одно и то же (`3a70722dd79b171fd666400a`, `complete=true`,
`observed_how=live`, `policy_digest=22fe05f6ae13ea56d370e258`, `policy_matched=true`);
`corpus_artifact` во втором прогоне несёт унаследованное наблюдение
(`observed_at=2026-11-05T11:38:26.324196+00:00`, `capture_id=3a70722d…`).
Сети во втором проходе — ноль запросов.

**Сравнение с первым замером (тот же стенд, код до F3):**

| проход | до F3 | после F3 |
|---|---|---|
| `auto` | 4 × `❌` | 4 × `❌` (`absence_as_of` записана в ячейку) |
| `frozen` | 4 × `failed`, `status=null`, `no_sources` | **4 × `❌`** с той же датой |

То есть измеренный дефект («нет сети» читалось как «нет наблюдений») закрыт, и публикуемое
утверждение теперь несёт дату: `absence_as_of` = дата наблюдения цитируемого документа, а не
момент прогона.

## Что этот замер **не** проверяет

* **Ветка `bounded`.** В пилоте и на стенде все параметры `explicit`; `bounded` покрыт только
  синтетическими тестами (`tests/pipeline/test_absence_dated.py`): два корня с разными датами
  ⇒ `min`; корень только с `adopted`-снятием ⇒ `❓` даже при подписанном `scope_acceptance`.
  Живой пакет с `parameter_reference` — следующий шаг, отдельной работой.
* **Расхождение `policy_digest`.** Проверено тестом (`policy_matched=False` ⇒ `complete=False`),
  на стенде отпечатки совпадают, поэтому живой ветки не было.
* **Правило `reuse_unconfirmed`** снято вместе с носителем; «до/после» по нему не измерялось и
  теперь не нужно — смешанный корпус (E.6 плана 19) остаётся отдельным вопросом.
* **Цена вызова оценщика** — по-прежнему не измерена (`evaluator-benchmark` не прогонялся).
  *(Уточнение 07.11.2026: на этой дате — верно; первый замер цены и κ —
  `2026-11-07-reasoning-effort-matrix.md`, 50 ячеек: 5 497 токенов промпта + 1 717 completion
  на ячейку, p50 14.8 с.)*

## Воспроизведение

```bash
.venv/bin/python - <<'PY'
import sqlite3, glob, json
for db in sorted(glob.glob("/tmp/opencode/demo3/runs/*/cohound.db")):
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True); c.row_factory = sqlite3.Row
    print(db.split("/")[-2],
          [(r["param_id"], r["status"], r["absence_as_of"]) for r in
           c.execute("select param_id, status, absence_as_of from cells order by param_id")])
    print("  coverage:", [(r["complete"], r["expected_sources"], r["reason"], r["roots_json"])
                          for r in c.execute("select complete, expected_sources, reason, roots_json from corpus_coverage")])
PY
```