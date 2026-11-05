# Даты актуальности в отчёте: замер F2 плана 20

**Дата:** 2026-11-05
**Срез дерева:** `9e5a1d9` (носитель `corpus_artifact`, SCHEMA 3.19.0) + правки F2 части 2
(`corpus_freshness.yaml`/`cell_freshness.csv`, блок `observation_freshness` в `plan`/`run`),
незакоммиченные на момент замера
**Стенд:** изолированный `COHOUND_HOME=/tmp/opencode/p5f2b` (пакеты/паспорта — из рабочего
дерева, `COHOUND_NO_DOTENV=1`), `state.db` и run-DB созданы с нуля
**Корпус:** `ru-pg-dbms`, продукт `jatoba`, два seed-корня; `--document-level text`,
`--max-depth 0`, `--serp-rounds 0`, `--until corpus`
**Граница замера:** `--until corpus` ⇒ `evaluate` не исполнялся, ячеек нет. Поэтому
`cell_freshness.csv` на этом стенде не создаётся, а `cells` / `cells_without_dated_evidence`
оба нули — и печатаются **вместе**, чтобы ноль не читался как «все ячейки датированы».

```bash
export COHOUND_HOME=/tmp/opencode/p5f2b COHOUND_NO_DOTENV=1
./hound.py run --run-yaml packages/ru-pg-dbms/run.yaml --package packages/ru-pg-dbms \
  --urls /tmp/opencode/p5-urls-jatoba.yaml --only-product jatoba \
  --serp-rounds 0 --max-depth 0 --document-level text --until corpus --follow
```

## Результат (реальная run-DB, не фикстура)

`corpus_artifact`: **9 строк, 0 без наблюдения**; `corpus_freshness.yaml`:

```yaml
reference: '2026-11-05T00:34:20.610795+00:00'
corpus_as_of: {from: '2026-11-05T00:33:23.306820+00:00', to: '2026-11-05T00:34:20.610795+00:00'}
products:
  jatoba:
    corpus_as_of: {from: …00:33:23.306820, to: …00:34:20.610795}
    corpus_age: {artifacts: 9, median_days: 0.0, p90_days: 0.0, max_days: 0.0}
    stale_roots:
    - {root_url: 'https://jatoba.ru/', as_of: …00:34:13.554168, lag_seconds: 7, lag_days: 0.0}
    roots_without_observation: 0
    artifacts_without_observation: 0
    cells: 0
    cells_without_dated_evidence: 0
```

Что здесь измерено, а не предположено:

1. **Даты разные и принадлежат артефактам.** `corpus_as_of` — 57 секунд разброса внутри
   одного прогона (время ответов сервера), `reference` — самое свежее наблюдение, а не
   `now()`: два вызова отчёта на одной БД дают байт-идентичные файлы.
2. **`stale_roots` говорит в секундах, а не в округлённых сутках.** `jatoba.ru` отстаёт от
   `gaz-is` на **7 секунд**: `lag_seconds: 7` при `lag_days: 0.0`. Решение «отстал»
   принимается по точному интервалу (`lag_seconds > 0`), округление — только печать.
3. **Члены контейнеров датированы.** 2 из 9 артефактов — члены
   `documentationJatoba18_6.zip`: у них нет своего `url_state` (наблюдался контейнер), и
   до правки они попадали в `artifacts_without_observation` (**2 из 9**). Теперь дата
   наследуется от внешнего контейнера — того, чьим ответом сервера получены байты:
   **0 из 9**. Вложенный член (`…zip!/…zip!/…pdf`) наследует от **внешнего** архива, а не
   от вложенного.
4. **Возраст мал, потому что корпус свежий.** `median_days/p90_days/max_days` = 0.0 —
   распределение по одним суткам, а не «порог не сработал»: потолков в отчёте нет, число
   публикуется как есть (A3).

## Дефекты, найденные этим замером (закрыты)

1. **Член контейнера считался ненаблюдённым.** `2 из 9` артефактов имели `NULL` в
   `observed_at`/`observed_how`/`capture_id`/`root_url`, хотя байты наблюдались вместе с
   контейнером. Это не косметика: F3 не смогла бы опереться на такую ячейку, а
   `bounded ❌` по корню с контейнером выглядел бы хуже, чем он есть.
2. **Решение «корень отстал» принималось по округлённой величине** (2 знака суток). На
   реальных данных это дало корень в `stale_roots` с `lag_days: 0.0` — вердикт и число
   противоречили друг другу. Теперь решение по `lag_seconds`, а `lag_days` — печать.
3. **Ноль без знаменателя.** `cells_without_dated_evidence: 0` при нуле ячеек читается как
   «все ячейки датированы». Теперь рядом печатается `cells`.

## Что осталось открытым

* **`evidence_as_of` на реальных ячейках не измерен**: для этого нужен прогон с
  `evaluate` (объявленная модель, LLM-вызовы), а этот замер остановлен на `corpus`.
  Приёмка ячеек закрыта подготовленной run-DB в тестах
  (`tests/pipeline/test_report_corpus_freshness.py`: три даты → три даты; `--json` и YAML
  из одного сборщика; детерминизм).
* **`corpus_docs.url`** по-прежнему различается формой записи между живым проходом и
  `frozen` (см. `2026-11-05-corpus-snapshot-observations.md`): на даты это больше не влияет
  (`url_key` нормализован), но представительный адрес документа — вопрос дедупа
  (план 19 §8.3).