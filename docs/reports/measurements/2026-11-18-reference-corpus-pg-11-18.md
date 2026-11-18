# Reference-корпус PostgreSQL 11–18: сборка и цена recon

**Дата:** 18.11.2026 · **Прибор:** `hound baseline build` / `hound baseline recon`
(`core/pipeline/reference_build.py`, `core/pipeline/reference_corpus.py`) ·
**Пакет:** `ru-pg-dbms` · **Линия:** `upstream-pg` (`postgresql`) ·
**Хост:** WSL2, 6 860 MiB, 12 threads, БП (режим объявлен оператором).

Вопрос замера — **этап 11 плана 27**: собрать reference-корпус линии предка и узнать,
сколько параметров каталога в нём подтверждается. До 18.11.2026 живых сборок не было ни
одной (только синтетика тестов), `baseline_facts.csv` был пуст.

## 1. Что собрано

Все восемь версий линии, по одному объявленному корню
`https://www.postgresql.org/docs/{v}/` (`docs_roots` в `sources/reference_products.yaml`),
глубина без ограничения (граница — path-prefix корня), `max_documents` без потолка.

| версия | build_id | документов | чанков | охват | сборка |
|---|---|---|---|---|---|
| 11 | `ref_postgresql_11_bc08e7dc` | 1 100 | 8 370 | полный | 4 мин 57 с |
| 12 | `ref_postgresql_12_4cbe5bd5` | 1 108 | 8 540 | полный | 5 мин 08 с |
| 13 | `ref_postgresql_13_c6c6d696` | 1 116 | 8 706 | полный | 4 мин 31 с |
| 14 | `ref_postgresql_14_2ceec963` | 1 141 | 9 252 | полный | 4 мин 24 с |
| 15 | `ref_postgresql_15_8a5312b2` | 1 151 | 9 369 | полный | 5 мин 15 с |
| 16 | `ref_postgresql_16_fa386a57` | 1 151 | 9 492 | полный | 5 мин 28 с |
| 17 | `ref_postgresql_17_df340374` | 1 131 | 9 605 | полный | 8 мин 54 с |
| 18 | `ref_postgresql_18_3b7052ee` | 1 136 | 9 763 | полный | 5 мин 27 с |

Итого 8 934 документа / 73 097 чанков, 354 MB на диске
(`~/.cohound/stands/ru-pg-dbms/reference/ru-pg-dbms/postgresql/`). `complete=true` у всех
восьми: обход исчерпал объявленный корень, а не упёрся в бюджет.

Пропуски по версиям (`duplicate_text` / `out_of_scope`): 26/22 730, 24/24 302, 24/24 309,
19/25 891, 19/25 767, 20/25 640, 14/25 195, 14/25 110. `out_of_scope` в 20 раз больше
числа документов — это нормальная арифметика дерева `/docs/{v}/`: каждая страница ссылается
на десятки других версий и разделов (`/docs/9.6/`, `/docs/devel/`, `/list/`), и они
отсекаются на границе корня. `duplicate_text` — страницы, чей текст совпал с уже взятым
(например, алиасы `index.html` и каталог того же раздела).

Стоимость одной версии — **4.4–5.5 мин** (17 выбилась в 8.9 мин: сеть, а не размер — она
на 26 документов меньше 16-й). Это цена этого хоста на этом хосте, не свойство линии.

## 2. Что показал recon: 45 `present`, из них подтверждаются 2

`hound baseline recon --baseline upstream-pg --version 18 --build ref_postgresql_18_3b7052ee`:

```
9763 чанков, 1136 документов, индекс bm25-py-2 (top_k=20)
параметров: 181; present 45, unclear 136
```

> **Числа этого раздела — запись прогона 18.11.2026 до смены правила зачёта.** В тот же
> день `select_quote` переведён со слова на фразу (`parameter_phrases`), и на том же
> корпусе recon даёт **`present` 37, `unclear` 144** при 37 уникальных цитатах и 0 общих
> (`2026-11-18-recon-matching.md` §4). Раздел оставлен как есть: он описывает, почему
> правило пришлось менять, и его цитаты — доказательство дефекта, а не текущее состояние.

Ручная проверка **всех 45** цитат: параметр подтверждают **2**.

| param_id | цитата | url |
|---|---|---|
| `xlsx_129_…_json_json_table` | `JSON_TABLE #` | `/docs/18/functions-json.html` |
| `xlsx_132_…_full_text_search` | `dict_xsyn — example synonym full-text search dictionary #` | `/docs/18/dict-xsyn.html` |

Остальные 43 — ложные `present`. Три характерных:

| параметр | цитата | почему ложь |
|---|---|---|
| Версия PostgreSQL в качестве основы | `- createuser — define a new PostgreSQL user account` | совпало слово `postgresql`; страница про утилиту `createuser` |
| Поддержка DBaaS/Cloud | `thanks Daniel Firer, as part of zeroday.cloud, for reporting this problem` | совпало `cloud`; это благодарность в release notes |
| Совместимость с Oracle PL/SQL | `Oracle Compatibility Mode #` | совпало `oracle`; раздел про ECPG, а не PL/SQL |

## 3. Причина: `concept_terms` каталога — не понятия

Цитата отбирается по терминам параметра (`parameter_terms`): совпал термин — кандидат
найден, цитата обязана быть дословной подстрокой (I22 соблюдается). Отказ — в составе
терминов, и он двойной.

**Первое, уже исправлено** (`351a07e`): термины брались ещё и из `rule` — служебной
формулировки каталога («…в исходной comparative **table**…»). Слово `table` находилось в
таблице стандарта SQL: «Разработчик» подтверждался цитатой
`| X040 |  | Basic table mapping |  |`. Убрали `rule`: 49 → 45 `present`, ложные остались.

**Второе, не исправлено**: `concept_terms` — это **разбиение `title` на слова**:

```
«Поддержка DBaaS/Cloud»        → {'en': ['dbaas', 'cloud'], 'ru': [..., 'поддержка', 'dbaas', 'cloud']}
«Совместимость с Oracle PL/SQL» → {'en': ['oracle', 'pl', 'sql'], 'ru': [..., 'совместимость', 'oracle', 'sql']}
«Версия PostgreSQL в качестве основы (последняя доступная)»
                                → {'en': ['postgresql'], 'ru': [..., 'версия', 'postgresql', 'качестве', ...]}
```

Совпадения **одного общего слова** достаточно. Ужесточение «все `en`-термины в цитате»
оставляет 26 из 45, и из них ~24 всё ещё ложные: у параметра с единственным термином
(`postgresql`) фильтр не работает вовсе — `createuser — define a new PostgreSQL user
account` содержит его целиком.

**Чего из этого не следует.** Не следует «recon не работает»: механизм детерминирован,
`absent` не выводится (проход не аттестует полноту, I2/I2c), охват публикуется, повторный
проход на тех же данных даёт того же кандидата. Следует, что **`concept_terms` каталога не
годится как поисковые понятия** — это авторская работа по 181 параметру, а не автоматика, и
подменить её эвристикой значило бы выдумать провенанс (A2, I22).

## 4. Следствие для доли наследования

Подписаны (`review=accepted`) два факта из 181 — те, чья цитата называет функцию.
`hound baseline report`: **`inherited cells: 0`**. Остальные 179 — `pending`, не наследуют.

Значит **доля наследования на этом профиле пока не измерена**, и это честный результат
этапа 11, а не провал: механизм собран и исполнен на живом корпусе, а вход (каталог
понятий) оказался непригоден. Триггер пересмотра — переделанные `concept_terms` либо
второй профиль, где каталог сразу пишется понятиями.

## 5. Побочно закрытое

`hound baseline report` впервые исполнен на живой сборке (открытый пункт §16.2.4 плана 27
был «`map`/`report --with-map` не исполнялись на реальной сборке»): `rc=0`, `inherited
cells: 0`, факты по версиям перечислены. `map` (карта через оценщика) по-прежнему не
исполнялся — он требует LLM-слота и отдельного замера.

## 6. Как воспроизвести

```bash
export COHOUND_NO_DOTENV=1
export COHOUND_HOME=$HOME/.cohound/stands/ru-pg-dbms
export COHOUND_PATHS_FILE=$HOME/.cohound/stands/ru-pg-dbms/paths.yaml

for v in 11 12 13 14 15 16 17 18; do
  .venv/bin/python -m core.cli baseline build --package packages/ru-pg-dbms \
    --passports-dir passports --baseline upstream-pg --version "$v"
done

.venv/bin/python -m core.cli baseline recon --package packages/ru-pg-dbms \
  --passports-dir passports --baseline upstream-pg --version 18
.venv/bin/python -m core.cli baseline report --package packages/ru-pg-dbms \
  --passports-dir passports
```

Повторный `build` той же версии при неизменном содержимом даёт `reused=true` и ничего не
перезаписывает (`build_id` — функция содержимого, §4 плана 27); версия `18` собиралась
дважды за вечер именно так.
