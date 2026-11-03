# Pandoc: дифференциальная приёмка по блочной структуре — базовый замер

**Дата:** 04.11.2026. **Класс:** приёмка разбора (сам замер условия измерения не меняет:
`core/` не трогается). **Срез:** план `docs/architecture/18-markdown-chunker-plan.md` §7,
«Дифференциальная приёмка против Pandoc»; коммит `91e3748`. **Инструмент:**
`scripts/conformance_pandoc.py`. **Оракул:** `/usr/bin/pandoc` 2.17.1.1 — только на хосте
замера/в CI, в рантайме проекта его нет.

## Что и как мерилось

Заявление «паритет с Pandoc» проверяется внешним оракулом, а не чтением кода. Инструмент
берёт markdown, получает AST Pandoc (`pandoc -f markdown -t json`) и сравнивает
**последовательность видов блоков верхнего уровня** с
`core.adapters.md_ast.parse_markdown(text, "pandoc")`.

**Режим — `types-only`, и это измеренный вывод, а не удобство.** В 2.17.1.1:

- `pandoc -t json` отдаёт блоки как `{"t": "Header", "c": …}`; ключа `sourcepos` нет ни у
  одного блока, флага `--sourcepos` в этой версии нет (`Unknown option`);
- формата `pandoc` как имени ридера нет: `pandoc -f pandoc` →
  `Unknown input format pandoc`; Pandoc markdown называется `markdown` — он и используется;
- диагностический `--trace` печатает по строке на блок, но это **не спаны**: заголовок из
  строки 1 репортится как `at line 3`, список из строк 5–7 — как `line 9` (номер текущей
  строки после поглощения блока), а содержимое обрезано до 80 символов. Восстановить
  диапазон строк по этому нельзя.

Поэтому точные границы строк и спаны этим инструментом **не проверяются**. Сравнение
видов проверяемо, но слабее: оно ловит пропущенную/лишнюю конструкцию и неверный класс
блока, но не сдвиг границы внутри правильного класса.

Нормализация сравнения объявлена в докстринге инструмента: `Header`↔`heading`,
`Para`/`Plain`↔`paragraph`, `BulletList`/`OrderedList`↔`list`, `CodeBlock`↔`fence`/`code`,
`Table`↔`table`, `DefinitionList`↔`definition_list`, `Div`↔`container`,
`RawBlock`↔`html`, `LineBlock` — без правила, `HorizontalRule`↔`thematic_break`. Отдельно:
`Para` с единственным `Math DisplayMath` сводится к `math` (у нас `$$…$$` — блок);
YAML-метаданные Pandoc держит в `meta`, а не в `blocks`, поэтому `front_matter` снимается
со сравнения; определения сносок у Pandoc — инлайновый `Note`, поэтому `footnote_def`
тоже снимается; подряд идущие `RawBlock` схлопываются (Pandoc режет один HTML-фрагмент).

Конструкция, которую Pandoc распознал, а `md_ast` — нет, попадает в
`unparsed_construct` с названием и счётчиком, а не в «совпало».

**Источники.** 40 **отслеживаемых git** файлов из 68 (`git ls-files`: 65 файлов
`docs/**/*.md` + `AGENTS.md`/`README.md`/`cohound-spec-v2.md` в корне; сортировка по пути,
затем равномерный шаг 68/40) и 19 сгенерированных конструкций — всего 59 документов.
Выборка привязана к коммиту, а не к рабочему дереву: неотслеживаемые файлы параллельных
полос в замер не попадают, иначе числа менялись бы от прогона к прогону.

## Команда

```bash
.venv/bin/python scripts/conformance_pandoc.py --out-json /tmp/pandoc-baseline.json
```

Код возврата базового прогона — **1** (расхождения есть). Блоков Pandoc **2 892**;
4.65 с на 7 GB RAM (подпроцесс `pandoc` — по одному документу последовательно).

## Числа

| класс | документов | блоков | что это |
|---|---:|---:|---|
| `unparsed_construct` | 9 | **11** | Pandoc распознал конструкцию, правила у `md_ast` нет |
| `sequence_length` | 12 | **52** | лишний/недостающий блок при выравнивании видов |
| `type_mismatch` | 0 | **0** | обе стороны дали блок, классы разошлись |
| `oracle_error` | 0 | **0** | отказов `pandoc` нет |

**Разделение по источникам.** На 40 документах дерева — **42 блока в 9 документах**, и все
три причины названы: список без пустой строки перед ним (40 блоков / 8 документов),
цитата без пустой строки перед ней (1 / 1) и один абзац, который Pandoc оставил после
обрыва GFM-таблицы (1 / 1). Остальное — 11 `unparsed_construct` и 10 блоков
`sequence_length` — сгенерированные конструкции, то есть список работы, а не дефект
корпуса. Из 19 сгенерированных форм блочно совпали 10, разошлись 9.

| конструкция | класс | блоков | документов | пример |
|---|---|---:|---:|---|
| список без пустой строки перед ним | `sequence_length` | 40 | 8 | `docs/architecture/00-overview.md:409` |
| цитата без пустой строки перед ней | `sequence_length` | 1 | 1 | `docs/architecture/09-product-onboarding-incremental-runs.md:4674` |
| абзац — остаток GFM-таблицы у Pandoc | `sequence_length` | 1 | 1 | `docs/reports/measurements/2026-11-03-md-chunk-geometry-tokens.md` |
| лишние абзацы таблиц/HTML-div | `sequence_length` | 5 | 3 | `generated:multiline_table:2` |
| example list `(@)` | `unparsed_construct` | 2 | 1 | `generated:example_list:1` |
| fancy list `a)` / `iv.` | `unparsed_construct` | 2 | 1 | `generated:fancy_list:1` |
| «лишний» thematic break таблицы | `sequence_length` | 2 | 2 | `generated:multiline_table:4` |
| grid table | `unparsed_construct` | 1 | 1 | `generated:grid_table:1` |
| multiline table | `unparsed_construct` | 1 | 1 | `generated:multiline_table:1` |
| simple table | `unparsed_construct` | 1 | 1 | `generated:simple_table:1` |
| подпись pipe-таблицы (`:` после таблицы) | `unparsed_construct` | 1 | 1 | `generated:table_caption:5` |
| line block | `unparsed_construct` | 1 | 1 | `generated:line_block:1` |
| raw TeX (`\newcommand`) | `unparsed_construct` | 1 | 1 | `generated:raw_tex:1` |
| нативный HTML-`Div` (`<div>…</div>`) | `unparsed_construct` | 1 | 1 | `generated:native_html_div:1` |
| code/heading/html таблиц и div | `sequence_length` | 1+1+1 | 3 | — |

### Что значит топ-1

`list_without_blank_line`: Pandoc по умолчанию требует **пустую строку перед списком**
(расширение `lists_without_preceding_blankline` выключено), markdown-it — нет. Пример
(`docs/architecture/00-overview.md:409`):

```markdown
**ADR-реестр (детально — `04-reliability-adr.md` §E):**
- 0001–0011 — accepted: порты/адаптеры (hexagonal) vs монолит; …
```

Pandoc видит здесь один абзац с дефисами, `md_ast` — абзац и список. То же в
`docs/architecture/03-data-config-cli.md:680`, `docs/architecture/05-dependency-stack.md:330`
и ещё пяти документах выборки.

`blockquote_without_blank_line` — та же причина для цитаты: Pandoc
(`blank_before_blockquote`) не начинает цитату внутри абзаца, CommonMark — начинает.
Пример (`docs/architecture/09-product-onboarding-incremental-runs.md:4674`):

```markdown
**`resolve_run_id`.** `--run-id` > resume незавершённого прогона того же
`cycle_id`/`cohort_digest`/`seeds_digest` (тот же `run_id`, …)
> `derive_run_id(..., run_scope, refresh_policy)`. `--update` + `--only-failed` —
```

`pandoc_only:paragraph` в `2026-11-03-md-chunk-geometry-tokens.md` — не наш дефект:
многострочная ячейка GFM-таблицы (строки 237–239) у Pandoc обрывает таблицу на строке 237,
и остаток ячейки становится отдельным абзацем. Это ограничение pipe-таблиц Pandoc; класс
назван `sequence_length`, потому что выравнивание видов видит лишний абзац.

## Сгенерированный набор (19 форм из руководства Pandoc)

| форма | результат на блочном уровне |
|---|---|
| grid table | `unparsed_construct:grid_table` |
| multiline table | `unparsed_construct:multiline_table` + 5 лишних блоков |
| simple table | `unparsed_construct:table` + 3 лишних блока |
| pipe table с подписью `: caption` | `unparsed_construct:table_caption` |
| line block | `unparsed_construct:line_block` |
| fenced div `::::: {#…}` | **совпало** (`Div`↔`container`) |
| definition list | **совпало** (`DefinitionList`↔`definition_list`) |
| сноски | **совпало** блочно; 2 `footnote_def` сняты как инлайновые |
| example list `(@)` | `unparsed_construct:example_list` ×2 |
| fancy list `a)` / `iv.` | `unparsed_construct:fancy_list` ×2 |
| citations `[@key]` | блочно **совпало**; инлайн не проверялся |
| bracketed span `[]{.x}` | блочно **совпало**; инлайн не проверялся |
| TOML metadata `+++` | блочно **совпало**: 2.17.1.1 TOML-метаданных не знает и видит `Para`; как метаданные конструкция **не проверена** |
| YAML front matter | **совпало**: `meta` у Pandoc непустой, `front_matter` снят (1 блок) |
| implicit figure `![…](…)` | блочно **совпало**: в JSON 2.17 это `Para[Image]`, отдельного `Figure` нет — как figure **не проверена** |
| subscript/superscript | блочно **совпало**; инлайн не проверялся |
| raw TeX `\newcommand` | `unparsed_construct:raw_tex` |
| display math `$$…$$` | **совпало** после нормализации `Para[DisplayMath]` → `math` |
| native HTML `<div>` | `unparsed_construct:html_div` + 2 лишних блока (`html`, `paragraph`) |

Снято со сравнения с причинами: `footnote_inline` — 2 блока в 1 документе, `metadata` —
1 блок в 1 документе. Это не «совпало»: модель у оракула другая (инлайн/`meta`), и
инлайновая часть сносок и метаданных здесь не проверяется.

## Границы: что именно НЕ проверяется

- **Инлайновая структура**: акценты, ссылки, цитаты `[@key]`, bracketed spans, sub/sup,
  формулы внутри абзаца, сноски, подписи. Блочно такие документы «совпадают» — это не
  доказательство инлайнового паритета.
- **Атрибуты блоков**: `{#id .class key=val}` у заголовков/заборов/div не сравниваются
  (переносятся в `md_ast.info`, но оракул не отдаёт их в сравнимом виде при `types-only`).
- **Точные границы строк и спаны**: недоступны в JSON 2.17.1.1 (см. выше), поэтому
  «блок того же вида, но со сдвинутой границей» этот инструмент не поймает.
- **Вложенность**: у `md_ast` контейнер — один блок с детьми, у Pandoc — дерево; сравнение
  верхнеуровневое, содержимое `Div`/`BlockQuote`/списка не сверяется.
- **Подпись таблицы** у pipe-table распознаётся по источнику (абзац `: …` сразу после
  таблицы), а не по AST: у Pandoc подпись внутри `Table`, у нас — отдельный абзац.
- **TOML-метаданные и implicit figure** на 2.17.1.1 как конструкции не проверяемы:
  оракул их не выделяет (`Para`), хотя `md_ast` тоже не выделяет — совпадение формальное.
  `Figure` в сопоставлении заведён на будущие версии `pandoc-types`, в 2.17 его нет.
- **HTML-фрагменты**: Pandoc режет один `<table>…</table>` на несколько `RawBlock` вокруг
  текста; схлопываются только подряд идущие `RawBlock`, поэтому блочная граница HTML
  остаётся грубой (как и в `scripts/conformance_md.py`).
- **Полный testsuite Pandoc** не подключён: набора примеров в репозитории нет, сравнение
  идёт на выборке дерева и сгенерированных формах.

## Работа следующего прохода (не приговор парсеру)

1. **`lists_without_preceding_blankline`** и **`blank_before_blockquote`** — единственные
   расхождения на реальном дереве (41 блок в 9 документах). Правила семантики профиля
   `pandoc`: решить, требуют ли они пустую строку перед списком/цитатой, и либо
   реализовать, либо явно объявить расхождение допустимым (сейчас оно видимо и посчитано).
2. **Pandoc-таблицы**: grid/multiline/simple + подпись — правила `core/adapters/md_dialect/`
   ещё не написаны (L3, решение «не делаем» основано на отсутствии конструкций в пилоте;
   инструмент показывает цену этого решения числом, а не мнением).
3. **fancy/example lists** — маркеры `a)`/`iv.`/`(@)`; на реальном дереве не встретились,
   только в сгенерированном наборе.
4. **line block, raw TeX, native HTML `Div`** — по одному блоку в сгенерированном наборе;
   на дереве не встретились.
5. **Инлайновая приёмка** (цитаты, спаны, sub/sup, сноски, атрибуты) — отдельный
   инструмент: без `sourcepos` в JSON её нельзя сделать этим; нужен либо HTML-рендер с
   `--sourcepos` в версии, где он есть, либо инлайновое сравнение AST.

## Воспроизведение

```bash
# оракул только на хосте замера/в CI; в рантайме проекта pandoc нет
pandoc --version                      # pandoc 2.17.1.1
.venv/bin/python scripts/conformance_pandoc.py --out-json /tmp/pandoc-baseline.json
# код 1 — расхождения есть; 2 — pandoc недоступен (видимый пропуск, не «0»)
.venv/bin/python -m pytest -q tests/adapters/test_conformance_pandoc.py
```

Инструмент годится как будущий CI-гейт: `0` — чисто, `1` — расхождения, `2` — оракула
нет (пропуск виден и не выдаётся за ноль).