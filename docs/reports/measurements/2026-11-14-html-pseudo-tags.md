# Псевдо-тег `<name:…>` — текст, который HTML-парсер принял за разметку

**Дата:** 2026-11-14
**Срез дерева:** `effe6d4` + незакоммиченная правка `core/adapters/extract_html.py`
(`trafilatura-html-6` → `trafilatura-html-7`, D4)
**Корпус:** `~/.cohound/data/corpus_raw` (44 626 файлов, 12 ГБ), чтение **только на чтение**,
сеть не использовалась
**Команда замера:** обход всех файлов корпуса настоящим `HtmlExtractor.extract` (не копией
правила), плюс `rg` для отбора кандидатов:

```bash
rg -l -a '<[a-zA-Z_][a-zA-Z0-9_-]*:[a-zA-Z0-9_.-]' ~/.cohound/data/corpus_raw | wc -l
# 248 файлов за 5.2 с
```

## Причина

`trafilatura` 2.3.1 (`main_extractor.py:515`, `handle_quotes`) создаёт **новый элемент**
с именем найденного тега. `lxml` отвергает двоеточие в имени:

```text
ValueError: Invalid tag name 'cluster_id:14841639068965178418'
```

Исключение ловится в `core.py:464` как «мусор»:

```python
except (TypeError, ValueError):
    LOGGER.warning("discarding data: %s", options.source)
    return None
```

То есть **весь документ** объявляется мусором, хотя текст в нём есть. Дальше
`HtmlExtractor` возвращает `error="trafilatura: извлечение не дало текста"`, фаза пишет
`parser_failed` — и страница выпадает из корпуса целиком.

Триггер — не разметка, а **проза**: лог etcd в `<pre>` (`header:<cluster_id:1484…>`),
пример SQL с `'<my:a xmlns:my="…">test'`, листинг XSLT (`<xsl:for-each>`). Двоеточие в
имени элемента допустимо только в пространствах имён XML/SVG, а `svg` снимается
пре-клином вместе с телом, — значит, в HTML такой элемент всегда означает «это текст».

## Замер

| | файлов |
|---|---|
| всего в `corpus_raw` | 44 626 |
| содержат конструкцию `<name:…>` | 248 |
| из них HTML (по сигнатуре начала файла) | 10 |

Десять HTML-файлов — это **две** страницы по пять копий (разные `content_hash` одного
документа от разных прогонов). Поведение до и после экранирования `<` перед таким именем:

| страница | до | после |
|---|---|---|
| `database.diasoft.ru/docs/ru/q-database/versiya-180/qdb-etcd` | **0** символов, `parser_failed` | **150 596** символов |
| `postgrespro.ru/docs/…` (примеры `xpath`/`xml_is_well_formed`) | 32 215 символов, примеры разорваны | 32 843 символа, примеры целы |

Что именно рвалось на второй странице — видно в диффе текста:

```diff
-SELECT xpath('/my:a/text()', '
-```
-test ',
+SELECT xpath('/my:a/text()', '<my:a xmlns:my="http://example.com">test',
              ARRAY[ARRAY['my', 'http://example.com']]);
```

`<my:a …>` парсился как элемент, закрывающий fence-блок уезжал в середину строки, и
запрос распадался на два блока с посторонним `test '` между ними. Это не потеря объёма
(он даже вырос), а **порча примера кода** — то есть тихая ошибка в тексте, который потом
попадает в свидетельства.

На четырёх файлах `qdb-etcd` (одна страница, четыре `content_hash`) счёт идёт с нуля:
`extract()` возвращал `None`. Отдельно проверено, что дело не в объёме и не в
`min_output_size` (`1`) и не в `max_tree_size` (не задан) — только в `Invalid tag name`.

## Правило

`_escape_pseudo_tags` (`core/adapters/extract_html.py`) заменяет `<` на `&lt;` **только**
перед именем, содержащим двоеточие:

```python
_PSEUDO_TAG_RE = re.compile(r"<(?=[a-zA-Z_][a-zA-Z0-9_-]*:)")
```

Узость правила проверена на тех же 248 файлах: `ok` — 6 HTML, ошибка — 4 (все четыре —
`qdb-etcd`, до правки), «не HTML» — 238. Документ без такой конструкции возвращается
**байт в байт**, поэтому проза (`a < b`), закрывающие теги, комментарии и обычные элементы
не задеваются.

Порядок в конвейере: D4 стоит **последней** среди правок входа. D2
(`_declare_main_region`) и D3 (`_neutralize_definition_lists`) делают round-trip через
`lxml`, который вернул бы псевдо-тег в строку.

## Цена и класс

- Стоимость: одно регулярное выражение над строкой, разбор не запускается.
- `content_hash` меняется у затронутых страниц ⇒ текст корпуса другой ⇒
  `execution_change`, версия читателя `trafilatura-html-7`. Эффект виден только после
  пере-краула этих URL: `extract_cache` ключуется `(content_hash, extractor_version, level)`,
  а `content_hash` в кэше — от **извлечённого текста**, поэтому старые записи не
  переиспользуются молча.
- Границы: правило не чинит случай, когда псевдо-тег стоит **внутри** `<script>`/`<style>`
  (они снимаются пре-клином раньше — тела нет, чинить нечего) и не пытается угадать, был ли
  `<name:…>` задуман как элемент: в HTML такого элемента не существует.

## Воспроизведение

```bash
.venv/bin/python - <<'PY'
import pathlib
from core.adapters.extract_html import HtmlExtractor
from core.ports.types import RawPage

ex = HtmlExtractor()
for f in pathlib.Path("/tmp/opencode/colon-tag-wide.txt").read_text().splitlines():
    p = pathlib.Path("/home/roger/.cohound/data/corpus_raw") / f
    b = p.read_bytes()
    if not b.lstrip()[:15].lower().startswith((b"<!doctype", b"<html", b"<head", b"<body", b"<?xml")):
        continue
    doc = ex.extract(RawPage(url="https://x/", content=b, fetched_at="2026-11-14T00:00:00",
                             content_type="text/html", status_code=200))
    print(f"{len(doc.text):>7} {doc.error or 'ok':30} {p.name[:16]}")
PY
```

Список кандидатов `/tmp/opencode/colon-tag-wide.txt` получен командой `rg` выше; он
пере-создаётся за 5 секунд и в репозиторий не кладётся (это срез корпуса, а не код).
