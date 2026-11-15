# Модель OCR не входит в колесо `rapidocr` — цена «зелено локально, красно в CI»

**Дата:** 15.11.2026 · **Хост:** WSL2, 7 GB RAM, 12 threads, RTX 3050 4 GB
**Приборы:** `scripts/fetch_ocr_models.py` (новый), `hound doctor` (группа
`deps:extract-pdf`, проверка `ocr:cyrillic`), `rapidocr-3.9.2.dist-info/RECORD`,
`gh run view --log-failed`.

## Что измерялось

`document_level=full` исполняет объявленный алфавит OCR (`cyrillic`,
`DOCLING_OCR_LANG`) **файлом** rec-модели `cyrillic_PP-OCRv5_rec_mobile.onnx`
(`DOCLING_OCR_REC_MODEL`). Вопрос: входит ли этот файл в колесо `rapidocr`, то есть
получает ли его чистая установка `uv sync --frozen --extra extract-pdf`.

## Ответ: не входит

`RECORD` установленного дистрибутива 3.9.2 перечисляет в `rapidocr/models/` ровно четыре
записи:

| файл | размер |
|---|---:|
| `models/.gitkeep` | 0 |
| `PP-OCRv6_det_small.onnx` | 9 929 594 |
| `PP-OCRv6_rec_small.onnx` | 21 234 383 |
| `ch_ppocr_mobile_v2.0_cls_mobile.onnx` | 585 532 |

Кириллической модели среди них нет. Она появляется в каталоге пакета только после
скачивания: `rapidocr/default_models.yaml` (v3.9.2, блок `PP-OCRv5`) объявляет её адрес
на modelscope и SHA-256, а `rapidocr.utils.download_models` качает по нему.

## Цена

Удаление файла из `site-packages` и прогон трёх файлов тестов (15.11.2026):

```
$ mv .venv/…/rapidocr/models/cyrillic_PP-OCRv5_rec_mobile.onnx /tmp/
$ pytest tests/adapters/test_extract_document.py tests/adapters/test_extract_office_real.py \
         tests/pipeline/test_extraction_eval.py
15 failed, 105 passed
```

Раскладка провалов: `test_extract_document` — 6, `test_extract_office_real` — 8,
`test_extraction_eval` — 1. Ровно **15** — то же число, которым был красный CI на
`65fc169` и на всех девяти пушах после него (`gh run view --log-failed`: 84 вхождения
`OcrEngineUnavailableError` в логе, 15 строк `FAILED`).

Локально прогон при этом зелёный: файл лежал в `site-packages` с 25.10, скачанный
вручную при замере 14.11. Отсюда и разрыв: **зелёный локальный прогон доказывал
состояние хоста, а не состояние репозитория.**

## Что сделано

- `scripts/fetch_ocr_models.py` — доставка объявленных моделей: адрес и SHA-256 берутся
  из `default_models.yaml` v3.9.2 (источник, который библиотека объявляет сама), схема
  адреса проверяется **до** открытия (S310: `urlopen` принял бы и `file:`), файл
  скачивается во временный `.part` в том же каталоге и переименовывается только после
  сверки хеша — половина модели на месте читалась бы как целая. Режимы: `--check`
  (только состояние), `--dry-run` (план без сети), `--dest` (другой каталог), `--json`.
- `hound doctor`, группа `deps:extract-pdf`, проверка `ocr:cyrillic`: путь резолвит
  **адаптер** (`_rapidocr_rec_model_path`), а не диагностика — иначе они разошлись бы
  именно там, где расхождение невидимо. Нет файла при установленном `rapidocr` —
  `fail` (не `warn`: установленные модули не делают объявленную ось исполнимой); нет
  `rapidocr` — `warn` (окружение без `extract-pdf` этого и не обещало).
- CI: шаг «Доставить rec-модель OCR» перед гейтами. Недоступность сети — видимый провал
  шага, а не «пропустим и поедем».

## Проверено

```
$ .venv/bin/python scripts/fetch_ocr_models.py --check --dest /tmp/…/ocrmodels
  НЕТ     cyrillic_PP-OCRv5_rec_mobile.onnx (нужен: document_level=full)   # exit 1
$ .venv/bin/python scripts/fetch_ocr_models.py --dest /tmp/…/ocrmodels
  скачано cyrillic_PP-OCRv5_rec_mobile.onnx                                # exit 0
$ sha256sum …/cyrillic_PP-OCRv5_rec_mobile.onnx
90f761b4bfcce0c8c561c0cb5c887b0971d3ec01c32164bdf7374a35b0982711   # = пин
```

`hound doctor --json` при удалённом файле: `ok: false`, `fail: 1` — единственный провал
это `ocr:cyrillic`; после возврата файла `ok: true`, `fail: 0`.

## Что этот замер не говорит

- **Он не про качество OCR.** 719 символов и 558 кириллицы вместо 176 символов
  транслитерации — замер 14.11.2026, он остаётся в силе и здесь только цитируется.
- **Он не делает офлайн-бандл весов.** V4 в `06-implementation-strategy.md` остаётся
  открытым: доставка идёт по сети с modelscope, лицензии весов и `repo_id`/`revision` в
  манифесте не объявлены. Прибор лишь делает доставку **явной и проверяемой** (пин +
  хеш) вместо «у кого-то файл есть».
- **Число 15 — про этот набор extras и этот lock.** При смене `rapidocr` или `docling`
  его нужно перемерить: пины в `MODELS` привязаны к v3.9.2.
