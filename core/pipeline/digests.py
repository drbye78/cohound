"""coHound — канонические дайджесты идентичности прогона (09 §6.2/§12.2/§12.5).

Модуль слоя ``pipeline``: **чистые функции без I/O**. Дайджесты — детерминированный
отпечаток входов измерения, а не случайный идентификатор:

* :func:`cohort_digest` — состав когорты (продукт, отношение к якорю, пересечение ролей),
  ``case_id``, ``frozen_at`` и ``cohort_ref``. Авторский текст (``selection_reason``) и
  отображаемые имена **не** входят: они не меняют измерение (§12.2).
* :func:`seeds_digest` — эффективный seed-набор: ``(product_id, order, normalized_url,
  role, profile_key, enabled)``, где ``order`` — авторская позиция корня **внутри
  продукта**. Порядок корней — часть идентичности (краул порядко-чувствителен: корень,
  идущий последним, может не выполниться вовсе, см. ``_ordered_unique``), а порядок
  **продуктов** в файле канонизируется сортировкой. Пустой набор — стабильный дайджест
  пустого множества, а не пустая строка.
* :func:`execution_contract_digest` — отпечаток **условий измерения** по списку §12.5.
  Значения приходят аргументом (mapping): функция не читает ни БД, ни файловую систему.
  ``None`` — часть контракта (отсутствие значения фиксируется как ``null``), поэтому
  пропущенный ключ — ошибка, а не «то же самое, что ``None``».

Граница этого среза: дайджесты только **вычисляются**. Семантика ``hound diff``
(``EXECUTION_FIELDS``/``index_build_id``) живёт в :mod:`core.pipeline.report`.

**Доктрина ``None`` (методология 2.4.0, §13).** В контракт входят только
**объявленные** оси — «функция входа» (``args``/``run.yaml``/файлы пакета/``deploy.yaml``/
константы кода), вычислимая до сборки портов. Наблюдаемые величины портов/диска
(``extractor_version``, ``embed_model_id``, ``lexical_engine_version``,
``vector_store``) остаются провенансом манифеста и ``pb``, но **не** входят в контракт:
их хеширование сделало бы ``run_id`` зависимым от установленных wheel'ов и диска
(два хоста с одним ``run.yaml`` — разные прогоны, R7 §11.7). Следствие названо прямо:
их смена **не блокирует** межцикловый diff, а видна через ``pb``/``index_build_id`` и
попадает в разрешённый класс ``content_change``.

**Граница этого утверждения (Д-8).** «Видна через ``pb``» верно только для того, что
действительно входит в наблюдаемые величины. Для сборки движка извлечения это
обеспечено явно: ``extractor_version`` документа — ``docling-doc-3:<level>/<device>/docling-<build>/<policy>``
(:func:`core.adapters.extract_document.docling_extractor_version`), то есть та же сборка,
что объявлена в контракте полем ``extractor_stack``. Отсюда два следствия, и оба
обязательны: апгрейд библиотеки меняет ``run_id`` (объявленная ось изменилась) **и**
не может переиспользовать текст старого движка — сборка входит в ключ ``extract_cache``
(``(content_hash, extractor_version, level)``), поэтому старая запись не находится, а
``pb``/``corpus_docs.extractor_version`` описывают **фактически прочитанный** текст.
Без этого сборка не была бы видна ни в ``pb``, ни в ключе кэша, и ``run_id`` называл бы
движок, которого не было.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from core.domain.models import CatalogRow, Cohort, SeedsConfig
from core.util.hashing import short_hash

# ВАЖНО: срез только вычисляет дайджест; границы ``hound diff``/``content_change`` не меняются.
__all__ = [
    "CELL_REUSE_SCHEMA",
    "EXECUTION_CONTRACT_FIELDS",
    "catalog_digest",
    "cell_reuse_key",
    "cohort_digest",
    "coverage_digest",
    "execution_contract_digest",
    "manual_artifacts_digest",
    "passports_digest",
    "render_modes_digest",
    "retrieved_doc_set_digest",
    "scoring_policy_version",
    "seeds_digest",
]

#: Поля execution contract (§12.5, методология 2.4.0). Порядок — документационный; хеш
#: канонический (``sort_keys=True``), поэтому порядок входа на результат не влияет.
#:
#: Наблюдаемые величины портов/диска (``extractor_version``, ``embed_model_id``,
#: ``lexical_engine_version``, ``vector_store``) здесь **отсутствуют** осознанно: они не
#: выводятся из входа (per-product композит корпуса/диск/wheel'ы), остаются провенансом
#: манифеста и ``partition_build_id``, но не блокируют межцикловый diff. Их смена видна
#: через ``pb``/``index_build_id`` и попадает в разрешённый класс ``content_change``.
#: Вместо ``extractor_version`` объявлен ``extractor_stack`` (константы стека извлечения
#: + сборка движка ``docling-<version>``). Это **одна и та же** сборка, что входит в
#: ``extractor_version`` документа и в ключ ``extract_cache``: ``run_id`` не может
#: объявить движок, текст которого не был прочитан (Д-8).
EXECUTION_CONTRACT_FIELDS: tuple[str, ...] = (
    "methodology_version",
    "scoring_policy_version",
    "status_dict_version",
    "catalog_version",
    "package_version",
    "cohort_digest",
    "seeds_digest",
    "llm_scenarios",
    "retrieval_profile",
    "cutoff_K",
    "retrieval_params_json",
    "chunker_version",
    "extractor_stack",
    # 16-runs-schema-boundary §4.2: **устройство эмбеддера** (``cpu`` | ``cuda``). Та же
    # природа, что у ``extractor_stack``: место счёта — условие измерения, а не провенанс
    # хоста. До этого поля устройство выбирал torch (``SentenceTransformer`` без
    # ``device=``), то есть два хоста с одним ``run.yaml`` могли посчитать векторы на
    # разных устройствах под одним ``run_id``, а ``pb``/``embed_cache`` этого не
    # различали. ``auto`` нет намеренно: объявленная ось обязана действовать (R5).
    # Смена — ``execution_change``; в ``embed_model_id`` устройство **не** входит: там
    # веса, а не место счёта.
    "embed_device",
    "crawl_strategy",
    "crawl_rate_limit",
    "crawl_config_digest",
    # I3/§5: число объявленных раундов SERP-контура. Раунд добавляет документы корпуса,
    # то есть доказательства, поэтому это **условие измерения**, а не провенанс: если
    # спрятать его в ``settings_provenance``, корпус изменится, а ``run_id`` — нет.
    "serp_rounds",
    # I3/§5: режим запроса внешнего поиска (``run.yaml: serp.mode``). Отдельная ось от
    # ``serp_rounds``, потому что это разные условия: раунды говорят «сколько раз
    # спрашивать», режим — «какой бэкенд отвечает». Замер 28.10: ``sync`` отвечает за
    # 6.9 с медиана, ``async`` — за 180 с при опросе 30 с и отказывает
    # ``search API backends are overloaded`` там, где sync работает; разные бэкенды
    # дают разный состав находок, то есть разный корпус.
    "serp_mode",
    "registry_spec_digest",
    # 07 §7/R7: профиль доверенных зон (``local`` | ``split``) — **условие измерения**, а
    # не провенанс хоста. Это не «где запущен процесс», а **какими механизмами получен
    # корпус**, и механизмы у профилей разные: ``split`` исполняет веб-работу в DMZ и
    # сегодня (а) игнорирует ``document_level``/``pdf_pipeline`` — уровень извлечения
    # объявлен, но применён не будет; (б) делает двойной фетч (шов + локальная сборка),
    # то есть другой набор наблюдённых URL и другой ``complete``; (в) не извлекает члены
    # контейнеров — архивный член не становится документом корпуса. Ни одно из трёх не
    # меняет ``run_id`` само по себе, поэтому без этого поля один и тот же ``run_id``
    # описывал бы два разных исполнения — ложь об условиях измерения, тот же класс, что
    # у шаблона промпта (``prompt_template``) и у ``--max-depth`` до своей правки.
    # Значение — объявленное (``deploy.yaml: profile`` через
    # ``DeclaredExecutionAxes.deploy_profile``), вычислимо до сборки портов, поэтому
    # остаётся «функцией входа», а не наблюдением; ``edge_capabilities`` (наблюдаемые
    # версии edge-компонентов) сюда не входят.
    "deploy_profile",
    "edge_schema_version",
    # 13-manual-artifacts §3.2: отпечаток объявленных ручных артефактов
    # (``sources/manual_artifacts.yaml``). Меняет корпус, значит и условия измерения —
    # но в материал входят только ``(product_id, url, sha256)``: ``path``/``provided_by``/
    # ``provided_at``/``reason`` это провенанс записи, а не измерение (как ``source``/
    # ``added_*`` у ``seeds_digest``). Пустой набор даёт стабильный дайджест пустого
    # множества, поэтому «артефактов нет» и «поле отсутствует» различимы.
    "manual_artifacts_digest",
    # §7.1/§7.4: версия шаблона промпта оценщика. Шаблон — не «настройка вида»: он
    # задаёт, какой ответ считается заявленным, а какой — отсутствием, то есть прямо
    # меняет вердикты. При этом он был единственным условием измерения, не входившим в
    # контракт: смена `evaluator_v3` → `evaluator_v4` меняла вердикты, не меняя `run_id`,
    # — тот же класс, что `--max-depth`, применявшийся при сборке фаз и не входивший в
    # идентичность. Значение — имя шаблона (`TEMPLATE_VERSION`) и его хеш: имя без
    # содержимого позволило бы переписать текст, не сменив идентичность.
    "prompt_template",
    # C3: геометрия нарезки чанков и состав входа эмбеддера. ``chunker_version`` — метка,
    # а не вывод из геометрии: правка ``chunk_size`` на другое число давала другой корпус
    # чанков при том же ``run_id`` (тот же класс, что ``embed_model_id`` до фикса этой
    # дыры). Значения читаются из самих чанкеров (``composite_chunking_spec``), поэтому
    # разъехаться с фактической нарезкой не могут.
    "chunking_spec",
    # V3: политика повтора при невалидном ответе оценщика. Повтор **переиздаёт запрос**,
    # поэтому вердикт зависит от истории сэмплов: при недетерминированном провайдере
    # (замер: 4.2 % ячеек меняют статус) «первый валидный» — это лотерея, и её размер
    # обязан быть объявлен. Поле меняет вердикты, значит это условие измерения, а не
    # провенанс (тот же довод, которым вошёл ``prompt_template``).
    "evaluation_policy",
    # C8: схема **лексического индекса** — состав полей и цепочка анализатора, то есть
    # то, из чего получается лексическая выдача. Версия движка (``tantivy-<ver>``) сюда
    # не входит: она наблюдаемая и хост-зависимая (правило §13), поэтому остаётся
    # провенансом ``pb``/манифеста. Схема же объявлена кодом — как ``chunking_spec``.
    # Замер, из-за которого это условие, а не оптимизация: переход 4 поля → 1 при
    # ``lang=None`` (единственный продовый путь) изменил ранжирование — прежняя схема
    # складывала баллы двух полей с одним текстом и держала русские стоп-слова термами
    # (Jaccard top-10 0.56 → 0.94).
    "lexical_spec",
    # §11.1: **объявленные входы корпуса**, менявшие корпус, но не ``run_id``. Тот же
    # класс, что ``--max-depth`` до своей правки и ``chunk_size`` (C3): значение
    # объявлено на входе (файл/флаг), вычислимо до сборки портов, и при этом определяет,
    # какие байты станут доказательствами.
    #
    # ``--urls`` отдельного поля **не** получает: это полная замена пакетного
    # seed-набора, и она уже входит в ``seeds_digest`` (composition root грузит
    # ``load_effective_seeds(urls_override=...)`` и хеширует результат — см.
    # ``CONTRACT_FLAGS`` запись ``urls`` с материалом ``seeds``). Второе поле на тот же
    # материал нарушило бы правило «одно поле — один материал»: два поля менялись бы
    # вместе, и разбор расхождения прогонов не смог бы их различить.
    # ``render_modes_digest`` — per-URL режим рендера (``--render-modes``): честный HTTP и
    # браузер отдают **разный** текст со страницы, поэтому «каким механизмом прочитан
    # документ» — условие измерения, а не деталь транспорта.
    "render_modes_digest",
    # ``passports_digest`` — содержимое авторских паспортов (``--passports-dir``).
    # Паспорт даёт документы корпуса и сателлиты, то есть доказательства; правка байтов
    # паспорта меняла корпус, не меняя идентичность. Материал — ``(product_id, sha256
    # набора файлов)``; путь и mtime не входят (переезд каталога не меняет измерение).
    "passports_digest",
)

#: Длина короткого дайджеста идентичности (§12.2/§6.2).
_DIGEST_LEN = 24


def _canonical(payload: Any) -> str:
    """Канонический JSON: стабильные ключи/разделители, UTF-8 без escape."""
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def cohort_digest(
    cohort: Cohort, *, case_id: str, cohort_ref: str | None = None
) -> str:
    """Дайджест состава когорты (09 §12.2), 24 hex-символа.

    Материал: ``case_id`` (аргумент), ``cohort_ref``, ``frozen_at`` (ISO-дата) и
    отсортированный по ``product_id`` список ``(product_id, anchor_relation,
    role_overlap)``.

    **Не** входят: ``selection_reason`` (авторский текст), ``canonical_name``/имена
    продуктов, порядок записей в файле. Поэтому правка обоснования выбора не создаёт
    новый прогон, а добавление/удаление продукта — создаёт.
    """
    payload = {
        "case_id": case_id,
        "cohort_ref": cohort_ref,
        "frozen_at": cohort.frozen_at.isoformat(),
        "products": [
            {
                "product_id": entry.product_id,
                "anchor_relation": entry.anchor_relation.value,
                "role_overlap": entry.role_overlap,
            }
            for entry in sorted(cohort.entries, key=lambda item: item.product_id)
        ],
    }
    return short_hash(_canonical(payload), _DIGEST_LEN)


def seeds_digest(seeds: SeedsConfig) -> str:
    """Дайджест эффективного seed-набора (09 §12.2), 24 hex-символа.

    Материал: список ``(product_id, order, normalized_url, role, profile_key, enabled)``
    по всем продуктам и seed'ам (включая ``enabled=false`` — отключение seed'а меняет
    корпус и coverage), отсортированный по ``(product_id, order)``. ``source``/
    ``source_ref``/``added_*`` **не** входят: это провенанс авторской записи, а не
    измерение.

    **Порядок корней внутри продукта входит в идентичность** (``order`` — авторская
    позиция в ``seeds.yaml``). Краул порядко-чувствителен: бюджет ``max_documents``
    расходуется в порядке обхода, поэтому корень, идущий последним, может не выполниться
    вовсе (``_ordered_unique``). Если бы дайджест сортировал только по URL, перестановка
    корней давала бы тот же ``run_id`` при другом корпусе — дыра в воспроизводимости
    (§15, I19). Порядок **продуктов** в файле, наоборот, канонизируется сортировкой по
    ``product_id``: продукт-цикл идёт в отсортированном порядке и от записи не зависит.

    Пустой набор даёт стабильный дайджест пустого множества (``{"products": []}``),
    а не пустую строку.
    """
    rows = [
        {
            "product_id": product_id,
            # Позиция корня **внутри продукта** (авторский порядок ``seeds.yaml``).
            "order": order,
            "normalized_url": seed.normalized_url,
            "role": seed.role.value,
            "profile_key": seed.profile_key,
            "enabled": seed.enabled,
        }
        for product_id, product in seeds.products.items()
        for order, seed in enumerate(product.seeds)
    ]
    rows.sort(key=lambda row: (row["product_id"], row["order"]))
    return short_hash(_canonical({"products": rows}), _DIGEST_LEN)

def catalog_digest(catalog: Iterable[CatalogRow]) -> str:
    """Дайджест **содержимого каталога** — честный источник ``catalog_version``.

    Проблема, которую это закрывает (`09` §12.5): поле ``catalog_version`` объявлено
    полем контракта и границей ``EXECUTION_FIELDS``, но источника у него не было — в
    контракт уходило ``None``, в манифест ``"0.0.0"``. То есть смена каталога (новый
    параметр, изменённое правило, другой ``absence_rule``) меняла предмет измерения,
    а ``run_id`` и ``hound diff`` этого не видели: сравнивались два одинаковых
    ``"0.0.0"``. Авторской версии каталога в пакете нет (``catalog.csv`` — просто
    таблица, ``catalog.lock.csv`` несёт только происхождение строк), поэтому
    единственный **доказуемый** ответ на вопрос «какой каталог» — отпечаток содержимого.

    Материал — все поля строки, кроме ``deprecated``/``replaced_by``: они помечают
    отмену параметра, а не его правило, и меняются отдельной процедурой (§13). Порядок
    канонизируется сортировкой по ``param_id``: перестановка строк в файле измерение не
    меняет.

    Почему это ``*_version``, а не отдельное поле: имя колонки — persisted data
    (``runs.catalog_version``) и граница diff; переименование сломало бы чтение старых
    прогонов ради косметики. Что значение **содержимое-адресное**, сказано здесь и в
    ``09`` §12.5, а не угадывается по формату.
    """
    rows = [
        {
            "param_id": row.param_id,
            "title": row.title,
            "module": row.module,
            "group": row.group,
            "importance": row.importance.value,
            "param_type": row.param_type.value,
            "rule": row.rule,
            "absence_rule": row.absence_rule.value,
            "role_binding": row.role_binding,
            "concept_terms": {
                lang: list(terms)
                for lang, terms in sorted(row.concept_terms.items())
            },
            "search_langs": list(row.search_langs),
        }
        for row in sorted(catalog, key=lambda item: item.param_id)
    ]
    return short_hash(_canonical({"catalog": rows}), _DIGEST_LEN)

def scoring_policy_version() -> str:
    """Версия политики расчёта §6 — честный источник ``scoring_policy_version``.

    Проблема та же, что у :func:`catalog_digest` (`09` §12.5): поле контракта без
    источника. Политика при этом **объявлена** — :func:`core.domain.scoring.scoring_policy_spec`
    возвращает ровно те значения, с которыми исполняется формула (``capability_policy``,
    ``unknown_policy``, ``partial_score``), и они же входят в ключ межпрогонного кэша
    вердиктов (`09` §10.5).

    Формат — ``<capability_policy>+<unknown_policy>@<hash>``: имя политики читаемо
    человеком (``resolved-only-v1+excluded``), хеш ловит смену ``partial_score`` и любых
    будущих слагаемых, которых в имени нет. Только имя было бы ложью о содержимом — тот
    же довод, по которому ``prompt_template`` несёт ``@hash``; только хеш не сказал бы,
    какая политика работала.

    Значения берутся **из домена**, а не переписываются здесь: вторая копия разошлась бы
    с первой (и ``test_manifest_scoring_defaults_match_declared_policy`` уже сверяет
    дефолты манифеста с этой же функцией).
    """
    from core.domain.scoring import scoring_policy_spec

    spec = scoring_policy_spec()
    name = f"{spec['capability_policy']}+{spec['unknown_policy']}"
    return f"{name}@{short_hash(_canonical(spec), 12)}"


def manual_artifacts_digest(rows: Iterable[tuple[str, str, str]]) -> str:
    """Дайджест объявленных ручных артефактов (13-manual-artifacts §3.2), 24 hex.

    Материал: отсортированные ``(product_id, url, sha256)``. ``path``/``provided_by``/
    ``provided_at``/``reason`` **не** входят — это провенанс записи, а не измерение
    (зеркало ``seeds_digest``, где исключены ``source``/``added_*``). ``sha256`` —
    **вычисленный** из байтов хеш (сверка объявленного — обязанность загрузчика, не
    этой функции): замена файла без правки декларации обязана менять ``run_id``.

    Пустой набор даёт стабильный дайджест пустого множества (``{"artifacts": []}``),
    а не пустую строку: «артефактов нет» — это объявленное условие измерения.

    Порядок аргумента не важен (сортировка внутри); дубликаты строк не схлопываются
    тихо — ``(product_id, url)`` уникален по контракту загрузчика, поэтому дубль здесь
    был бы дефектом декларации, и он виден в материале.
    """
    material = sorted((str(pid), str(url), str(sha)) for pid, url, sha in rows)
    return short_hash(
        _canonical({"artifacts": [list(row) for row in material]}), _DIGEST_LEN
    )


def render_modes_digest(modes: Mapping[str, str]) -> str:
    """Дайджест per-URL режимов рендера (``--render-modes``), 24 hex-символа.

    Материал: отсортированные ``(url, mode)``. Режим определяет, **чем** прочитан
    документ: HTTP отдаёт серверный HTML, браузер — отрендеренный DOM. Это разный текст,
    значит разные доказательства и разные вердикты; поэтому режим — условие измерения, а
    не настройка транспорта.

    Порядок не важен (URL уникален по контракту загрузчика), поэтому сортировка внутри.
    """
    material = sorted((str(url), str(mode)) for url, mode in modes.items())
    return short_hash(
        _canonical({"render_modes": [list(row) for row in material]}), _DIGEST_LEN
    )

def passports_digest(digests: Mapping[str, str | None]) -> str:
    """Дайджест содержимого паспортов продукта, 24 hex-символа.

    Принимает **уже вычисленные** дайджесты байтов
    (:func:`core.config.passports.passport_content_digests`): формула байтов живёт там,
    где живёт загрузчик паспортов, и вторая копия здесь разошлась бы с ней.

    ``None`` у продукта — «паспорта нет», и это наблюдаемое состояние, отличимое от
    «паспорт есть, байты такие»: появление паспорта меняет корпус, значит и контракт.
    Путь каталога и mtime не входят: переезд каталога измерение не меняет.
    """
    material = sorted(
        (str(product_id), digest) for product_id, digest in digests.items()
    )
    return short_hash(
        _canonical({"passports": [list(row) for row in material]}), _DIGEST_LEN
    )

def execution_contract_digest(values: Mapping[str, Any]) -> str:
    """Дайджест условий измерения (09 §12.5), 24 hex-символа.

    ``values`` обязан содержать **все** поля :data:`EXECUTION_CONTRACT_FIELDS`.
    Пропущенный ключ — :class:`ValueError` (иначе «нет поля» и ``None`` дали бы один
    и тот же хеш); лишний ключ — тоже ошибка (контракт не должен молча расширяться).
    ``None`` включается в материал как JSON ``null``: отсутствие значения — часть
    контракта.

    Функция не читает БД/файлы: значения передаёт composition root (или тест).
    """
    keys = set(values)
    missing = [field for field in EXECUTION_CONTRACT_FIELDS if field not in keys]
    if missing:
        raise ValueError(
            f"execution_contract: отсутствуют поля {missing} "
            "(None нужно передать явно)"
        )
    extra = sorted(keys - set(EXECUTION_CONTRACT_FIELDS))
    if extra:
        raise ValueError(f"execution_contract: неизвестные поля {extra}")
    payload = {field: values[field] for field in EXECUTION_CONTRACT_FIELDS}
    return short_hash(_canonical(payload), _DIGEST_LEN)


# ---------------------------------------------------------------------------
# Перенос ячеек (09 §10.1, схема ``cell-reuse-2``): ключ и его слагаемые
# ---------------------------------------------------------------------------

#: Версия пространства ключей переноса ячейки (09 §10.1).
#:
#: ``cell-reuse-2`` — ключ **per-cell**: он несёт отпечаток retrieved-набора **этой**
#: ячейки (``retrieved_doc_set_digest``), а не doc-set продукта (решение владельца
#: 29.10.2026). Прежняя схема ``cell-reuse-1`` была per-product: изменение любого
#: документа продукта инвалидировало все его ячейки, и цена этого консерватизма не
#: измерялась. Значение входит в материал ключа, поэтому смена схемы — несовместимость
#: по построению: старые ключи не совпадут, и ячейка переоценится (безопасное
#: направление — лишняя переоценка, а не чужой вердикт).
CELL_REUSE_SCHEMA = "cell-reuse-2"

#: Длина ключа переноса ячейки (24 hex-символа — как у прочих дайджестов §12.2).
_CELL_REUSE_LEN = 24


def retrieved_doc_set_digest(retrieved: Iterable[Any]) -> str:
    """Отпечаток retrieved-набора **одной ячейки** (09 §10.1), 24 hex-символа.

    Материал — множество пар ``(product_id, content_hash)`` переданных чанков, без
    ``score``/``rank``/``chunk_id``:

    * **порядок не входит** (``sorted`` на множестве): порядок выдачи может измениться от
      пересборки партиции при неизменном наборе документов, а текст-то тот же — перенос в
      этом случае допустим (09 §10.1). Если окажется, что порядок влияет на вердикт, в
      ключ войдёт ``prompt_hash``, а не ранг;
    * **``chunk_id`` не входит**: он несёт ``partition_build_id`` (16 §6), и пересборка
      партиции с тем же содержимым инвалидировала бы ячейку без изменения доказательств;
    * **``content_hash`` — это и есть версия текста документа** (I19): чанк адресуется
      хешем собственного текста, поэтому правка документа меняет отпечаток даже тогда,
      когда ``doc_version_id`` формально тот же (пере-экстракция того же URL).

    ``doc_version_id`` в материал **не входит** осознанно: ``RetrievedChunk`` его не несёт
    (версия документа есть у чанка-субстрата, а не у выдачи), и выводить её из
    ``chunk_id`` значило бы привязать ключ к форме идентификатора сборки. Требование §10.1
    («``sorted(doc_version_id)`` + их ``content_hash``») исполняется по существу:
    отпечаток — функция **текстов**, из которых извлечено доказательство.

    Пустой срез даёт стабильный дайджест пустого множества, а не пустую строку: «ничего
    не нашлось» — наблюдённый факт, отличимый от «набор не считали».
    """
    material = sorted(
        {
            (str(getattr(chunk, "product_id", "") or ""), str(getattr(chunk, "content_hash", "") or ""))
            for chunk in retrieved
        }
    )
    return short_hash(
        _canonical({"retrieved": [list(row) for row in material]}), _CELL_REUSE_LEN
    )


def coverage_digest(coverage: Any | None) -> str | None:
    """Стабильный дайджест семантики покрытия корпуса (09 §10.1), 24 hex-символа.

    Входят только семантические поля — ``complete``, ``expected_sources``,
    ``observed_sources``, ``blocked``, ``unreadable_origins``, ``partial_blocks``,
    ``truncated``, ``reason``. **Не входят** ``computed_at`` (момент вычисления),
    ``run_id``/``product_id`` (адрес записи) и счётчики ``reused/revalidated/new``: они
    меняются от того, ответил ли сервер 304 или 200 с тем же телом, — это не семантическое
    изменение, а разные способы наблюдения одного и того же корпуса. Иначе ключ переноса
    ломался бы на каждом прогоне, где краул пошёл другим путём при том же результате.

    ``None`` — покрытия нет вовсе (прогон не дошёл до корпуса): это **не** «пустое
    покрытие», и перенос в таком прогоне запрещён отдельно (§10.2).
    """
    if coverage is None:
        return None
    payload = {
        "complete": bool(getattr(coverage, "complete", False)),
        "expected_sources": int(getattr(coverage, "expected_sources", 0) or 0),
        "observed_sources": int(getattr(coverage, "observed_sources", 0) or 0),
        "blocked": {
            str(kind): int(count)
            for kind, count in sorted((getattr(coverage, "blocked", None) or {}).items())
        },
        "unreadable_origins": sorted(
            str(origin) for origin in (getattr(coverage, "unreadable_origins", None) or ())
        ),
        "partial_blocks": {
            str(origin): {
                str(kind): int(count)
                for kind, count in sorted((counts or {}).items())
            }
            for origin, counts in sorted(
                (getattr(coverage, "partial_blocks", None) or {}).items()
            )
        },
        "truncated": bool(getattr(coverage, "truncated", False)),
        "reason": getattr(coverage, "reason", None),
    }
    return short_hash(_canonical(payload), _CELL_REUSE_LEN)


def cell_reuse_key(
    *,
    product_id: str,
    param_id: str,
    catalog_row_digest: str,
    prompt_template_digest: str,
    model_id: str,
    llm_scenario: str,
    model_config_digest: str,
    retrieval_profile: str | None,
    cutoff_k: int | None,
    retrieval_params_json: str | None,
    retrieved_doc_set_digest: str,
    coverage_digest: str | None,
    methodology_version: str,
    status_dict_version: str | None,
    scoring_policy_version: str,
    capability_policy: str,
    unknown_policy: str,
    partial_score: float,
    attestation_digest: str,
    partition_build_id: str | None,
) -> str:
    """Ключ переноса ячейки (09 §10.1, схема :data:`CELL_REUSE_SCHEMA`), 24 hex-символа.

    Совпал ключ — вердикт прошлого прогона переносим; не совпал — ячейка измеряется
    заново. Поэтому в материал входит **всё**, от чего вердикт зависит, и все значения —
    **текущие**, а не взятые из строки ``prev``: иначе смена шаблона/модели/правил не
    инвалидировала бы перенос (``prompt_template_digest`` считается от текущего шаблона и
    правила параметра, а не от ``prev.prompt_hash``; ``model_config_digest`` — от текущей
    конфигурации оценщика, а не от ``prev.model_id``).

    Состав (порядок фиксирован — это материал ключа, а не документация):

    * схема ключа (``cell-reuse-2``);
    * адрес ячейки: ``product_id`` + ``param_id``;
    * строка каталога (:func:`core.pipeline.eval_cache.catalog_row_digest`) — ``rule``/
      ``absence_rule``/``param_type``/``role_binding``/… задают, что считается заявленным;
    * ``prompt_template_digest`` — версия шаблона + текст правила (текущие);
    * ``model_id``/``llm_scenario``/``model_config_digest`` — маршрутизация и конфигурация
      оценщика (I21): смена слота или декодера — переоценка;
    * ``retrieval_profile``/``cutoff_k``/``retrieval_params_json`` — условия ретривала
      (I16/I20): смена K меняет набор доказательств, значит и вердикт;
    * ``retrieved_doc_set_digest`` — **набор документов этой ячейки** (per-cell);
    * ``coverage_digest`` — против какого наблюдения корпуса измерено;
    * ``methodology_version``/``status_dict_version``/``scoring_policy_version``/
      ``capability_policy``/``unknown_policy``/``partial_score`` — правила ответа (§10.5,
      §13): смена семантики — переоценка **всех** ячеек;
    * ``attestation_digest`` — действующие человеческие подписи (O13): решение человека
      не переносится неявно;
    * ``partition_build_id`` — продукт-партиция retrieved-набора. Именно **партиция**, а не
      ``composition_id``: пересборка чужой партиции не должна инвалидировать ячейку.

    ``None`` в значениях сериализуется как ``null`` и отличим от пустой строки: «условие не
    объявлено» — тоже условие измерения, и выдавать его за объявленное нельзя.
    """
    material = "\x1f".join(
        (
            CELL_REUSE_SCHEMA,
            str(product_id),
            str(param_id),
            str(catalog_row_digest),
            str(prompt_template_digest),
            str(model_id),
            str(llm_scenario),
            str(model_config_digest),
            _key_part(retrieval_profile),
            _key_part(cutoff_k),
            _key_part(retrieval_params_json),
            str(retrieved_doc_set_digest),
            _key_part(coverage_digest),
            str(methodology_version),
            _key_part(status_dict_version),
            str(scoring_policy_version),
            str(capability_policy),
            str(unknown_policy),
            repr(float(partial_score)),
            str(attestation_digest),
            _key_part(partition_build_id),
        )
    )
    return short_hash(material, _CELL_REUSE_LEN)


def _key_part(value: Any) -> str:
    """Слагаемое ключа: ``None`` отличим от пустой строки (маркер ``∅``).

    Без маркера «не объявлено» и «объявлено пустым» давали бы один материал, и ключ
    переноса молча уравнял бы два разных условия измерения.
    """
    return "∅" if value is None else str(value)
