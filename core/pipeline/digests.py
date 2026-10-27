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
обеспечено явно: ``extractor_version`` документа — ``docling-doc-3:<level>/docling-<build>``
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

from core.domain.models import Cohort, SeedsConfig
from core.util.hashing import short_hash

# ВАЖНО: срез только вычисляет дайджест; границы ``hound diff``/``content_change`` не меняются.
__all__ = [
    "EXECUTION_CONTRACT_FIELDS",
    "cohort_digest",
    "execution_contract_digest",
    "manual_artifacts_digest",
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
    "crawl_strategy",
    "crawl_rate_limit",
    "crawl_config_digest",
    # I3/§5: число объявленных раундов SERP-контура. Раунд добавляет документы корпуса,
    # то есть доказательства, поэтому это **условие измерения**, а не провенанс: если
    # спрятать его в ``settings_provenance``, корпус изменится, а ``run_id`` — нет.
    "serp_rounds",
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
