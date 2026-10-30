"""Перенос ячеек между прогонами (carry-forward), 09 §10 — схема ``cell-reuse-2``.

Модуль отвечает на **один** вопрос: можно ли вердикт ячейки прошлого прогона перенести в
текущий, не спрашивая оценщика. Он **не** пишет в хранилища и не решает за вызывающего:
возвращает готовую к записи ячейку и копии её evidence (или ``None`` — «надо мерить»), а
персистенцию и правила допуска ``❌`` исполняет Ш6 (:mod:`core.pipeline.evaluate`). Разделение
не косметическое: правило «``❌`` требует ``live|revalidated``» живёт в Ш6, и перенос обязан
проходить **тот же** эшелон, а не свою копию.

**Ключ считается после ретривала (per-cell, решение владельца 29.10.2026).** До ретривала
неизвестно, какие документы вошли в ячейку, — значит, ключ невычислим. Поэтому перенос не
батч-проход по продукту, а решение по одной ячейке между ``retrieve_for_cell`` и
``evaluate_cell``. Ретривал при этом выполняется всегда (он и так выполнялся до вызова
оценщика); экономия — только пропуск вызова модели. Порога «сколько документов изменилось»
нет: порог был бы ещё одной осью для калибровки (A3 — мерить, а не предугадывать).

**Что запрещает перенос** (§10.3) — каждое условие названо, потому что «перенос не
случился» обязано быть объяснимым:

* нет прогона-предшественника (первый прогон, resume **текущего** прогона) — переносить
  не из чего;
* нет покрытия либо ``coverage.complete is False``: неполный корпус не доказывает
  отсутствия (I2c), и перенесённый вердикт выглядел бы подтверждённым тем, что не наблюдалось;
* в строке измерения нет ``cell_reuse_key`` (ячейка записана до появления переноса, либо
  adoption — он ключи не восстанавливает) — сравнивать не с чем;
* ключ не совпал (каталог/шаблон/модель/правила/ретривал/набор документов/покрытие/
  подписи) — §10.3;
* предыдущая ячейка ``stale`` — её вердикт описывает прошлую сборку, и снятие пометки
  переоценкой, а не переносом, единственный честный путь;
* состояние не переносимо (``failed``/``preliminary``): это не вердикт (I4/I13);
* цепочка evidence не проходит проверку (см. :func:`_evidence_chain_valid`);
* в текущем прогоне у ячейки есть высокий источник (реестр/наследование) — прецедент
  источника (§2.2 п.7/п.8) сильнее переноса, и затирать его корпусной копией нельзя;
* вердикт — ``❌``, а правило допуска ``❌`` его не подтверждает: решение принимает
  вызывающий (Ш6), потому что правило живёт там.

Нейтрален к предметной категории (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from core.domain.enums import CellState, MeasurementOrigin
from core.domain.models import (
    CatalogRow,
    Cell,
    CellEvidence,
    absence_evidence_id,
)
from core.pipeline.digests import (
    cell_reuse_key,
    coverage_digest,
    retrieved_doc_set_digest,
)
from core.pipeline.eval_cache import EvalConditions, catalog_row_digest
from core.pipeline.idempotency import evidence_id_for
from core.pipeline.prompt import TEMPLATE_VERSION
from core.ports.state import CellMeasurementRow, ProductStatePort
from core.ports.store import StorePort
from core.ports.vector_index import RetrievalResult
from core.util.hashing import short_hash
from core.util.ids import doc_version_of

__all__ = [
    "CARRYABLE_STATES",
    "CarriedCell",
    "CellReuseConditions",
    "attestation_digest",
    "carry_forward_cell",
    "cell_reuse_conditions",
    "evidence_digest",
    "measurement_row",
    "model_config_digest",
    "prompt_template_digest",
]

#: Состояния, вердикт которых переносим (09 §10.2). ``evaluated``/``reviewed`` — вердикт
#: получен (второе — с человеческим решением, I6/I7); ``pending`` — кандидат очереди ревью:
#: переносится **как** ``pending``, потому что это не вердикт, а место в очереди, и
#: пере-спрашивать оценщика значило бы заново платить за тот же вопрос. ``failed`` (I4) и
#: ``preliminary`` (I13) не переносятся: это не вердикты, и заморозка сбоя прибора
#: навсегда зафиксировала бы транзиентный отказ адаптера.
CARRYABLE_STATES: frozenset[CellState] = frozenset(
    {CellState.EVALUATED, CellState.REVIEWED, CellState.PENDING}
)

#: Роль модели в контракте §7.4: маршрутизация детерминирована фазой (I21), поэтому
#: сценарий ячейки — всегда оценщик. Значение входит в ключ: если маршрутизация когда-нибудь
#: станет другой, ключ разойдётся, а не совпадёт молча.
_EVALUATOR_SCENARIO = "evaluator"


@dataclass(frozen=True)
class CellReuseConditions:
    """Объявленные условия ячейки, не зависящие от ретривала (09 §10.1).

    Строится **один раз на прогон** (:func:`cell_reuse_conditions`): половина слагаемых
    ключа — свойства прогона (правила, модель, подписи), а не ячейки. Пересчитывать их на
    каждую пару «продукт × параметр» значило бы читать политику расчёта сотни раз.

    :param product_id: продукт-партиция (подпись ``scope_acceptance`` — per product).
    :param conditions: объявленные методологические условия прогона
        (:class:`~core.pipeline.eval_cache.EvalConditions`).
    :param scoring_policy_version: версия политики расчёта
        (:func:`core.pipeline.digests.scoring_policy_version`).
    :param model_id / llm_scenario / model_config_digest: маршрутизация и конфигурация
        оценщика (I21).
    :param attestation_digest: отпечаток действующих человеческих подписей (O13).
    """

    product_id: str
    conditions: EvalConditions
    scoring_policy_version: str
    model_id: str
    llm_scenario: str
    model_config_digest: str
    attestation_digest: str


@dataclass(frozen=True)
class CarriedCell:
    """Результат переноса: ячейка текущего прогона и её доказательства.

    Не персистится модулем: запись — обязанность Ш6, которая одна знает, как писать
    ячейку и evidence (``_persist``) и какие правила допуска к ней применить.
    """

    cell: Cell
    #: Копии evidence с детерминированными ``evidence_id`` текущего прогона (S8).
    evidence: list[CellEvidence]
    #: Протокол отсутствия с ``evidence_id`` текущего прогона.
    absence_evidence: list[dict[str, Any]]
    #: Ключ, которым помечена перенесённая ячейка (он же — ключ прошлого прогона).
    cell_reuse_key: str
    #: Отпечаток retrieved-набора текущей ячейки (per-cell, §10.1).
    retrieved_doc_set_digest: str
    #: ``prompt_hash`` прошлого прогона (из строки ``cell_measurement``): перенесённый
    #: вердикт получен **тем** промптом, и записывать его как «неизвестно» значило бы
    #: терять провенанс. ``None`` — строка индекса промпт не сохраняла (adoption).
    prompt_hash: str | None = None


def model_config_digest(evaluator: Any) -> str:
    """Дайджест конфигурации оценщика (09 §10.1), 24 hex-символа.

    Материал — ``model_version``/``decoder``/``seed``/``temperature``/``endpoint_kind``:
    это те же условия, что входят в ключ кэша вердиктов (§10.5), но **без** промпта и
    правил — они идут отдельными слагаемыми ключа переноса. ``model_id`` сюда не входит:
    он в ключе отдельным полем, чтобы разбор расхождения называл именно модель.
    """
    payload = {
        "model_version": getattr(evaluator, "model_version", "") or "",
        "decoder": str(getattr(evaluator, "decoder", "none")),
        "seed": getattr(evaluator, "seed", None),
        "temperature": getattr(evaluator, "temperature", None),
        "endpoint_kind": str(getattr(evaluator, "endpoint_kind", "") or ""),
    }
    return short_hash(json.dumps(payload, ensure_ascii=False, sort_keys=True), 24)


def prompt_template_digest(param: CatalogRow) -> str:
    """Отпечаток **текущего** шаблона и правила параметра (09 §10.1), 24 hex-символа.

    Материал — версия шаблона (``TEMPLATE_VERSION``) и правило отсутствия параметра.
    Берётся **текущее** значение, а не ``prev.prompt_hash``: иначе смена шаблона или
    правила не инвалидировала бы перенос — ровно та ошибка, из-за которой ключ кэша
    вердиктов (§10.5) переехал в формат 2.

    Почему не ``prompt_hash``: он включает ``sorted(chunk_ids)``, а ``chunk_id`` несёт
    ``partition_build_id`` (16 §6). Пересборка партиции с тем же содержимым меняла бы
    ключ, хотя доказательства те же. Порядок выдачи (который тоже мог бы входить через
    отрендеренный текст) по §10.1 перенос не запрещает.
    """
    material = "\x1f".join(
        (
            TEMPLATE_VERSION,
            str(param.rule or ""),
            str(getattr(param.absence_rule, "value", param.absence_rule) or ""),
        )
    )
    return short_hash(material, 24)


def attestation_digest(product_id: str, scope_acceptance: str | None) -> str:
    """Отпечаток человеческих подписей продукта (O13), 24 hex-символа.

    Перенос не должен переносить **решение человека** неявно: ``scope_acceptance``
    авторизует публикацию ``❌`` при неполном пространстве (I2c), и её смена обязана
    инвалидировать перенос. Сегодня это единственный производитель человеческой подписи,
    доступный конвейеру: таблица ``state.db.attestation`` объявлена, но писателя в прогоне
    не имеет (§4.2), поэтому строить отпечаток из неё значило бы описывать несуществующий
    канал. ``reviewed_by``/``N/A`` едут вместе с ячейкой и видны в отчёте как
    ``measurement_origin=carried`` — это провенанс, а не скрытое решение.
    """
    material = f"{product_id}\x1f{'∅' if scope_acceptance is None else scope_acceptance}"
    return short_hash(material, 24)


def evidence_digest(evidence: Iterable[CellEvidence]) -> str:
    """Отпечаток набора evidence ячейки, 24 hex-символа.

    Материал — ``(source_id, source_kind, content_hash, citation_quote)``, отсортированный:
    ``evidence_id`` **не входит**, потому что он содержит ``run_id`` и у каждого прогона
    свой, — отпечаток обязан быть сравнимым между прогонами. Порядок не входит по той же
    причине, что и в :func:`core.pipeline.digests.retrieved_doc_set_digest`.

    Служит проверкой целостности: если набор evidence прошлого прогона пересчитался
    иначе, чем записанный отпечаток, переносить нечего — строка индекса описывает не то,
    что лежит в БД.
    """
    material = sorted(
        (
            str(ev.source_id),
            str(getattr(ev.source_kind, "value", ev.source_kind)),
            str(ev.content_hash or ""),
            str(ev.citation_quote or ""),
        )
        for ev in evidence
    )
    return short_hash(
        json.dumps(material, ensure_ascii=False, sort_keys=True), 24
    )


def cell_reuse_conditions(
    *,
    product_id: str,
    conditions: EvalConditions,
    scoring_policy_version: str,
    evaluator: Any,
    scope_acceptance: str | None,
    model_id: str | None = None,
) -> CellReuseConditions:
    """Собирает условия переноса продукта (09 §10.1) — **без** I/O.

    ``model_id`` разрешается так же, как в :func:`core.pipeline.evaluate.evaluate_cell`
    (явный аргумент → атрибут оценщика → маркер «не объявлена»): ключ обязан описывать ту
    модель, которая действительно работала, а не ту, которую хотелось бы назвать.
    """
    resolved_model = (
        model_id or getattr(evaluator, "model_id", "") or _UNSPECIFIED_MODEL
    )
    return CellReuseConditions(
        product_id=str(product_id),
        conditions=conditions,
        scoring_policy_version=str(scoring_policy_version),
        model_id=str(resolved_model),
        llm_scenario=_EVALUATOR_SCENARIO,
        model_config_digest=model_config_digest(evaluator),
        attestation_digest=attestation_digest(product_id, scope_acceptance),
    )


#: Маркер «модель не объявлена» — тот же смысл, что у ``evaluate._UNSPECIFIED_MODEL_ID``:
#: пустая строка слилась бы с «объявлена пустой», а это разные условия измерения.
_UNSPECIFIED_MODEL = "unspecified"


def _retrieval_key_parts(
    retrieval: RetrievalResult | None,
) -> tuple[str | None, int | None, str | None, str | None]:
    """``(profile, cutoff_K, retrieval_params_json, partition_build_id)`` среза.

    Берутся из **провенанса ретривала** (I16/I20), а не из аргументов фазы: ключ обязан
    описывать тот профиль/K, под которым фактически получен переданный срез. Строка
    ``retrieval_params_json`` строится той же формулой, что в провенансе ячейки
    (``evaluate._provenance``), — иначе ключ и ячейка описывали бы условия по-разному.
    """
    if retrieval is None:
        return None, None, None, None
    profile = retrieval.profile or None
    cutoff = int(retrieval.cutoff_K) if retrieval.cutoff_K else None
    params = json.dumps(
        {"profile": retrieval.profile, "cutoff_K": retrieval.cutoff_K},
        ensure_ascii=False,
        sort_keys=True,
    )
    return profile, cutoff, params, (retrieval.build_id or None)


def _partition_of(retrieved: Sequence[Any], fallback: str | None) -> str | None:
    """Продукт-партиция retrieved-набора: ``RetrievedChunk.build_id``.

    Именно партиция продукта, а не ``composition_id``: композиция меняется при пересборке
    **любой** партиции когорты, и перенос ячейки чужого продукта стал бы невозможен без
    изменения её собственных доказательств — прямо против смысла per-cell (§10.1).
    """
    for chunk in retrieved:
        build_id = str(getattr(chunk, "build_id", "") or "")
        if build_id:
            return build_id
    return fallback


def _retrieved_doc_versions(retrieved: Sequence[Any]) -> frozenset[str]:
    """Версии документов retrieved-набора текущей ячейки (O5).

    Источник — ``chunk_id`` каждого чанка (``product:doc_version_id:chunk_no``), разбор —
    :func:`core.util.ids.doc_version_of` (та же грамматика, что у писателя evidence в Ш6).
    Чанки с legacy-адресом (``product:content_hash:chunk_no``) версии не несут и в набор
    не попадают: у такой строки ``doc_version_id`` в БД не записан, и требование к ней не
    предъявляется — иначе исторические прогоны стали бы непереносимыми из-за формы адреса,
    а не из-за смены документа.
    """
    versions = {
        version
        for chunk in retrieved
        if (version := doc_version_of(str(getattr(chunk, "chunk_id", "") or "")))
    }
    return frozenset(versions)


def _evidence_chain_valid(
    evidence: Sequence[CellEvidence],
    *,
    product_id: str,
    retrieved_hashes: frozenset[str],
    retrieved_doc_versions: frozenset[str] = frozenset(),
) -> bool:
    """Цепочка доказательств переносимой ячейки (09 §10.2, I17/I19, O5).

    Проверяется ровно то, что делает перенос честным:

    * ``ev.product_id == product_id`` — I17: доказательство чужого продукта не переезжает;
    * ``ev.content_hash`` непуст и **входит в retrieved-набор текущей ячейки**: иначе
      переносилось бы доказательство из документа, которого ячейка в этом прогоне не
      видела, и вердикт выглядел бы подтверждённым текстом, которого у неё не было;
    * ``ev.doc_version_id`` (O5), если он записан, **тоже** входит в retrieved-набор
      версий: ``content_hash`` — версия текста и не различает уровень извлечения и версию
      экстрактора, поэтому один и тот же текст в другой версии документа проходил бы
      прежнюю проверку.

    **Проверка строже прежней, не слабее.** Адрес версии добавляется к требованию по
    ``content_hash``, а не заменяет его, поэтому набор разрешённых переносов только
    сжимается. Строки прошлых прогонов без ``doc_version_id`` (колонки не было) остаются
    на прежней проверке по ``content_hash`` — «не записано» не превращается ни в «любая
    версия подходит», ни в отказ читать прогон.

    Пустой набор evidence перенос не запрещает: ``❓`` без цитат — законный исход, и его
    защищает ключ (включая отпечаток retrieved-набора), а не цитата.
    """
    for ev in evidence:
        if str(ev.product_id) != str(product_id):
            return False
        content_hash = str(ev.content_hash or "")
        if not content_hash or content_hash not in retrieved_hashes:
            return False
        doc_version = str(ev.doc_version_id or "")
        if doc_version and doc_version not in retrieved_doc_versions:
            return False
    return True


def _absence_chain_valid(
    records: Sequence[dict[str, Any]],
    *,
    retrieved_hashes: frozenset[str],
    retrieved_doc_versions: frozenset[str] = frozenset(),
) -> bool:
    """Протокол отсутствия: цитируемые документы обязаны быть в retrieved-наборе.

    Записи без ``content_hash`` — уровень корпуса (протокол «искали и не нашли» по
    контуру), а не цитата из документа: их законность держится на отпечатке покрытия и
    правила ``❌``, а не на конкретном чанке. Записи **с** ``content_hash`` проверяются так
    же, как типизированный evidence: перенос не должен опираться на текст, которого
    ячейка в этом прогоне не видела. Записанный ``doc_version_id`` (O5) проверяется тем же
    правилом и так же **добавляется** к проверке по хешу.
    """
    for record in records:
        content_hash = str(record.get("content_hash") or "")
        if content_hash and content_hash not in retrieved_hashes:
            return False
        doc_version = str(record.get("doc_version_id") or "")
        if doc_version and doc_version not in retrieved_doc_versions:
            return False
    return True


def _copy_evidence(
    evidence: Sequence[CellEvidence],
    *,
    run_id: str,
    partition_build_id: str | None,
    index_build_id: str | None,
) -> list[CellEvidence]:
    """Копии evidence с детерминированными ``evidence_id`` текущего прогона (S8).

    ``evidence_id`` прошлого прогона содержит его ``run_id``, поэтому копия обязана
    получить свой: иначе PK ``cell_evidence`` столкнулся бы с чужой строкой (или, хуже,
    строка осталась бы адресована прошлому прогону). ``seq`` — позиция в порядке вставки,
    так что повторный перенос в тот же прогон идемпотентен.
    """
    copies: list[CellEvidence] = []
    for seq, ev in enumerate(evidence):
        copies.append(
            ev.model_copy(
                update={
                    "evidence_id": evidence_id_for(
                        run_id,
                        str(ev.product_id),
                        str(ev.param_id),
                        str(ev.source_id),
                        ev.source_kind,
                        seq,
                    ),
                    "run_id": run_id,
                    "partition_build_id": partition_build_id or ev.partition_build_id,
                    "index_build_id": index_build_id or ev.index_build_id,
                }
            )
        )
    return copies


def _copy_absence(
    records: Sequence[dict[str, Any]], *, run_id: str
) -> list[dict[str, Any]]:
    """Копии протокола отсутствия с ``evidence_id`` текущего прогона."""
    copies: list[dict[str, Any]] = []
    for seq, record in enumerate(records):
        copy = dict(record)
        copy["evidence_id"] = absence_evidence_id(
            run_id, str(record.get("product_id", "")), str(record.get("param_id", "")), seq
        )
        copy["run_id"] = run_id
        copies.append(copy)
    return copies


def carry_forward_cell(
    param: CatalogRow,
    *,
    product_id: str,
    run_id: str,
    prev_run_id: str | None,
    state: ProductStatePort | None,
    prev_store: StorePort | None,
    conditions: CellReuseConditions,
    coverage: Any | None,
    retrieved: Sequence[Any],
    retrieval: RetrievalResult | None = None,
    protected: Cell | None = None,
) -> CarriedCell | None:
    """Переносит ячейку из ``prev_run_id``, если её ключ совпал (09 §10.2).

    Возвращает ``None`` — «надо мерить»; это **не** ошибка и не «перенос не удался»:
    отсутствие предшественника, неполное покрытие и несовпадение ключа одинаково законны,
    и все они названы в докстринге модуля.

    :param param: строка каталога (её дайджест — слагаемое ключа).
    :param run_id: текущий прогон (``Cell.run_id`` копии).
    :param prev_run_id: прогон-предшественник; ``None`` — переносить не из чего (первый
        прогон или resume **текущего**: ``previous_run_id`` не указывает на самого себя).
    :param state: durable-состояние (строки ``cell_measurement``); ``None`` — механизм
        выключен (модульный вызов без ``state.db``).
    :param prev_store: БД прогона-предшественника (**только чтение**): в ней лежат ячейка
        и её evidence. ``None`` — перенос невозможен: без доказательств вердикт не переезжает.
    :param conditions: объявленные условия прогона (:func:`cell_reuse_conditions`).
    :param coverage: покрытие корпуса текущего прогона; ``None``/``complete=False`` —
        перенос запрещён (§10.3).
    :param retrieved: продукт-скоупед top-K **текущей** ячейки (I17/I20).
    :param retrieval: провенанс среза (профиль/K/сборка) — слагаемые ключа.
    :param protected: ячейка высокого источника текущего прогона (реестр/наследование);
        не ``None`` — перенос запрещён: прецедент источника сильнее корпусной копии.
    """
    if prev_run_id is None or prev_run_id == run_id:
        return None
    if state is None or prev_store is None:
        return None
    if protected is not None:
        return None
    if coverage is None or not bool(getattr(coverage, "complete", False)):
        # I2c: неполный корпус не доказывает отсутствия, и перенесённый вердикт выглядел бы
        # подтверждённым наблюдением, которого не было. Покрытие проверяется до чтения
        # строки измерения: это самый дешёвый и самый частый запрет.
        return None

    previous = state.get_cell_measurement(prev_run_id, product_id, param.param_id)
    if previous is None or not previous.cell_reuse_key:
        # Нет строки/ключа — сравнивать не с чем. Adoption ключи не восстанавливает
        # (``state_adopt``), поэтому принятый прогон не становится источником переноса.
        return None
    try:
        if CellState(previous.state) not in CARRYABLE_STATES:
            return None
    except ValueError:
        return None

    profile, cutoff_k, retrieval_params_json, build_id = _retrieval_key_parts(retrieval)
    partition_build_id = _partition_of(retrieved, build_id)
    retrieved_digest = retrieved_doc_set_digest(retrieved)
    key = cell_reuse_key(
        product_id=product_id,
        param_id=param.param_id,
        catalog_row_digest=catalog_row_digest(param),
        prompt_template_digest=prompt_template_digest(param),
        model_id=conditions.model_id,
        llm_scenario=conditions.llm_scenario,
        model_config_digest=conditions.model_config_digest,
        retrieval_profile=profile,
        cutoff_k=cutoff_k,
        retrieval_params_json=retrieval_params_json,
        retrieved_doc_set_digest=retrieved_digest,
        coverage_digest=coverage_digest(coverage),
        methodology_version=conditions.conditions.methodology_version,
        status_dict_version=conditions.conditions.status_dict_version,
        scoring_policy_version=conditions.scoring_policy_version,
        capability_policy=conditions.conditions.capability_policy,
        unknown_policy=conditions.conditions.unknown_policy,
        partial_score=conditions.conditions.partial_score,
        attestation_digest=conditions.attestation_digest,
        partition_build_id=partition_build_id,
    )
    if key != previous.cell_reuse_key:
        # §10.3: расхождение любого слагаемого — переоценка. Причина не угадывается:
        # ключ — отпечаток условий, и «что именно изменилось» читается сравнением строк.
        return None

    previous_cell = prev_store.get_cell(prev_run_id, product_id, param.param_id)
    if previous_cell is None:
        # Строка индекса есть, а ячейки нет — расхождение двух носителей. Переносить
        # нечего, и «тихий пропуск» здесь был бы скрытым дефектом записи.
        return None
    if previous_cell.stale_reason is not None:
        # Stale означает «вердикт описывает прошлую сборку»: снимается переоценкой
        # (``revalidate``), а не переносом.
        return None
    try:
        if CellState(previous_cell.state) not in CARRYABLE_STATES:
            return None
    except ValueError:
        return None

    evidence = prev_store.iter_cell_evidence(prev_run_id, product_id, param.param_id)
    retrieved_hashes = frozenset(
        str(getattr(chunk, "content_hash", "") or "") for chunk in retrieved
    )
    retrieved_hashes = frozenset(item for item in retrieved_hashes if item)
    retrieved_doc_versions = _retrieved_doc_versions(retrieved)
    if not _evidence_chain_valid(
        evidence,
        product_id=product_id,
        retrieved_hashes=retrieved_hashes,
        retrieved_doc_versions=retrieved_doc_versions,
    ):
        return None
    if not _absence_chain_valid(
        previous_cell.absence_evidence,
        retrieved_hashes=retrieved_hashes,
        retrieved_doc_versions=retrieved_doc_versions,
    ):
        return None
    if previous.evidence_digest and previous.evidence_digest != evidence_digest(evidence):
        # Отпечаток набора в строке индекса не совпал с тем, что лежит в БД: строка
        # описывает другие доказательства. Перенос по такой паре был бы переносом
        # вердикта без его основания.
        return None

    carried = previous_cell.model_copy(
        update={
            "run_id": run_id,
            "measurement_origin": MeasurementOrigin.CARRIED,
            "carried_from_run_id": prev_run_id,
            "cell_reuse_key": key,
            # Провенанс сборки — текущий: ячейка описывает нынешний прогон, а совпадение
            # ключа уже подтвердило, что доказательства те же.
            "index_build_id": build_id or previous_cell.index_build_id,
            "retrieval_profile": profile or previous_cell.retrieval_profile,
            "retrieval_params_json": (
                retrieval_params_json or previous_cell.retrieval_params_json
            ),
        }
    )
    return CarriedCell(
        cell=carried,
        evidence=_copy_evidence(
            evidence,
            run_id=run_id,
            partition_build_id=partition_build_id,
            index_build_id=build_id,
        ),
        absence_evidence=_copy_absence(previous_cell.absence_evidence, run_id=run_id),
        cell_reuse_key=key,
        retrieved_doc_set_digest=retrieved_digest,
        prompt_hash=previous.prompt_hash,
    )


def measurement_row(
    cell: Cell,
    *,
    param: CatalogRow,
    conditions: CellReuseConditions,
    coverage: Any | None,
    retrieval: RetrievalResult | None,
    retrieved: Sequence[Any],
    evidence_digest_value: str,
    measured_at: str,
    prompt_hash: str | None = None,
) -> CellMeasurementRow:
    """Строка ``cell_measurement`` для записанной ячейки (09 §4.2/§10.1).

    Пишется **и** для ``observed``, и для ``carried``: индекс переноса обязан описывать
    все ячейки прогона, иначе цепочка рвётся на втором цикле (перенесённая ячейка не
    стала бы источником для третьего прогона).

    ``doc_set_digest`` в per-cell таблице — набор документов **этой** ячейки
    (``retrieved_doc_set_digest``, схема ``cell-reuse-2``), а не doc-set продукта: таблица
    per-cell, и второй смысл в ней означал бы, что «набор документов» в строке и в ключе
    разный. Product-level digest сюда не пишется.

    Ключ берётся из ячейки, если он там уже есть (``carried``), и вычисляется заново —
    если нет (``observed``). Обе ветви дают **одну** формулу
    (:func:`core.pipeline.digests.cell_reuse_key`); разница только в том, кто её вызвал.

    :param retrieval: провенанс среза (профиль/K/сборка) — слагаемые ключа.
    :param retrieved: **тот** срез, по которому мерили (``chunks_by_param`` ячейки). Он
        передаётся отдельно, а не берётся из ``retrieval.chunks``: у ячейки с несколькими
        срезами (композиция плеч) провенанс и фактический вход оценщика — разные объекты,
        и ключ обязан описывать вход.
    """
    profile, cutoff_k, retrieval_params_json, build_id = _retrieval_key_parts(retrieval)
    retrieved_digest = retrieved_doc_set_digest(retrieved)
    partition_build_id = _partition_of(retrieved, build_id)
    key = cell.cell_reuse_key or cell_reuse_key(
        product_id=str(cell.product_id),
        param_id=str(cell.param_id),
        catalog_row_digest=catalog_row_digest(param),
        prompt_template_digest=prompt_template_digest(param),
        model_id=conditions.model_id,
        llm_scenario=conditions.llm_scenario,
        model_config_digest=conditions.model_config_digest,
        retrieval_profile=profile,
        cutoff_k=cutoff_k,
        retrieval_params_json=retrieval_params_json,
        retrieved_doc_set_digest=retrieved_digest,
        coverage_digest=coverage_digest(coverage),
        methodology_version=conditions.conditions.methodology_version,
        status_dict_version=conditions.conditions.status_dict_version,
        scoring_policy_version=conditions.scoring_policy_version,
        capability_policy=conditions.conditions.capability_policy,
        unknown_policy=conditions.conditions.unknown_policy,
        partial_score=conditions.conditions.partial_score,
        attestation_digest=conditions.attestation_digest,
        partition_build_id=partition_build_id,
    )
    return CellMeasurementRow(
        run_id=str(cell.run_id),
        product_id=str(cell.product_id),
        param_id=str(cell.param_id),
        state=str(getattr(cell.state, "value", cell.state)),
        status=(
            None if cell.status is None else str(getattr(cell.status, "value", cell.status))
        ),
        measured_at=measured_at,
        prompt_hash=prompt_hash,
        model_id=conditions.model_id,
        llm_scenario=conditions.llm_scenario,
        index_build_id=cell.index_build_id,
        partition_build_id=partition_build_id,
        doc_set_digest=retrieved_digest,
        coverage_digest=coverage_digest(coverage),
        catalog_row_digest=catalog_row_digest(param),
        cell_reuse_key=key,
        evidence_digest=evidence_digest_value,
    )
