"""coHound — объявленные оси исполнения (declared-слой execution contract, §13).

Модуль слоя ``cli`` (**composition root**, Z0): единственное место, где из
``(args, inputs, deploy)`` разрешаются **декларированные** оси исполнения. Значение
используется двумя потребителями без второго резолвера:

* :func:`core.pipeline.run_identity.build_execution_contract` — отпечаток условий
  измерения (``execution_contract_digest`` → ``run_id``/lineage/``hound diff``);
* :func:`core.pipeline.report.build_manifest` — поля ``run_manifest.yaml``.

**Почему эти девять осей объявлены, а наблюдаемые — нет.** Предикат — «функция входа»
против «функции собранных портов»:

* объявленная ось вычислима на старте из ``args``/``run.yaml``/файлов пакета/
  ``deploy.yaml``/констант кода, то есть **до** сборки портов и корпуса. Два хоста с
  одинаковыми входами получат одно и то же значение (R7 §11.7);
* наблюдаемая величина (``extractor_version``, ``embed_model_id``,
  ``lexical_engine_version``, ``vector_store``) — функция **собранных** портов/диска:
  версия экстрактора — per-product композит фазы корпуса, эмбеддер читается с диска
  при сборке индекса, лексический движок и векторное хранилище зависят от
  установленных wheel'ов. Такие значения остаются провенансом манифеста/``pb``, но в
  контракт **не** входят: иначе два хоста с одним ``run.yaml`` дали бы разный
  ``run_id``, сломав воспроизводимость (R7 §11.7) и запретив diff по причине, которую
  нельзя объявить на входе (методология 2.4.0, §13).

``extractor_stack`` — **объявленное** поле контракта: константы стека извлечения
(HTML-адаптер + семейство docling-адаптера + фактическая версия установленной
библиотеки ``docling``). Сборка движка (``docling-<version>``) берётся из
:func:`core.adapters.extract_document.docling_engine_build` — **того же** вызова, из
которого строится ``extractor_version`` документа (``docling-doc-3:<level>/<device>/docling-<build>/<policy>``,
ключ ``extract_cache`` и слагаемое ``pb``). Две независимые формулы разошлись бы, и
``run_id`` объявлял бы движок, которого не было (Д-8, §2.5). ``extractor_version`` при
этом остаётся наблюдаемой величиной и в контракт не входит: в ``run_id`` едет только
объявленный стек, поэтому добавление сборки в ключ кэша ``run_id`` не меняет.

``DOCLING_ADAPTER_FAMILY`` — реэкспорт из адаптера, чтобы у строки семейства был **один**
источник (``__all__`` сохраняет прежнюю публичную поверхность модуля). Уровень извлечения —
отдельная операционная ось ``crawl.document_level``, уже входящая в
``crawl_config_digest``; дублировать его в стеке значило бы считать одну ось дважды.

``chunker_version`` — **композит** правил нарезки, а не версия одного чанкера: у plain text
свой абзацный чанкер, у markdown/HTML/EPUB — заголовочный, поэтому значение —
``"md-heading-4+text-para-2"`` (:func:`core.adapters.chunker_family.composite_chunker_version`).
Формула одна на декларацию и на собранный порт: разойдись они — ``execution_contract_digest``
описывал бы не ту нарезку, которой получен корпус (R7 §11.7). Смена строки — версионная
граница класса ``execution_versions`` (``execution_change``, §13): меняется ``run_id``,
``methodology_version`` — нет.

**Инвентарь операторских осей краула — один список.** :data:`CRAWL_AXIS_DESTS` называет
и флаги CLI, и слагаемые ``crawl_config_digest`` одной записью на ось; его читают и
:func:`add_crawl_axis_flags` (регистрация флагов), и :func:`_crawl_axis_overrides` (значения
в дайджест). Причина — измеренный дефект: ``--max-depth``/``--max-documents``/``--exclude-*``
применялись только при сборке фаз, поэтому прогон с другим корпусом мог жить под прежним
``run_id``. Список осей разъезжается молча — поэтому он назван один раз.

**Инвентарь флагов идентичности — тоже один список.** :data:`CONTRACT_FLAGS` называет
флаги, чьи значения входят в идентичность прогона, и **материал** каждого
(:data:`IDENTITY_MATERIALS`): значения ``retrieval_profile``/``cutoff_K`` идут в
:func:`core.pipeline.run_identity.build_execution_contract` отдельными аргументами (так их
передаёт ``run``), поэтому «декларированными осями» они не являются и в датакласс не
переносятся; остальные питают другие слагаемые ``run_id`` (09 §6.2) — ``seeds_digest``
(``--urls``), ``cycle_id`` (``--cycle-id``/``--update``), ``run_scope``
(``--only-failed``/``--only-product``), ``refresh_policy`` (``--refresh``). Одна запись
инвентаря описывает ``dest`` флага, форму и то, что он питает; её читают
:func:`add_contract_flags` (регистрация), :func:`contract_flag_values` (значения в
контракт) и :func:`identity_flag_values` (значения в прочий материал ``run_id``).

Причина, по которой список назван один раз, измерена дважды. Сначала ``plan`` не принимал
``--retrieval-profile``/``--cutoff-k`` и печатал **другой** ``run_id``, чем прогон. Затем
то же обнаружилось у ``--urls``/``--cycle-id``/``--update``/``--only-failed``/
``--only-product``/``--refresh``: они меняли ``run_id`` у ``run`` и отсутствовали у
``plan``. Инструмент, чья единственная задача — предсказать идентичность до запуска,
обязан предсказывать верно, а не «для тех флагов, которые вспомнили».

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.adapters.chunker_family import (
    composite_chunker_version,
    composite_chunking_spec,
)
from core.adapters.extract_document import (
    DOCLING_ADAPTER_FAMILY,
    DOCLING_MARKDOWN_POLICY,
    DOCLING_OFFICE_ADAPTER_FAMILY,
    DOCLING_TEXT_FINALIZE,
    docling_engine_build,
)
from core.adapters.extract_epub import EPUB_READER_VERSION
from core.adapters.extract_html import HtmlExtractor
from core.adapters.extract_pdf_text import PDFIUM_ADAPTER_VERSION
from core.adapters.extract_text import TextExtractor
from core.adapters.lexical_tantivy import lexical_schema_spec
from core.adapters.llm_local import (
    EVALUATOR_DEFAULT_SEED,
    EVALUATOR_DEFAULT_TEMPERATURE,
    decoder_wire_type,
    endpoint_host,
)
from core.cli.context import (
    _document_level,
    _load_registry_specs,
    _load_render_modes,
    _non_negative_int,
    _pdf_pipeline,
    _positive_int,
    _resolve_passports_dir,
)
from core.cli.errors import EXIT_USAGE, CliError
from core.cli.wiring import (
    manifest_deploy_fields,
    registry_specs_digest,
    resolve_evaluator_model_identity,
)
from core.config.deploy import (
    ENV_SERP,
    DeployConfig,
    resolve_llm_slot,
    resolve_serp_rounds,
)
from core.config.passports import passport_content_digests
from core.domain.dictionary import load_dictionary
from core.domain.enums import PdfDevice, RefreshPolicy, SearchMode
from core.domain.models import CrawlRunConfig, canonical_rate_limit
from core.pipeline.crawl_frontier import resolve_embed_device, resolve_pdf_device
from core.pipeline.digests import passports_digest, render_modes_digest
from core.pipeline.evaluation_policy import evaluation_policy_spec
from core.pipeline.manual_artifacts import load_manual_artifacts
from core.pipeline.normalize import RunInputs
from core.pipeline.report import crawl_control_digest
from core.pipeline.retrieve import RetrievalProfile

__all__ = [
    "CONTRACT_FLAGS",
    "CONTRACT_FLAG_DESTS",
    "CRAWL_AXIS_DESTS",
    "DOCLING_ADAPTER_FAMILY",
    "IDENTITY_MATERIALS",
    "MATERIAL_CONTRACT",
    "MATERIAL_CYCLE",
    "MATERIAL_REFRESH",
    "MATERIAL_SCOPE",
    "MATERIAL_SEEDS",
    "ContractFlag",
    "DeclaredExecutionAxes",
    "IdentityFlags",
    "add_contract_flags",
    "add_crawl_axis_flags",
    "add_serp_rounds_flag",
    "contract_flag_values",
    "declared_status_dict_version",
    "extractor_stack_version",
    "identity_flag_values",
    "resolve_declared_axes",
]

#: Путь словаря статусов относительно корня репозитория (источник ``status_dict_version``).
_STATUS_DICTIONARY_PATH = (
    Path(__file__).resolve().parents[2] / "docs" / "status-dictionary.yaml"
)

#: **Единый инвентарь операторских осей краула** (CLI), §13/AGENTS «не менять ось краула
#: на месте».
#:
#: ``dest`` — одновременно имя атрибута ``args`` и ключ ``CrawlLimits.as_dict()``: одна
#: запись описывает и флаг, и слагаемое ``crawl_config_digest``. Потребители:
#:
#: * :func:`add_crawl_axis_flags` — регистрация флагов (``plan``; ``run``/``crawl``
#:   объявляют те же флаги в ``_add_phase4_flags``, паритет проверяется тестом);
#: * :func:`_crawl_axis_overrides` — чтение значений в дайджест (общий код ``run``/``plan``
#:   через :func:`resolve_declared_axes`).
#:
#: Список — не удобство, а страховка от того дефекта, который уже был: ``--max-depth`` и
#: ``--max-documents`` применялись только при сборке фаз, поэтому другой корпус жил под
#: прежним ``run_id``. Новая ось добавляется **здесь**, и оба потребителя видят её сразу.
CRAWL_AXIS_DESTS: tuple[str, ...] = (
    "max_depth",
    "max_documents",
    "exclude_paths",
    "exclude_documents",
    "document_level",
    "pdf_pipeline",
    "document_timeout_s",
    "host_concurrency",
    "extract_backlog_mb",
)


def add_crawl_axis_flags(parser: argparse.ArgumentParser) -> None:
    """Регистрирует операторские оси краула (:data:`CRAWL_AXIS_DESTS`) на парсере.

    Форма флагов, типы и help **совпадают** с ``run``/``crawl`` (``_add_phase4_flags``):
    ``plan`` обязан принимать тот же вход, иначе напечатанный ``run_id`` описывает не тот
    прогон, который можно запустить. Расхождение ловит
    ``tests/cli/test_plan_crawl_axes.py::test_crawl_flag_inventory_matches_run``.

    Функция — регистрация, не разрешение: значения читает
    :func:`_crawl_axis_overrides`. ``plan`` ничего не пишет и не исполняет.
    """
    parser.add_argument(
        "--max-depth",
        dest="max_depth",
        type=_non_negative_int,
        default=None,
        metavar="D",
        help="глубина BFS-обхода in-site ссылок (0 — только seeds; дефолт 0)",
    )
    parser.add_argument(
        "--max-documents",
        dest="max_documents",
        type=_positive_int,
        default=None,
        metavar="N",
        help="потолок добытых страниц на (продукт, seed-root); усечение → partial-сеанс",
    )
    parser.add_argument(
        "--exclude-path",
        dest="exclude_paths",
        action="append",
        default=None,
        metavar="GLOB",
        help="glob блэклиста по пути URL (fnmatchcase, повторяемый)",
    )
    parser.add_argument(
        "--exclude-document",
        dest="exclude_documents",
        action="append",
        default=None,
        metavar="GLOB",
        help="glob блэклиста по basename URL, напр. '*.pdf' (повторяемый)",
    )
    parser.add_argument(
        "--document-level",
        dest="document_level",
        type=_document_level,
        default=None,
        metavar="LEVEL",
        help=(
            "уровень извлечения документов: off|text|layout|layout_tables|full "
            "(дефолт full = сегодняшнее поведение)"
        ),
    )
    parser.add_argument(
        "--pdf-pipeline",
        dest="pdf_pipeline",
        type=_pdf_pipeline,
        default=None,
        metavar="PIPELINE",
        help=(
            "механизм извлечения PDF: structured|text_layer "
            "(дефолт structured = docling: layout/таблицы/OCR)"
        ),
    )


    parser.add_argument(
        "--document-timeout",
        dest="document_timeout_s",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            "потолок времени на извлечение одного документа в секундах "
            "(дефолт 1800 = сегодняшнее поведение; ось входит в run_id при объявлении)"
        ),
    )
    parser.add_argument(
        "--host-concurrency",
        dest="host_concurrency",
        type=_positive_int,
        default=None,
        metavar="N",
        help=(
            "сколько host'ов процесс краула ведёт одновременно "
            "(дефолт: последовательно, как сегодня; ось входит в run_id при объявлении)"
        ),
    )
    # F6 (план 17, §4.3): watermark бэклога конверсии. Не «для безопасности»: предел
    # измеряет, сколько байтов ждёт конверсии, прежде чем приём приостановить видимо.
    parser.add_argument(
        "--extract-backlog",
        dest="extract_backlog_mb",
        type=_positive_int,
        default=None,
        metavar="MB",
        help=(
            "предел бэклога конверсии в МБ retained-файлов очереди: при достижении "
            "приём приостанавливается и публикуется crawl_backpressure (дефолт: предел "
            "не объявлен; ось входит в run_id при объявлении)"
        ),
    )

def add_serp_rounds_flag(parser: argparse.ArgumentParser) -> None:
    """Флаг ``--serp-rounds`` (I3/§5) для входов, считающих identity.

    Раунды SERP-контура входят в ``EXECUTION_CONTRACT_FIELDS`` (``serp_rounds``), поэтому
    ``plan`` обязан принимать флаг наравне с ``run``: иначе напечатанный ``run_id`` не
    описывает прогон с поиском. Приоритет значения — один и тот же резолвер
    (:func:`core.config.deploy.resolve_serp_rounds`) для обоих входов:
    ``флаг > COHOUND_SERP > run.yaml: serp.rounds > deploy.search.serp.rounds > 0``.
    """
    parser.add_argument(
        "--serp-rounds",
        dest="serp_rounds",
        default=None,
        metavar="N",
        help="раунды SERP-контура внешнего поиска (0 — выключить). Приоритет: "
        "флаг > COHOUND_SERP > run.yaml: serp.rounds > deploy.search.serp.rounds > 0. "
        "Значение входит в execution contract (serp_rounds) ⇒ новый run_id; бюджет "
        "объявляется в run.yaml: serp.budget",
    )


#: Материал флага идентичности: **что именно** питает его значение в ``run_id`` (§13).
#: Инвентарь (:data:`CONTRACT_FLAGS`) называет материал каждой записи, поэтому «второго
#: списка» нет: регистрация (:func:`add_contract_flags`), чтение в контракт
#: (:func:`contract_flag_values`), чтение в прочие слагаемые ``run_id``
#: (:func:`identity_flag_values`) и сторож паритета в тестах читают одну запись.
#:
#: * :data:`MATERIAL_CONTRACT` — аргумент
#:   :func:`core.pipeline.run_identity.build_execution_contract`
#:   (``retrieval_profile``/``cutoff_K``);
#: * :data:`MATERIAL_SEEDS` — ``seeds_digest`` (``--urls`` заменяет seed-набор);
#: * :data:`MATERIAL_CYCLE` — ``cycle_id`` (``--cycle-id``/``--update``);
#: * :data:`MATERIAL_SCOPE` — ``run_scope`` (``--only-failed``/``--only-product``);
#: * :data:`MATERIAL_REFRESH` — ``refresh_policy`` (``--refresh``).
MATERIAL_CONTRACT = "contract"
MATERIAL_SEEDS = "seeds"
MATERIAL_CYCLE = "cycle"
MATERIAL_SCOPE = "scope"
MATERIAL_REFRESH = "refresh"

#: Допустимые материалы (закрытый список: новый материал — новая ветка чтения).
IDENTITY_MATERIALS: tuple[str, ...] = (
    MATERIAL_CONTRACT,
    MATERIAL_SEEDS,
    MATERIAL_CYCLE,
    MATERIAL_SCOPE,
    MATERIAL_REFRESH,
)


@dataclass(frozen=True)
class ContractFlag:
    """Одна запись инвентаря флагов идентичности (:data:`CONTRACT_FLAGS`).

    :param dest: имя атрибута ``args`` (то же, что ``dest`` флага);
    :param flag: option string CLI (``--retrieval-profile``);
    :param help: help флага — **тот же**, что у ``run``/``crawl`` (паритет проверяется
        тестом, читающим собранные парсеры);
    :param material: что значение питает в идентичности — один из
        :data:`IDENTITY_MATERIALS`;
    :param contract_arg: имя параметра
        :func:`core.pipeline.run_identity.build_execution_contract`; ``None`` — флаг питает
        не контракт, а иное слагаемое ``run_id`` (материал в :attr:`material`);
    :param metavar: подпись значения в usage (``None`` — как у ``run``: без подписи);
    :param choices: допустимые значения (``None`` — свободное значение);
    :param type: конвертер argparse (``None`` — ``str`` по умолчанию);
    :param action: ``action`` argparse (``"store_true"`` для булевых флагов);
    :param default: дефолт argparse. ``None`` — «не объявлено» для value-флагов,
        ``False`` — «не включено» для булевых: прежнее поведение, при котором отсутствие
        флага равно его выключенности, а не третьему состоянию.

    Инвариант формы проверяется в :meth:`__post_init__`: материал ``contract`` обязан
    нести ``contract_arg`` (иначе значение молча не попало бы в контракт), а любой иной
    материал — не нести (иначе флаг читался бы как аргумент контракта, которого у
    :func:`~core.pipeline.run_identity.build_execution_contract` нет).
    """

    dest: str
    flag: str
    help: str
    material: str = MATERIAL_CONTRACT
    contract_arg: str | None = None
    metavar: str | None = None
    choices: tuple[str, ...] | None = None
    type: Any = None
    action: str | None = None
    default: Any = None

    def __post_init__(self) -> None:
        if self.material not in IDENTITY_MATERIALS:
            raise ValueError(
                f"contract flag {self.flag}: неизвестный материал {self.material!r} "
                f"(допустимо: {IDENTITY_MATERIALS})"
            )
        is_contract = self.material == MATERIAL_CONTRACT
        if is_contract != (self.contract_arg is not None):
            raise ValueError(
                f"contract flag {self.flag}: material={self.material!r}, "
                f"contract_arg={self.contract_arg!r} — материал contract требует "
                "contract_arg, иной материал его запрещает"
            )


#: **Единый инвентарь флагов идентичности** (§13, I16/I20, 09 §6.2).
#: ``dest`` — имя атрибута ``args``; ``material`` — что значение питает; ``contract_arg``
#: — имя параметра :func:`core.pipeline.run_identity.build_execution_contract` (у
#: ``--cutoff-k`` оно отличается: контрактное поле называется ``cutoff_K``). Потребители:
#:
#: * :func:`add_contract_flags` — регистрация флагов (``plan``; ``run``/``crawl``
#:   объявляют те же флаги собственными местами регистрации, паритет проверяется тестом,
#:   читающим собранные парсеры);
#: * :func:`contract_flag_values` — значения в контракт (``plan``; ``run`` читает те же
#:   ``args``-атрибуты в своём composition root);
#: * :func:`identity_flag_values` — значения в прочие слагаемые ``run_id``
#:   (``cycle_id``/``run_scope``/``refresh_policy``/``seeds_digest``).
#:
#: Почему часть записей — не поля :class:`DeclaredExecutionAxes`: профиль/``cutoff_K``
#: попадают в контракт **отдельными аргументами**
#: :func:`~core.pipeline.run_identity.build_execution_contract` (так их передаёт ``run``),
#: а ``--cycle-id``/``--update``/``--only-failed``/``--only-product``/``--refresh``/``--urls``
#: вообще не являются материалом контракта — они входят в ``derive_run_id``/``seeds_digest``.
#: Переносить их в датакласс значило бы завести вторую форму разрешения рядом с той,
#: которой уже пользуется ``run``, — ровно тот дефект, от которого этот модуль
#: избавляется. Список один, и он же — материал теста паритета: новая ось добавляется
#: **здесь**, и оба входа видят её сразу.
CONTRACT_FLAGS: tuple[ContractFlag, ...] = (
    ContractFlag(
        # I16: профиль ретривала — условие измерения, а не настройка удобства.
        dest="retrieval_profile",
        flag="--retrieval-profile",
        contract_arg="retrieval_profile",
        choices=tuple(profile.value for profile in RetrievalProfile),
        metavar="PROFILE",
        help="зафиксированный профиль ретривала R0|R1|R2 (I16). Изменение измеренной "
        "настройки требует пере-измерения (`hound experiment retrieval`, A3)",
    ),
    ContractFlag(
        # I20: один K на цикл (golden и ретривал режутся одним значением).
        dest="cutoff_k",
        flag="--cutoff-k",
        contract_arg="cutoff_K",
        type=_positive_int,
        metavar="K",
        help="retrieval_cutoff_K (I20) выреза ретривала для фазы оценки; дефолт — "
        "DEFAULT_RETRIEVAL_CUTOFF_K (10)",
    ),
    ContractFlag(
        # ``--urls`` — полная замена пакетного seed-набора, то есть ``seeds_digest`` —
        # поле контракта. Полагать его «операционным» значило бы дать корпусу измениться
        # под прежним ``run_id``.
        dest="urls",
        flag="--urls",
        material=MATERIAL_SEEDS,
        help="опциональный JSON/YAML {product_id: [url, ...]} — операторский корпусный "
        "вход Ш5; без него корпус URL пуст (doc_domains паспортов не фетчатся молча)",
    ),
    ContractFlag(
        # ``cycle_id`` — слагаемое ``derive_run_id``: другой цикл = другой прогон.
        dest="cycle_id",
        flag="--cycle-id",
        material=MATERIAL_CYCLE,
        help="идентификатор цикла (ISO-дата/ISO-неделя); без --update — "
        "run.yaml:cycle_id > run.yaml:snapshot_date",
    ),
    ContractFlag(
        # ``--update`` не меняет материал напрямую, но переключает ветку резолва цикла
        # (``today()`` вместо ``snapshot_date``), значит меняет ``cycle_id`` → ``run_id``.
        dest="update",
        flag="--update",
        material=MATERIAL_CYCLE,
        action="store_true",
        default=False,
        help="явно начать новый цикл: требует cycle_id, отличного от предыдущего (09 §2.4)",
    ),
    ContractFlag(
        # ``only_failed``/``only_product`` — ``run_scope``, слагаемое ``run_id``: иначе
        # scoped-прогон совпал бы с полным и был бы пропущен как resume (09 §6.2).
        dest="only_failed",
        flag="--only-failed",
        material=MATERIAL_SCOPE,
        action="store_true",
        default=False,
        help="только провалы: FAILED текущего сеанса, иначе url_state прошлого прогона; "
        "ремонт идёт известными URL (авторские корни не обходятся)",
    ),
    ContractFlag(
        dest="only_product",
        flag="--only-product",
        material=MATERIAL_SCOPE,
        metavar="ID",
        help="только указанный продукт из --urls",
    ),
    ContractFlag(
        # ``refresh_policy`` — тоже слагаемое ``run_id``: политика наблюдения корпуса
        # решает, будет ли он пере-прочитан, то есть другим ли будет измерение.
        dest="refresh",
        flag="--refresh",
        material=MATERIAL_REFRESH,
        choices=tuple(policy.value for policy in RefreshPolicy),
        help="политика обновления корпуса: auto|revalidate|refresh|offline (дефолт auto)",
    ),
)

#: ``dest``-ы инвентаря — то, что проверяется паритетом ``run``/``plan`` по собранным
#: парсерам (``tests/cli/test_plan_contract_flags.py``). Выводится из
#: :data:`CONTRACT_FLAGS`, чтобы второго списка не было.
CONTRACT_FLAG_DESTS: tuple[str, ...] = tuple(spec.dest for spec in CONTRACT_FLAGS)


def add_contract_flags(parser: argparse.ArgumentParser) -> None:
    """Регистрирует флаги идентичности (:data:`CONTRACT_FLAGS`) на парсере.

    Флаги **порождены** инвентарём: ни ``option string``, ни ``dest``, ни тип/``choices``/
    ``metavar``/``action``/``default``, ни help не написаны здесь второй раз. Форма должна
    совпадать с ``run``/``crawl`` (места регистрации — ``configure`` и
    ``_add_phase4_flags``): ``plan`` обязан принимать тот же вход, иначе напечатанный
    ``run_id`` описывает не тот прогон, который можно запустить. Расхождение ловит
    ``tests/cli/test_plan_contract_flags.py::test_contract_flag_inventory_matches_run``,
    который читает **собранные** парсеры, а не этот код.

    Значения читают :func:`contract_flag_values` (материал контракта) и
    :func:`identity_flag_values` (прочие слагаемые ``run_id``); ``plan`` ничего не пишет
    и не исполняет.
    """
    for spec in CONTRACT_FLAGS:
        kwargs: dict[str, Any] = {
            "dest": spec.dest,
            "default": spec.default,
            "help": spec.help,
        }
        if spec.action is not None:
            kwargs["action"] = spec.action
        if spec.metavar is not None:
            kwargs["metavar"] = spec.metavar
        if spec.choices is not None:
            kwargs["choices"] = list(spec.choices)
        if spec.type is not None:
            kwargs["type"] = spec.type
        parser.add_argument(spec.flag, **kwargs)


def contract_flag_values(args: argparse.Namespace) -> dict[str, Any]:
    """Контрактные флаги — **под именами параметров контракта**.

    Возвращает ``{"retrieval_profile": ..., "cutoff_K": ...}``: словарь можно передать
    :func:`core.pipeline.run_identity.build_execution_contract` как ``**kwargs``. ``None``
    означает «флаг не объявлен» — контрактная доктрина фиксирует это как ``null``, а не
    подменяет дефолтом Ш6: ``run_id`` описывает объявленное, а не предположенное.

    Читаются ровно записи материала :data:`MATERIAL_CONTRACT`: список флагов и их чтение —
    одна запись, поэтому разъехаться молча они не могут. Флаги иных материалов сюда **не**
    попадают: их ``dest`` не являются параметрами ``build_execution_contract``, и такой
    ``**kwargs`` был бы ``TypeError`` — «широкий» словарь здесь дефект, а не запас.
    """
    return {
        spec.contract_arg: getattr(args, spec.dest, None)
        for spec in CONTRACT_FLAGS
        if spec.material == MATERIAL_CONTRACT
    }


@dataclass(frozen=True)
class IdentityFlags:
    """Флаги идентичности, питающие ``run_id`` **вне** execution contract (09 §6.2).

    Поля названы ``dest`` инвентаря: ``IdentityFlags(**{spec.dest: ...})`` падает
    ``TypeError``, если в :data:`CONTRACT_FLAGS` появился флаг идентичности, а этот
    читатель его не знает. Разъехаться молча список флагов и их чтение не могут — то же
    свойство, что у :func:`contract_flag_values`, но для ``seeds_digest``/``cycle_id``/
    ``run_scope``/``refresh_policy``.

    ``None``/``False`` у поля означают «флаг не объявлен»: план не подставляет дефолт
    вместо объявленного значения. Дефолты применяют те же резолверы, что у прогона
    (:func:`core.pipeline.run_identity.resolve_cycle_id`/``resolve_run_scope``/
    ``resolve_refresh_policy``) — второй формулы нет.
    """

    urls: str | None
    cycle_id: str | None
    update: bool
    only_failed: bool
    only_product: str | None
    refresh: str | None


def identity_flag_values(args: argparse.Namespace) -> IdentityFlags:
    """Значения флагов идентичности, кроме контрактных — по ``dest`` инвентаря.

    Читаются ровно записи :data:`CONTRACT_FLAGS` с материалом, отличным от
    :data:`MATERIAL_CONTRACT` (материал контракта читает :func:`contract_flag_values`:
    он идёт в другой аргумент и под другим именем).
    """
    values = {
        spec.dest: getattr(args, spec.dest, None)
        for spec in CONTRACT_FLAGS
        if spec.material != MATERIAL_CONTRACT
    }
    return IdentityFlags(**values)


@dataclass(frozen=True)
class DeclaredExecutionAxes:
    """Декларированные оси исполнения — вход контракта и манифеста (§13).

    Все значения — «функция входа» (``args``/``run.yaml``/файлы пакета/``deploy.yaml``/
    константы кода); наблюдаемые величины портов/диска здесь отсутствуют по построению
    (см. докстринг модуля). ``llm_scenarios`` — кортеж отображений (детерминированная
    таблица §7.4/I21 без ``run_id``: идентификатор прогона выводится из контракта и не
    может входить в его материал).
    """

    crawl_strategy: str | None
    crawl_rate_limit: str | None
    crawl_config_digest: str | None
    #: I3/§5: число раундов SERP-контура и **уровень**, который его задал
    #: (``cli``/``env``/``run.yaml``/``deploy.yaml``/``default``). Раунды входят в
    #: контракт (``serp_rounds``): они меняют корпус, значит и условия измерения.
    serp_rounds: int | None
    serp_source: str | None
    #: Режим запроса внешнего поиска (``run.yaml: serp.mode``). Решается **кодом** из
    #: объявленного входа: ``None`` в run.yaml ⇒ эффективный дефолт ``async`` всё равно
    #: объявляется, иначе ``run_id`` не описывал бы, каким бэкендом снят корпус.
    serp_mode: str | None
    registry_spec_digest: str | None
    deploy_profile: str | None
    edge_schema_version: str | None
    chunker_version: str | None
    llm_scenarios: tuple[dict[str, Any], ...]
    extractor_stack: str
    #: Устройство эмбеддера (ось ``embed.device``, :class:`core.domain.enums.EmbedDevice`):
    #: строка ``cpu``/``cuda`` из той же цепочки ``app < run``, что увидит исполнение
    #: (:func:`core.pipeline.crawl_frontier.resolve_embed_device`). Входит в контракт
    #: (поле ``embed_device``), потому что векторы CPU и CUDA численно разные: без оси
    #: ``run_id`` не описывал бы, на чём считался индекс. ``None`` — ось не передана
    #: (чистые вызовы/legacy-конструкторы в тестах): «не объявлено», а не «cpu».
    embed_device: str | None = None
    #: 13-manual-artifacts §3.2: отпечаток объявленных ручных артефактов
    #: (``sources/manual_artifacts.yaml``). Объявленный вход (файл пакета), поэтому
    #: вычислим до сборки портов; материал — ``(product_id, url, sha256)``, где
    #: ``sha256`` **вычислен из байтов** (объявленный — сверка, не основание доверия).
    #: ``None`` — резолвер не вызывался (DI/legacy): «неизвестно», не «пусто».
    manual_artifacts_digest: str | None = None
    #: C3: геометрия нарезки и состав входа эмбеддера — читается из самих чанкеров
    #: (:func:`core.adapters.chunker_family.composite_chunking_spec`), поэтому не может
    #: разъехаться с фактической нарезкой. ``None`` — ось не передана (чистые вызовы,
    #: legacy-конструкторы в тестах): «не объявлено», а не «выключено».
    chunking_spec: dict[str, Any] | None = None
    #: V3: политика повтора оценщика
    #: (:func:`core.pipeline.evaluation_policy.evaluation_policy_spec`). ``None`` — не объявлена.
    evaluation_policy: dict[str, Any] | None = None
    #: §11.1: объявленные входы **корпуса**. Оба определяют, какие байты станут
    #: доказательствами, и до правки не входили в ``run_id``: документ мог быть прочитан
    #: другим механизмом (браузер против HTTP) или паспорт мог быть другим, оставаясь под
    #: прежней идентичностью. ``None`` — ось не передана (чистые вызовы/legacy-
    #: конструкторы): «не объявлено» отличимо от «объявлено пустое множество».
    render_modes_digest: str | None = None
    passports_digest: str | None = None
    #: C8: схема лексического индекса — читается из адаптера, а не из копии констант:
    #: разъехаться с фактической схемой она не может (тот же приём, что у
    #: ``chunking_spec``).
    lexical_spec: dict[str, Any] | None = None


def extractor_stack_version(
    pdf_device: PdfDevice | str = PdfDevice.CPU,
) -> str:
    """Декларированный стек извлечения — **все читатели**, производящие текст корпуса.

    Формат: ``<html>+<text>+<epub>+<pdfium>+<office>+<docling>/<device>/<build>/<policy>/<finalize>``.

    Пример: ``trafilatura-html-5+text-plain-2+epub-2+pdfium-text-2+docling-office-1+
    docling-doc-3/cpu/docling-2.135.0/md-plain-underscore/finalize-1``.

    **Почему здесь все читатели, а не только HTML и docling.** Поле объявлено ради
    одного: смена текста не имеет права пройти под прежним ``run_id`` (Д-8, A3). До
    02.11.2026 в строке были только HTML-читатель и docling, поэтому правка текстового
    читателя, читателя EPUB или механизма ``text_layer`` меняла корпус, не меняя
    идентичности прогона: два разных корпуса жили бы под одним ``run_id``. Каждый
    читатель назван здесь своим ``version`` — той же константой, что штампует документ
    и ключ ``extract_cache``, иначе две формулы разошлись бы молча (Д-8).

    ``<policy>`` — политика экспорта текста
    (:data:`core.adapters.extract_document.DOCLING_MARKDOWN_POLICY`). Она здесь по той же
    причине, что и сборка: текст документа от неё зависит (замер 2026-11-01: экранирование
    ``_`` давало 26 искажённых имён на одном PDF), а ``extractor_version`` в контракт не
    входит. Без этого сегмента правка политики меняла бы корпус под прежним ``run_id``.

    ``<finalize>`` — очистка текста после чтения
    (:data:`core.adapters.extract_document.DOCLING_TEXT_FINALIZE`): удаление управляющих
    символов и нейтрализация каркаса промпт-инъекций. Это та же граница, что и у
    HTML/текстового читателей (``core.util.sanitize.finalize_extracted_text``), поэтому
    её версия обязана быть в стеке: иначе документный путь менял бы корпус молча
    (дефект 02.11.2026: очистка не вызывалась на docling/``text_layer`` вовсе).

    ``<device>`` — объявленная ось ``crawl.pdf_device`` (P9). Она здесь потому, что
    ``extractor_version`` **наблюдаем** и в контракт не входит (методология 2.4.0: он
    остаётся провенансом), а устройство — условие измерения: текст документа от него
    зависит, и без сегмента ``cpu``/``cuda`` два прогона с разным устройством имели бы
    один ``run_id``. Поле контракта при этом не новое: ``extractor_stack`` и есть
    объявленный аналог ``extractor_version``.

    **Оба механизма PDF названы, хотя в прогоне работает один.** Выбор механизма —
    объявленная ось (``crawl.pdf_pipeline``, входит в ``crawl_config_digest``), а версия
    читателя — нет. Можно было бы включать ``pdfium`` только при ``text_layer``, но тогда
    функция стала бы зависеть от резолва оси (второй формулы), а лишнее изменение
    ``run_id`` при правке неиспользуемого читателя безопаснее молчаливой смены корпуса.
    У офисной ветви своего ``device``/``level`` нет вовсе — ``SimplePipeline`` не считает
    моделями; поэтому она названа одним семейством без хвоста
    (:func:`core.adapters.extract_document.docling_office_version`).
    """
    device = PdfDevice(pdf_device)
    return (
        f"{HtmlExtractor.version}+{TextExtractor.version}+{EPUB_READER_VERSION}"
        f"+{PDFIUM_ADAPTER_VERSION}+{DOCLING_OFFICE_ADAPTER_FAMILY}"
        f"+{DOCLING_ADAPTER_FAMILY}/{device.value}/"
        f"{docling_engine_build()}/{DOCLING_MARKDOWN_POLICY}/{DOCLING_TEXT_FINALIZE}"
    )


def declared_status_dict_version() -> str | None:
    """``status_dict_version`` из ``docs/status-dictionary.yaml`` (честный источник).

    Словарь — источник истины перечислений (§4.1); его версия — декларированная
    величина. Файл отсутствует (установка без ``docs/``) → ``None``: значение не
    подменяется правдоподобным дефолтом.
    """
    try:
        data = load_dictionary(_STATUS_DICTIONARY_PATH)
    except (OSError, ValueError):
        return None
    version = data.get("status_dictionary_version")
    return str(version) if version is not None else None


def _crawl_strategy(run_crawl: Any) -> str | None:
    """Стратегия краула run-слоя (``run.yaml:crawl.strategy``) либо ``None``."""
    value = getattr(run_crawl, "strategy", None)
    return None if value is None else str(getattr(value, "value", value))


def _resolve_llm_scenarios(
    *, args: argparse.Namespace, inputs: RunInputs, deploy: DeployConfig
) -> tuple[dict[str, Any], ...]:
    """Детерминированная таблица ``scenario → модель`` (§7.4/I21) из объявленных входов.

    Источник — ``deploy.llm`` (слоты) + ``--llm-slot``/``run.yaml:llm_slot`` +
    ``--llm-model``; env-ключи ``COHOUND_LLM_*`` **не** читаются: это операционная среда
    хоста, а не декларированный вход прогона (иначе два хоста с одним ``run.yaml`` получили
    бы разный ``run_id``).

    Идентичность модели резолвится **той же** функцией, что и runtime-оценщик
    (:func:`core.cli.wiring.resolve_evaluator_model_identity`): аргумент ``--llm-model`` >
    поле слота > дефолт ``decoder``. Двух формул нет — иначе контракт и рантайм разошлись бы
    (именно этот разрыв и был измерен: манифест называл модель, которая не работала).

    Сценарий ``evaluator`` объявляется, только если слот **активен** — задан ``base_url``
    (условие активности зеркалит ``build_evaluator_llm``): пустой слот означает, что
    оценщик не поднимается, и выдумывать маршрут нельзя. Остальные сценарии
    (``search_adapter``/``discovery``/``concept_scout``) в ``deploy.llm`` слотов не имеют
    и здесь не объявляются — пустая таблица честнее выдуманной.

    Endpoint из env (``COHOUND_LLM_BASE_URL``) сюда не входит по построению: он не
    декларирован, поэтому таблица остаётся пустой, а факт «оценщик поднят из env вне
    контракта» сообщает :func:`core.cli.wiring.llm_env_notices`.

    :raises core.cli.errors.CliError: слот активен, но модель не объявлена ни слотом,
        ни ``--llm-model`` (тихий дефолт запрещён — это была бы ложная провенанс-запись).
    """
    llm = getattr(deploy, "llm", None)
    explicit = getattr(args, "llm_slot", None)
    if explicit is None:
        explicit = getattr(getattr(inputs, "run", None), "llm_slot", None)
    slot = resolve_llm_slot(llm, explicit=explicit)
    config = getattr(llm, slot, None) if llm is not None else None
    base_url = getattr(config, "base_url", None)
    if not (base_url and str(base_url).strip()):
        return ()
    model_id, decoder = resolve_evaluator_model_identity(
        slot_config=config,
        slot_name=slot,
        model_id=getattr(args, "llm_model", None),
    )
    # V5/V6: условия генерации — часть того, что описывает ``run_id``. Поле ``decoder``
    # до правки было единственным, а ``seed``/``temperature``/``constrained_decoding``
    # писались NULL, хотя объявлены в схеме и обещаны §7.1/02 C.1: манифест не мог
    # подтвердить даже greedy-режим, не говоря о форме ответа. Значения берутся из тех же
    # констант, что уходят в адаптер (одна формула, не вторая).
    row = {
        "scenario": "evaluator",
        "model_id": model_id,
        "model_version": getattr(config, "model_version", None) or None,
        "endpoint_kind": "api",
        # I18: оценщик без веб-тула — структурная гарантия, а не настройка.
        "web_tool": False,
        "decoder": decoder,
        # Метка и провод: провенанс называет **исполненный** механизм, а не запрошенную
        # метку (V4: в живом прогоне манифест объявлял json_object, шёл json_schema).
        "constrained_decoding": decoder_wire_type(decoder),
        "seed": EVALUATOR_DEFAULT_SEED,
        "temperature": EVALUATOR_DEFAULT_TEMPERATURE,
        # Хост endpoint'а: «куда ушёл корпус» — факт манифеста. Схема/путь/секрет сюда
        # не входят намеренно (V6). Стенд RnD объявлен исключением в спеке §10 и обязан
        # быть виден именно здесь.
        "endpoint_host": endpoint_host(str(base_url)),
        "max_tokens": getattr(config, "max_tokens", None),
    }
    return (row,)


def _crawl_axis_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """Операторские оси краула из CLI — **один** инвентарь для ``run``/``crawl``/``plan``.

    Это **объявление** прогона, а не уточнение исполнения: каждая ось меняет состав
    корпуса, значит ``run_id`` (§13). Читаются ровно флаги :data:`CRAWL_AXIS_DESTS`;
    значения применяются к **run-слою** (:func:`_run_layer_overrides`) — ровно так же, как
    это делает исполнение (``build_phases``/``crawl``: ``model_copy(update=...)`` поверх
    ``run.yaml:crawl``), поэтому дайджест и прогон видят одну и ту же цепочку
    app < run < site: явный per-site профиль остаётся сильнее флага.

    Отдельный шаг вместо подстановки в ``inputs.run.crawl``: синтез секции ``crawl``
    объявил бы за оператора и остальные оси (``strategy`` имеет непустой дефолт), и
    ``--pdf-pipeline structured`` — эффективно no-op — дал бы другой ``run_id``.

    До этой правки в дайджест попадали только ``--document-level``/``--pdf-pipeline``, а
    ``--max-depth``/``--max-documents``/``--exclude-*`` применялись лишь при сборке фаз:
    прогон с другим корпусом жил под тем же ``run_id``. Инвентарь осей теперь назван
    списком (:data:`CRAWL_AXIS_DESTS`), а паритет флагов ``run``/``plan`` проверяется
    тестом — разъехаться молча они больше не могут.

    ``{}`` — оси не заданы (поведение и ``run_id`` неизменны).
    """
    overrides: dict[str, Any] = {}
    for dest in CRAWL_AXIS_DESTS:
        value = getattr(args, dest, None)
        if value is not None:
            # Значения не приводятся к строкам: ``model_copy`` отдаёт их в
            # ``resolve_crawl_limits``, который сам канонизирует enum/list.
            overrides[dest] = value
    return overrides


def _run_layer_overrides(run_crawl: Any, overrides: dict[str, Any]) -> Any:
    """run-слой осей краула с операторскими уточнениями (тот же путь, что у исполнения).

    Одна формула на контракт и на исполнение: ``build_phases``/``crawl`` накладывают
    ``--max-depth``/``--max-documents``/``--exclude-*``/``--document-level``/
    ``--pdf-pipeline`` на ``run.yaml:crawl`` через ``model_copy(update=...)``, и
    :func:`core.pipeline.report.crawl_control_digest` получает **тот же** объект. Если бы
    дайджест накладывал флаг поверх разрешённой цепочки (как это делал параметр
    ``overrides``), флаг объявлялся бы сильнее per-site профиля, тогда как прогон исполнял
    профиль: ``run_id`` описывал бы не тот корпус (живой пример — пакет, где
    ``domain_profiles.yaml`` задаёт ``max_depth``/``max_documents`` на хост).

    ``overrides == {}`` — исходный объект без изменений (поведение и ``run_id``
    байт-в-байт, включая случай ``run.yaml`` без секции ``crawl``: ``run_crawl=None``).

    :raises pydantic.ValidationError: не ожидается — значения уже провалидированы argparse;
        защита от ``model_copy`` не нужна, ключи совпадают с полями ``CrawlRunConfig``.
    """
    if not overrides:
        return run_crawl
    base = run_crawl if run_crawl is not None else CrawlRunConfig()
    return base.model_copy(update=dict(overrides))


def _manual_artifacts_digest(args: argparse.Namespace) -> str:
    """Отпечаток объявленных ручных артефактов (13-manual-artifacts §3.2).

    Читается **тот же** файл пакета тем же загрузчиком, что и фаза корпуса
    (:func:`core.pipeline.manual_artifacts.load_manual_artifacts`): двух формул
    дайджеста нет, иначе ``run_id`` описывал бы не тот корпус, который собран.

    Загрузка — здесь, а не в фазе корпуса, потому что ось объявлена: она обязана быть
    видна ``run_id`` **до** запуска. Объявленный, но нечитаемый/несовпавший по ``sha256``
    артефакт — отказ ``EXIT_USAGE`` (§3.1: сверка хеша — не основание доверия, а проверка).
    Отсутствие файла — не ошибка: стабильный дайджест пустого множества.

    :raises core.cli.errors.CliError: декларация есть, но невалидна.
    """
    package_dir = getattr(args, "package", None)
    try:
        return load_manual_artifacts(package_dir).digest()
    except ValueError as exc:
        raise CliError(
            f"manual_artifacts: {exc}",
            code="manual_artifacts",
            exit_code=EXIT_USAGE,
        ) from exc


def _serp_axis(
    *, args: argparse.Namespace, run: Any, deploy: DeployConfig
) -> tuple[int, str, str]:
    """Эффективные раунды SERP-контура и уровень-источник (I3/§5).

    Приоритет — как у LLM-слота (``CLI > env > run.yaml > deploy.yaml > default``,
    :func:`core.config.deploy.resolve_serp_rounds`). Env ``COHOUND_SERP`` здесь
    **читается**, в отличие от ``COHOUND_LLM_MODEL``: это операционный троттл контура, а
    раунды — условие измерения. Скрыть env-значение из контракта значило бы дать прогон,
    у которого ``run_id`` описывает выключенный контур, а корпус собран с поиском.

    Бюджет объявляется **только** в ``run.yaml: serp.budget``; включение контура без
    бюджета — отказ ``EXIT_USAGE``, а не молчаливый безлимит.

    **Режим поиска объявляется, а не наследуется из окружения.** ``run.yaml: serp.mode``
    задаёт бэкенд провайдера (``sync`` — прямой POST, ``async`` — operation-очередь).
    Если вход его не назвал, объявляется **эффективный дефолт кода** (``SearchMode.ASYNC``),
    а не пустое значение: «чем снимали корпус» — часть идентичности, и ``None`` в контракте
    означал бы неизвестность там, где режим фактически известен. Env ``YANDEX_SEARCH_MODE``
    конвейером не читается (её читает разведка, чей выход — кандидаты, не evidence).

    :raises ValueError: значение уровня не разбирается (не целое/отрицательное).
    :raises core.cli.errors.CliError: раунды включены, а бюджет не объявлен.
    """
    run_serp = getattr(run, "serp", None)
    rounds, source = resolve_serp_rounds(
        getattr(deploy, "search", None),
        cli=getattr(args, "serp_rounds", None),
        env=os.environ.get(ENV_SERP),
        run_rounds=getattr(run_serp, "rounds", None),
    )
    if rounds > 0 and getattr(run_serp, "budget", None) is None:
        raise CliError(
            f"SERP-контур включён ({rounds} раунд(ов), источник {source}), но "
            "run.yaml: serp.budget не объявлен. Бюджет определяет число ушедших "
            "запросов, то есть стоимость и состав корпуса: безлимит по умолчанию "
            "запрещён — объявите serp.budget: <запросов на продукт на раунд>",
            code="serp",
            exit_code=EXIT_USAGE,
        )
    declared_mode = getattr(run_serp, "mode", None)
    mode = str(declared_mode or SearchMode.ASYNC)
    return rounds, source, mode


def resolve_declared_axes(
    *,
    args: argparse.Namespace,
    inputs: RunInputs,
    deploy: DeployConfig | None,
) -> DeclaredExecutionAxes:
    """Разрешает объявленные оси исполнения из входов (без портов и корпуса).

    Чистая функция входа: читает ``args``/``inputs``/``deploy`` и файлы пакета
    (спецификации реестров — только YAML), не собирает адаптеры и не трогает диск
    прогона. Один и тот же вызов — источник и контракта, и манифеста.

    :param args: разобранные аргументы CLI (нужны ``--package``, ``--llm-slot`` и
        ``--llm-model``).
    :param inputs: нормализованные входы прогона (``run``/``package``/профили домена).
    :param deploy: resolved ``DeployConfig``; ``None`` — безопасный дефолт ``local``
        (значения не выдумываются сверх дефолтов конфига).
    :raises ValueError: неизвестное имя LLM-слота (``--llm-slot``/``run.yaml``).
    :raises core.cli.errors.CliError: объявленный реестр пакета отсутствует/невалиден; либо
        LLM-слот активен, а модель оценщика не объявлена ни слотом, ни ``--llm-model``.
    """
    config = deploy if deploy is not None else DeployConfig(profile="local")
    run = getattr(inputs, "run", None)
    run_crawl = getattr(run, "crawl", None)
    axis_overrides = _crawl_axis_overrides(args)
    deploy_fields = manifest_deploy_fields(config)
    serp_rounds, serp_source, serp_mode = _serp_axis(args=args, run=run, deploy=config)
    # §11.1: входы корпуса считаются тем же кодом, что читает исполнение
    # (``_load_corpus_urls``/``_load_render_modes``/``_resolve_passports_dir``), — вторая
    # формула дайджеста разошлась бы с фактическим корпусом. Функции бросают ``CliError``
    # на нечитаемом файле: объявленный вход, который нельзя прочитать, не может быть
    # частью идентичности — это отказ, а не ``None``.
    render_modes = _load_render_modes(getattr(args, "render_modes", None))
    passports_dir = _resolve_passports_dir(args, getattr(args, "paths", None))
    cohort = getattr(inputs, "cohort", None)
    cohort_products = (
        [str(entry.product_id) for entry in cohort.entries]
        if cohort is not None
        else []
    )
    return DeclaredExecutionAxes(
        crawl_strategy=_crawl_strategy(run_crawl),
        crawl_rate_limit=canonical_rate_limit(getattr(run_crawl, "rate_limit", None)),
        # Чистая функция pipeline (app < run < per-site) — вызывается, а не дублируется:
        # вторая формула дайджеста разошлась бы с манифестом. Операторские оси краула
        # накладываются на **run-слой** — тот же путь, что у исполнения, иначе флаг
        # оказался бы сильнее per-site профиля, которого прогон не отменял.
        crawl_config_digest=crawl_control_digest(
            run_crawl=_run_layer_overrides(run_crawl, axis_overrides),
            app=config.crawl,
            profiles=getattr(inputs, "domain_profiles", None),
        ),
        serp_rounds=serp_rounds,
        serp_source=serp_source,
        serp_mode=serp_mode,
        registry_spec_digest=registry_specs_digest(_load_registry_specs(args, inputs)),
        deploy_profile=deploy_fields["deploy_profile"],
        edge_schema_version=deploy_fields["edge_schema_version"],
        # Композит правил нарезки (markdown + plain text), а не версия одного чанкера:
        # с появлением текстового чанкера набор чанков определяется парой правил.
        # Смена строки — версионная граница ``execution_versions`` (§13), меняет ``run_id``;
        # ``METHODOLOGY_VERSION`` не затрагивается.
        chunker_version=composite_chunker_version(),
        # C3/V3: геометрия нарезки и политика повтора. Обе величины — «функция кода и
        # входа», а не наблюдение: читаются из тех же мест, откуда их берёт исполнение.
        chunking_spec=composite_chunking_spec(),
        evaluation_policy=evaluation_policy_spec(),
        render_modes_digest=render_modes_digest(
            {url: mode.value for url, mode in render_modes.items()}
        ),
        passports_digest=passports_digest(
            passport_content_digests(cohort_products, passports_dir)
        ),
        lexical_spec=lexical_schema_spec(),
        llm_scenarios=_resolve_llm_scenarios(args=args, inputs=inputs, deploy=config),
        # P9: устройство — объявленная ось (``app < run`` той же формулой, что у
        # исполнения); входит в стек, значит и в ``run_id``. Значение резолвится, а не
        # читается у устройства: доступность CUDA — отдельный отказ (preflight), не
        # условие идентичности (иначе ``run_id`` стал бы функцией хоста).
        extractor_stack=extractor_stack_version(
            resolve_pdf_device(
                config.crawl,
                getattr(getattr(inputs, "run", None), "crawl", None),
            )
        ),
        # Ось ``embed.device``: та же формула ``app < run``, что у исполнения, и без
        # per-site слоя (устройство — свойство хоста, а не сайта). Значение — строка
        # (``cpu``/``cuda``), чтобы поле контракта/манифеста было JSON-сериализуемо.
        # Доступность CUDA здесь **не** проверяется: это preflight, а не условие
        # идентичности (иначе ``run_id`` стал бы функцией хоста).
        embed_device=resolve_embed_device(
            config.embed,
            getattr(getattr(inputs, "run", None), "embed", None),
        ).value,
        # 13-manual-artifacts §3.2: ручные артефакты объявлены файлом пакета, поэтому
        # отпечаток — функция входа (как seeds_digest), а не наблюдение прогона.
        manual_artifacts_digest=_manual_artifacts_digest(args),
    )
