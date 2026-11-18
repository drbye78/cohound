"""coHound — линтер входов прогона (W5/T5.2, ``03`` §D.3): блокирует прогон.

Проверяет правила §4.2 плюс новые, которые **нельзя** выразить одиночными
pydantic-валидаторами домена (нужно сопоставление нескольких артефактов):

- сумма весов модулей = 100 (defensive: домен уже проверяет при загрузке);
- архетип C при непустом ``baselines`` (manifest и/или ``baselines.csv``) → **блок**
  (§1.5/§4.2);
- реестр ``package.yaml: baselines``/``baseline_participants`` обязан ссылаться на
  ``baseline_id``, объявленные в ``sources/reference_products.yaml`` → **блок**
  (план 27 §3.1); объявленная линия без единой строки ``baselines.csv`` → warning:
  механизм выключен, и это видно до прогона, а не по ``no_policy_row``;
- ``absence_rule ∈ {parameter_reference, distribution_absence}`` без настроенной
  процедуры аттестации полноты источника ``scope_attested_by`` → **блок** (§4.2/I2b);
- пустые ``concept_terms`` у параметра → warning (§4.2; S13);
- ``golden-full`` без страты ожидаемых ❌ → **блок** (§4.2/§11.6); наличие тиров
  ``fast``/``full`` в golden-наборе → warning; **объявленное** отсутствие эталона
  (``state: absent``/``declined`` в ``sources/golden_status.yaml``) — не недочёт:
  сверка не выполняется, и это решение, а не пропуск;
- ``showcase`` с ``rule`` без публичной величины/метрики → **блок** (зеркало домена);
- ``param_id`` не в нижнем регистре → **блок** (зеркало домена);
- ``products`` — проекция когорты (S11) и ``anchor ∈ products`` → **блок** (§4.5);
- авторские поля, не заполненные человеком (пустые ``definition`` роли и
  ``selection_reason`` записи когорты) → **incomplete** (A3): не блокирует создание/
  правку пакета, но запрещает прогон (иначе неполный пакет молча выдал бы
  evaluated-ячейки). Обязательные непустые CSV-поля каталога (``title``/``rule``) уже
  отклоняются загрузчиком (``_req``), поэтому отдельного уровня не требуют;
- продукт когорты без единой golden-строки и без объявленного waiver'а → **incomplete**
  (§11.6): «эталон есть» не значило «эталон покрывает когорту», и продукт без строк
  раньше не проверялся вовсе — неотличимо от «строки потеряли». Лечится строками
  ``golden.csv`` либо объявлением ``exempt_products`` в ``sources/golden_status.yaml``;
- waiver для продукта вне когорты или для продукта, у которого строки есть → **warning**:
  устаревшее объявление называет решение, которого больше нет (сверка при этом идёт).

Каждое правило — отдельный маленький предикат из :data:`RULES`, поэтому оно
тестируется изолированно и адресуется кодом (``code``). Нейтрален к предметной
категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from core.config.run_config import cross_check_products
from core.domain.enums import AbsenceRuleType, Archetype, GoldenTier, ParamType, Status
from core.pipeline.golden_status import STATE_ABSENT, STATE_DECLINED
from core.pipeline.normalize import RunInputs

#: Порядок сгруппирован по назначению (типы → правила → предикаты), а не по алфавиту:
#: комментарий-разделитель — часть документации публичной поверхности модуля.
__all__ = [  # noqa: RUF022
    "LintReport",
    "LintRule",
    "Severity",
    "RULES",
    "lint_run_inputs",
    "assert_lint",
    # правила (по одному предикату; тестируются изолированно)
    "check_weights_sum_to_100",
    "check_archetype_c_baselines",
    "check_absence_attestation",
    "check_empty_concept_terms",
    "check_golden_full_absent_stratum",
    "check_golden_presence",
    "check_golden_coverage",
    "check_golden_exempt_sync",
    "check_showcase_metric",
    "check_slug_lowercase",
    "check_anchor_in_products",
    "check_baseline_reference_product",
    "check_manifest_baselines_declared",
    "check_baseline_policy_present",
    "check_passport_upstreams",
    "check_upstream_presence",
    "check_products_match_cohort",
    "check_reference_product_not_in_cohort",
    "check_reference_versions",
    "check_unfilled_authoring",
]

#: Уровень правила: блокирует прогон, только предупреждает или помечает неполноту (A3).
#: ``incomplete`` — авторские поля ещё не заполнены человеком: не блокирует создание и
#: правку пакета, но запрещает прогон (см. :attr:`LintReport.ready`).
Severity = Literal["blocking", "warning", "incomplete"]

#: absence-правила, требующие действующей процедуры аттестации ``source_scope`` (I2b).
_ATTESTED_ABSENCE_RULES: frozenset[AbsenceRuleType] = frozenset(
    {AbsenceRuleType.PARAMETER_REFERENCE, AbsenceRuleType.DISTRIBUTION_ABSENCE}
)

#: Токены публичной величины/метрики — зеркало ``CatalogRow._check`` (домен §4.2).
#: Дублируется намеренно: линтер обязан re-check'нуть правило даже для объекта,
#: собранного в обход валидатора (``model_construct``/``model_copy``).
_SHOWCASE_TOKENS: tuple[str, ...] = ("величин", "metric", "велич", "%", "числ", "значени")


@dataclass
class LintReport:
    """Отчёт линтера: блокирующие нарушения, предупреждения и неполнота (A3).

    ``ok`` = нет блокирующих. :meth:`raise_if_blocking` превращает блокирующие
    сообщения в :class:`ValueError` (сводки для CLI, ``03`` §E.2). ``incomplete`` —
    авторские поля, ещё не заполненные человеком: отдельный уровень, не блокирующий
    создание/правку пакета, но блокирующий прогон (:attr:`ready`).
    """

    blocking: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Прогон разрешён: нет ни одного блокирующего нарушения."""
        return not self.blocking

    @property
    def complete(self) -> bool:
        """Все обязательные авторские поля заполнены человеком (A3)."""
        return not self.incomplete

    @property
    def ready(self) -> bool:
        """Пакет готов к прогону: нет блокирующих нарушений И нет незаполненных полей."""
        return not self.blocking and not self.incomplete

    def raise_if_blocking(self) -> None:
        """Поднимает :class:`ValueError` со склейкой блокирующих сообщений."""
        if self.blocking:
            raise ValueError("; ".join(self.blocking))


@dataclass(frozen=True)
class LintRule:
    """Именованное правило линтера: код, уровень и проверка ``RunInputs → [msg]``."""

    code: str
    severity: Severity
    check: Callable[[RunInputs], list[str]]


# ---------------------------------------------------------------------------
# Правила (по одному предикату)
# ---------------------------------------------------------------------------


def check_weights_sum_to_100(inputs: RunInputs) -> list[str]:
    """§4.2: сумма весов модулей package.yaml обязана быть 100 (defensive re-check)."""
    total = round(sum(module.weight for module in inputs.package.modules), 6)
    if total != 100:
        return [
            f"§4.2: сумма весов модулей package.yaml должна быть 100, получено {total}"
        ]
    return []


def check_archetype_c_baselines(inputs: RunInputs) -> list[str]:
    """§1.5/§4.2: архетип C не заполняет baselines — непустой набор есть ошибка.

    Проверяются оба носителя: ``package.yaml: baselines`` (реестр в манифесте) и
    ``baselines.csv`` (строки наследования).
    """
    if inputs.package.archetype is not Archetype.C:
        return []
    manifest_ids = set(inputs.package.baselines)
    csv_ids = {row.baseline_id for row in inputs.baselines}
    found = sorted(manifest_ids | csv_ids)
    if not found:
        return []
    return [
        "§4.2/§1.5: архетип C не заполняет baselines "
        f"(найдено: {', '.join(found)})"
    ]


def _declared_reference_ids(inputs: RunInputs) -> set[str] | None:
    """``baseline_id`` объявленных reference-продуктов; ``None`` — декларации нет."""
    declared = getattr(inputs, "reference_products", None)
    if declared is None:
        return None
    return {product.baseline_id for product in declared.products}


def _declared_versions(inputs: RunInputs) -> dict[str, list[str]]:
    """Объявленные версии reference-продуктов: ``baseline_id → [version, …]``.

    Пустой словарь — декларации нет: сравнивать версии не с чем, и правило версии
    не применяется (неизвестность не выдаётся за нарушение, A3).
    """
    declared = getattr(inputs, "reference_products", None)
    if declared is None:
        return {}
    return {
        product.baseline_id: [version.version for version in product.versions]
        for product in declared.products
    }


def check_manifest_baselines_declared(inputs: RunInputs) -> list[str]:
    """План 27 §3.1/§6.4: реестр манифеста обязан ссылаться на объявленные линии.

    ``package.yaml: baselines`` и ``package.yaml: baseline_participants`` — списки
    **``baseline_id``**, а не ``product_id``: оба поля ссылаются на линии, объявленные в
    ``sources/reference_products.yaml``. До этого правила расхождение «реестр объявляет
    линию, которой в декларации нет» проходило ``hound package validate`` с ``ok: yes``:
    поля читал только запрет «C ⇒ пусто», и опечатка в имени линии не отличалась от
    осознанного объявления.

    Правило применяется только при наличии декларации: у пакета без
    ``sources/reference_products.yaml`` сравнивать не с чем, и блокировать его значило бы
    потребовать декларацию, которой профиль ещё не завёл (старые пакеты остаются
    валидными).

    Уровень — **blocking**: имя, которого нет среди ``baseline_id``, — либо опечатка,
    либо ссылка на линию другого профиля; в обоих случаях наследование по нему не
    заработает, а молчаливое «не заработало» неотличимо от «наследовать нечего».
    """
    declared = _declared_reference_ids(inputs)
    if declared is None:
        return []
    registry = [*inputs.package.baselines, *inputs.package.baseline_participants]
    undeclared = sorted({str(bid) for bid in registry if str(bid) not in declared})
    if not undeclared:
        return []
    declared_list = ", ".join(sorted(declared)) if declared else "ни одной линии"
    return [
        "manifest-baseline-undeclared: package.yaml объявляет "
        + ", ".join(repr(bid) for bid in undeclared)
        + " в baselines/baseline_participants, но sources/reference_products.yaml "
        f"объявляет: {declared_list} (§27.3.1)"
    ]


def check_baseline_policy_present(inputs: RunInputs) -> list[str]:
    """План 27 §7.1: объявленная линия без строк ``baselines.csv`` — видимое состояние.

    ``package.yaml: baselines`` объявляет **линии**, а ``baselines.csv`` — **политику
    наследования по параметрам**. Линия в реестре без единой строки политики означает, что
    механизм наследования для неё выключен: наследовать по ней нечему. Это не ошибка
    (пакет может быть не заполнен), поэтому уровень — ``warning``, но состояние обязано
    быть названо: иначе последствие видно только как ``no_policy_row`` в базисе прогона,
    то есть уже после замера.

    Формулировка сообщения намеренно прямая: «строк нет — механизм выключен», а не
    «наследовать нечего» — второе читается как свойство предка, тогда как это состояние
    пакета.
    """
    csv_ids = {str(row.baseline_id) for row in inputs.baselines}
    empty = sorted({str(bid) for bid in inputs.package.baselines if str(bid) not in csv_ids})
    return [
        f"baseline-policy-empty: линия {bid!r} объявлена в package.yaml:baselines, но в "
        "baselines.csv нет ни одной строки с этим baseline_id — строк наследования нет, "
        "механизм выключен, а не «наследовать нечего»"
        for bid in empty
    ]


def check_baseline_reference_product(inputs: RunInputs) -> list[str]:
    """План 27 §6.4/§10: строка ``baselines.csv`` обязана иметь reference-продукт.

    ``direct``-наследование без reference-продукта — это ровно тот путь, которым 181 «✅»
    превращаются в фикцию: цитата берётся не из документации предка, а из паспортной
    визитки продукта («мы на базе X»), и обслуживает все параметры сразу. Поэтому связь
    «строка базлайна → объявленный reference-продукт» проверяется **до** прогона, а не
    выясняется из пустого ``inheritance.jsonl``.

    Пустой ``baselines.csv`` правило не задевает (архетип C и пилот, где механизм ещё не
    заполнен): блокируется только строка, которой некуда наследовать.
    """
    if not inputs.baselines:
        return []
    declared = _declared_reference_ids(inputs)
    known = set() if declared is None else declared
    orphans = [
        (row.baseline_id, row.param_id)
        for row in inputs.baselines
        if row.baseline_id not in known
    ]
    if not orphans:
        return []
    first_bid, first_pid = sorted(orphans)[0]
    suffix = "" if declared is not None else " (sources/reference_products.yaml отсутствует)"
    return [
        f"baseline-without-reference-product: строка ({first_bid}, {first_pid}) "
        f"наследует от baseline_id {first_bid!r}, но reference-продукт для него не "
        f"объявлен (§27.6.4){suffix}; всего строк без предка: {len(orphans)}"
    ]


def check_reference_product_not_in_cohort(inputs: RunInputs) -> list[str]:
    """План 27 §10: reference-продукт не входит в сравнение когорты.

    ``product_id`` reference-продукта делит namespace с продуктами когорты намеренно
    (два смысла одного slug'а в одном пакете — источник молчаливых ошибок), поэтому
    пересечение обязано ловиться линтером, а не глазами: иначе «карта предка» и «карта
    продукта» смешались бы в отчёте, а карта когорты получила бы лишнего участника.

    **Исключение — объявленная точка отсчёта** (``comparison_base: true``). Такая запись
    когорты в сравнение не входит по смыслу флага: продукт измеряется, чтобы дать Δ,
    а не чтобы получить место в матрице. Запрещать её — значит запретить профилю
    измерять собственного предка: одна и та же линия законно объявляется и предком
    (``sources/reference_products.yaml``), и точкой отсчёта (``cohort.yaml``) — две
    декларации одного факта, каждая верная по отдельности. Поэтому id точки отсчёта
    вычитается с обеих сторон — из когорты и из ``run.products``: иначе «разрешено
    в когорте, запрещено в прогоне» дало бы то же падение через другое поле.
    """
    declared = getattr(inputs, "reference_products", None)
    if declared is None or not declared.products:
        return []
    base_ids = {
        str(entry.product_id)
        for entry in inputs.cohort.entries
        if getattr(entry, "comparison_base", False)
    }
    cohort_ids = {str(entry.product_id) for entry in inputs.cohort.entries} - base_ids
    run_ids = {str(pid) for pid in getattr(inputs.run, "products", ())} - base_ids
    collisions = sorted(
        {product.product_id for product in declared.products} & (cohort_ids | run_ids)
    )
    if not collisions:
        return []
    return [
        "reference-product-in-cohort: product_id "
        + ", ".join(repr(pid) for pid in collisions)
        + " объявлен как reference-продукт и одновременно участвует в когорте (§27.10)"
    ]


def check_reference_versions(inputs: RunInputs) -> list[str]:
    """План 27 §7.2: версии — числовые, мажор приводится однозначно.

    Факт ищется **ровно на объявленной мажорной версии**, поэтому нечисловая версия
    (``"abc"``) сделала бы поиск неопределённым: «не нашли» стало бы неотличимо от
    «сравнивать нечем». Это линт-ошибка, а не «пропустим».

    Проверяются обе стороны: объявленные версии reference-продукта и ``min_version``
    строк ``baselines.csv`` (это пол применимости, и он сравнивается численно).
    """
    from core.pipeline.inheritance import major_version

    violations: list[str] = []
    declared = getattr(inputs, "reference_products", None)
    if declared is not None:
        for product in declared.products:
            for version in product.versions:
                if major_version(version.version) is None:
                    violations.append(
                        f"baseline-version-numeric: {product.baseline_id}: версия "
                        f"{version.version!r} не содержит числового мажора (§27.7.2)"
                    )
    for row in inputs.baselines:
        if row.min_version and major_version(row.min_version) is None:
            violations.append(
                f"baseline-version-numeric: ({row.baseline_id}, {row.param_id}): "
                f"min_version {row.min_version!r} не содержит числового мажора (§27.7.2)"
            )
    return sorted(violations)


def check_passport_upstreams(inputs: RunInputs) -> list[str]:
    """План 27 §3.2/§10: предки паспорта объявлены, их число согласовано с архетипом.

    Проверяется только когорта: каталог паспортов содержит продукты всех пакетов, и
    чужой паспорт не должен валить этот.

    * ``baseline_id`` из ``passport.upstreams`` обязан быть объявлен в
      ``reference_products.yaml`` — иначе продукт объявляет линию, о которой профиль
      ничего не знает, и наследовать по ней нечему;
    * мажор версии предка обязан быть **объявлен** у той же линии: ``inheritance`` ищет
      факт по буквальному мажору (``inheritance.py``), поэтому предок «18.4.1» при
      объявленной «18» молча даёт ``baseline_fact_missing`` — «наследования нет» без
      единого слова о том, что версия не объявлена. Правило применяется, только когда
      декларация есть и версия разбирается: сравнивать не с чем — не отказ;
    * архетип A — линия одна, поэтому **больше одного** предка у продукта быть не может
      (это была бы заявка формы B в A-пакете), и линия обязана быть объявлена **хоть
      кем-то**: A-пакет без единого предка не использует наследование вовсе;
    * архетип C — предков нет (эталона нет, §1.5);
    * архетип B **не требует** ни одной объявленной линии, и это отличие от буквы
      §27.3.2 плана — осознанное. Регламент §1.5 описывает B как «несколько
      OSS-альтернатив + коммерческие на их основе», где базлайн-участник включается
      **«если он существует»**: кросс-линейное сравнение без общего предка — штатная
      форма B, а не противоречие. Блокировать её значило бы требовать от автора выдумать
      линию (A3), и цена такого правила измерена: канонический валидный пакет профиля
      (`tests/config/fixtures/package.yaml`: B, `baselines: [baseline_a]`, продукты без
      предков) перестал проходить линтер. Поэтому в B проверяется **состояние продукта**
      (предупреждение :func:`check_upstream_presence`), а не метод пакета.

    **Продукт без предка не блокирует** (отличие от буквы §3.2 плана, и оно осознанное):
    «вендор не называет версию» — честное состояние (объявленный продукт может не
    указывать предка: версия ядра ≠ версия линии, на которой он стоит), и требовать предка
    значило бы заставить автора его выдумать.
    Такие продукты публикуются **предупреждением**, а не блоком; блок — только
    противоречие объявленному (два предка в A, предок в C, необъявленный baseline_id).

    Паспорта приходят из раскладки, а не из пакета, поэтому проверка работает, когда
    composition root их передал (``inputs.passports``); неизвестность не выдаётся за
    «нарушений нет» — она просто не проверяется здесь. По той же причине пакетные
    правила (A/B) судят только о **наблюдённых** паспортах когорты: каталог может быть
    чужим и не содержать ни одного её продукта, и «предков нет» тогда — отсутствие
    данных, а не утверждение о методе профиля.
    """
    passports = getattr(inputs, "passports", None) or {}
    if not passports:
        return []
    from core.pipeline.inheritance import major_version

    cohort_ids = [str(entry.product_id) for entry in inputs.cohort.entries]
    # Правило судит только о **наблюдённых** паспортах когорты. Каталог паспортов приходит
    # из раскладки и может быть чужим (другой пакет) или не содержать продуктов когорты;
    # тогда «предков нет» — не наблюдение, а отсутствие данных, и блокировать им нельзя.
    observed_ids = [pid for pid in sorted(cohort_ids) if passports.get(pid) is not None]
    declared = _declared_reference_ids(inputs)
    declared_versions = _declared_versions(inputs)
    archetype = inputs.package.archetype
    violations: list[str] = []
    declared_lines = 0
    for product_id in observed_ids:
        passport = passports[product_id]
        upstreams = list(getattr(passport, "upstreams", ()) or ())
        ids = [str(upstream.baseline_id) for upstream in upstreams]
        if ids:
            declared_lines += 1
        undeclared = sorted({bid for bid in ids if bid not in (declared or set())})
        if undeclared and declared is not None:
            violations.append(
                f"passport-upstream-undeclared: {product_id}: "
                + ", ".join(repr(bid) for bid in undeclared)
                + " не объявлен в sources/reference_products.yaml (§27.10)"
            )
        # Версия предка: мажор ищется ровно на объявленной версии линии (§27.7.2).
        for upstream in upstreams:
            versions = declared_versions.get(str(upstream.baseline_id))
            if versions is None:
                continue  # линия не объявлена — об этом уже сказано выше
            major = major_version(str(upstream.version))
            if major is None:
                continue  # версия не разбирается — сравнивать нечем
            if major in {m for m in map(major_version, versions) if m is not None}:
                continue
            violations.append(
                f"upstream-version-undeclared: {product_id}: предок "
                f"{upstream.baseline_id!r} объявлен на версии {upstream.version!r} "
                f"(мажор {major}), а в sources/reference_products.yaml у этой линии "
                "объявлены версии: " + ", ".join(versions) + " (§27.7.2)"
            )
        if archetype is Archetype.A and len(upstreams) > 1:
            violations.append(
                f"upstreams-by-archetype: {product_id}: архетип A — одна линия, "
                f"объявлено предков: {len(upstreams)} (§27.3.2)"
            )
        if archetype is Archetype.C and upstreams:
            violations.append(
                f"upstreams-by-archetype: {product_id}: архетип C не объявляет upstream "
                f"(объявлено {len(upstreams)}) (§1.5/§27.3.2)"
            )
    if archetype is Archetype.A and observed_ids and declared_lines == 0:
        violations.append(
            "upstreams-by-archetype: архетип A не объявляет ни одного предка ни у одного "
            "продукта когорты — пакет описывает линию, которой нет (§27.3.2)"
        )
    return violations


def check_upstream_presence(inputs: RunInputs) -> list[str]:
    """План 27 §3.2: продукт пакета A/B без объявленного предка — видимое состояние.

    Не блокирует: «вендор не называет версию» — честное состояние (продукт может не
    указывать предка: версия ядра ≠ версия линии, на которой он стоит), и требовать предка
    значило бы заставить автора его выдумать (A3, precision-first). Предупреждение нужно затем,
    чтобы «наследование для продукта недоступно» было названо, а не выяснялось из
    отсутствия ячеек.

    Архетип B проверяется по той же причине, но с другой стороны: в B предков может быть
    несколько (форк и линия, на которой продукт стоит), и продукт без единого предка
    лишён ускорения — однако его отсутствие всё равно остаётся фактом о вендоре, а не о
    методе профиля. Заявку метода проверяет :func:`check_passport_upstreams` на уровне
    пакета; здесь — состояние конкретного продукта.
    """
    passports = getattr(inputs, "passports", None) or {}
    archetype = inputs.package.archetype
    if not passports or archetype not in (Archetype.A, Archetype.B):
        return []
    messages: list[str] = []
    for product_id in sorted(str(entry.product_id) for entry in inputs.cohort.entries):
        passport = passports.get(product_id)
        if passport is None:
            continue
        if not list(getattr(passport, "upstreams", ()) or ()):
            messages.append(
                f"upstreams-by-archetype: {product_id}: архетип {archetype.value}, "
                "но предок не объявлен — наследование для продукта недоступно (§27.3.2)"
            )
    return messages


def check_absence_attestation(inputs: RunInputs) -> list[str]:
    """§4.2/I2b: ``parameter_reference``/``distribution_absence`` требуют процедуры.

    Процедура аттестации полноты источника — пакетная настройка
    ``RunInputs.scope_attestation_procedure`` (загружается из опционального
    ``scope_attestation.yaml`` в каталоге пакета, см. ``normalize``). Если она не
    задана, а каталог содержит такие absence-правила — прогон блокируется: без
    механизма ``scope_attested_by`` ❌ по этим правилам недоказуем (I2b).
    """
    if (inputs.scope_attestation_procedure or "").strip():
        return []
    offenders = sorted(
        row.param_id
        for row in inputs.catalog
        if row.absence_rule in _ATTESTED_ABSENCE_RULES
    )
    if not offenders:
        return []
    return [
        "§4.2/I2b: absence_rule parameter_reference/distribution_absence требует "
        "действующей процедуры аттестации source_scope "
        f"(scope_attestation_procedure не настроена); затронуты: {', '.join(offenders)}"
    ]


def check_empty_concept_terms(inputs: RunInputs) -> list[str]:
    """§4.2/S13: пустые ``concept_terms`` → warning (немой поиск по синонимам)."""
    missing = sorted(
        row.param_id for row in inputs.catalog if not _has_terms(row.concept_terms)
    )
    if not missing:
        return []
    return [f"§4.2: пустые concept_terms у параметров: {', '.join(missing)}"]


def check_golden_full_absent_stratum(inputs: RunInputs) -> list[str]:
    """§4.2/§11.6: golden-full обязан включать страту ожидаемых ❌.

    Проверяется состав **тира full**: если full-строки есть, среди них обязана
    быть хотя бы одна с ``expected_status = ❌`` (через ``registry_absence`` /
    ``distribution_absence``). Иначе критический класс false-❌ нечем тренировать.
    """
    full_rows = [row for row in inputs.golden if row.tier is GoldenTier.FULL]
    if not full_rows:
        return []
    if any(row.expected_status is Status.ABSENT for row in full_rows):
        return []
    return [
        "§4.2/§11.6: golden-full обязан включать страту ожидаемых ❌ "
        "(registry_absence/distribution_absence)"
    ]


def check_golden_presence(inputs: RunInputs) -> list[str]:
    """§11.6: presence-предупреждения golden-набора (тиры fast/full).

    Учитывает **объявленное** состояние эталона (:mod:`core.pipeline.golden_status`):

    * ``present`` — прежнее поведение (пустой набор и недостающие тиры — warning);
    * ``absent``/``declined`` **с объявлением** — не warning: это законный режим
      («сверять не с чем» / «эталон объявлен непригодным»), а не недочёт пакета;
    * ``absent`` **без объявления** (и ``golden_status is None``, т.е. входы собраны в
      обход :func:`~core.pipeline.normalize.load_inputs`) — warning остаётся: молчаливое
      отсутствие обязано быть видимым.
    """
    status = inputs.golden_status
    if status is not None:
        if status.state == STATE_DECLINED:
            return []
        if status.state == STATE_ABSENT and status.declared:
            return []
    if not inputs.golden:
        if status is not None and not status.file_present:
            return [
                "§11.6: golden.csv отсутствует и эталон не объявлен — golden-сеть "
                "отсутствует (объявите state: absent в sources/golden_status.yaml, "
                "если сверка намеренно не выполняется)"
            ]
        return ["§11.6: golden.csv пуст — golden-сеть отсутствует"]
    messages: list[str] = []
    if not any(row.tier is GoldenTier.FAST for row in inputs.golden):
        messages.append("§11.6: отсутствует тир golden-fast")
    if not any(row.tier is GoldenTier.FULL for row in inputs.golden):
        messages.append("§11.6: отсутствует тир golden-full")
    return messages


def check_golden_coverage(inputs: RunInputs) -> list[str]:
    """§11.6: у каждого продукта когорты есть golden-строки **или** объявленный waiver.

    Правило исполняет требование, которое до сих пор было надписью: «эталон есть» ничего
    не говорило о том, покрывает ли он когорту. Продукт без единой golden-строки раньше
    просто не проверялся — и это было неотличимо от «строки потеряли».

    Применимость: только когда сверка реально идёт, то есть golden-строки есть
    (``state: present`` либо выведенное ``present``). При ``absent``/``declined``
    строк нет по объявлению, сверки нет — и требовать покрытие не по чему.

    Уровень — **``warning``, не блокирующий** (решение владельца 29.10.2026). Причина —
    статус самого инструмента: ``golden.csv`` не входит в ``execution_contract_digest``,
    не влияет на вердикты и на карту (§11.3), то есть не является условием измерения.
    Гейт, который останавливает прогон, объявлял бы проверочный инструмент условием
    замера — и делал бы невозможным первый замер продукта: эталон аннотируют **по**
    результату первого прогона, ответного ключа до него не существует (так и появился
    пятый продукт когорты — прогон, затем аннотация). Поэтому неполнота здесь
    **видима**, но не запрещает измерять: предупреждение печатают ``plan``/``run``
    (в т.ч. в ``--json``), а объявленный waiver дополнительно виден в отчёте
    (``golden_exempt_product``).

    Рассинхронизация объявления с данными (waiver для продукта вне когорты или для
    продукта, у которого строки есть) — отдельное правило
    :func:`check_golden_exempt_sync`: это уже не неполнота, а устаревшее объявление.
    """
    sets = _golden_coverage_sets(inputs)
    if sets is None:
        return []
    products, covered, exempt = sets
    return [
        f"§11.6: у продукта '{product_id}' когорты нет ни одной строки golden.csv; "
        "объявите эталон для него либо waiver в sources/golden_status.yaml "
        "(exempt_products: product_id, reason, declared_by)"
        for product_id in products
        if product_id not in covered and product_id not in exempt
    ]


def check_golden_exempt_sync(inputs: RunInputs) -> list[str]:
    """§11.6: waiver'ы по продуктам обязаны быть согласованы с когортой и golden.csv.

    Waiver — **объявление**, и устаревшее объявление хуже отсутствующего: оно называет
    решение, которого больше нет. Два случая, оба ``warning`` (не блокируют прогон —
    сверка идёт, просто объявление врёт):

    * waiver для продукта, которого нет в когорте (продукт убрали, объявление осталось);
    * waiver для продукта, у которого golden-строки **есть** (эталон появился, waiver
      не сняли).

    Правило не срабатывает без объявления (``golden_status is None`` — входы собраны в
    обход :func:`~core.pipeline.normalize.load_inputs`).
    """
    status = inputs.golden_status
    if status is None or not status.exempt_products:
        return []
    products = {entry.product_id for entry in inputs.cohort.entries}
    covered = {row.product_id for row in inputs.golden}
    messages = [
        f"§11.6: waiver продукта '{product_id}' объявлен в "
        "sources/golden_status.yaml (exempt_products), но продукта нет в когорте — "
        "объявление рассинхронизировано с составом когорты"
        for product_id in sorted(status.exempt_ids - products)
    ]
    messages.extend(
        f"§11.6: waiver продукта '{product_id}' объявлен, но golden-строки для него "
        "есть — уберите waiver либо строки: объявление рассинхронизировано с golden.csv"
        for product_id in sorted(status.exempt_ids & covered)
    )
    return messages


def _golden_coverage_sets(
    inputs: RunInputs,
) -> tuple[list[str], set[str], set[str]] | None:
    """Материал правил покрытия: ``(продукты когорты, покрытые, waiver'ы)`` или ``None``.

    ``None`` — сверка не идёт (строк эталона нет: ``absent``/``declined`` либо набор
    пуст): тогда требовать покрытие не по чему.
    """
    status = inputs.golden_status
    if status is not None and not status.comparison_available:
        return None
    if not inputs.golden:
        return None
    products = sorted({entry.product_id for entry in inputs.cohort.entries})
    covered = {row.product_id for row in inputs.golden}
    exempt = set(status.exempt_ids) if status is not None else set()
    return products, covered, exempt


def check_showcase_metric(inputs: RunInputs) -> list[str]:
    """§4.2: showcase → ``rule`` обязан упоминать публичную величину/метрику."""
    offenders = sorted(
        row.param_id
        for row in inputs.catalog
        if row.param_type is ParamType.SHOWCASE
        and not any(token in row.rule.lower() for token in _SHOWCASE_TOKENS)
    )
    if not offenders:
        return []
    return [
        "§4.2: param_type=showcase требует публичную величину/метрику в rule; "
        f"нарушают: {', '.join(offenders)}"
    ]


def check_slug_lowercase(inputs: RunInputs) -> list[str]:
    """§4.2: ``param_id`` — slug в нижнем регистре (зеркало ``CatalogRow``)."""
    offenders = sorted(
        row.param_id
        for row in inputs.catalog
        if not row.param_id or row.param_id != row.param_id.lower()
    )
    if not offenders:
        return []
    return [f"§4.2: param_id — slug в нижнем регистре; нарушают: {', '.join(offenders)}"]


def check_anchor_in_products(inputs: RunInputs) -> list[str]:
    """§4.5: ``anchor`` обязан входить в ``products`` (defensive re-check)."""
    if inputs.run.anchor not in inputs.run.products:
        return [
            f"§4.5: anchor '{inputs.run.anchor}' обязан входить в products"
        ]
    return []


def check_products_match_cohort(inputs: RunInputs) -> list[str]:
    """S11 (§4.5): ``products`` — проекция состава когорты (один источник)."""
    return sorted(cross_check_products(inputs.run, inputs.cohort))


def check_unfilled_authoring(inputs: RunInputs) -> list[str]:
    """A3: обязательные авторские поля, ещё не заполненные человеком.

    Незаполненность выражается явно (``None`` у опциональных полей роли/когорты) и не
    подменяется правдоподобным текстом-заглушкой.
    Возвращает детерминированный список сообщений ``A3:`` — прогон такого пакета
    запрещён (:attr:`LintReport.ready`), но создание/правка разрешены.

    Покрытые поля: ``RoleDef.definition`` (пусто/``None``) и
    ``CohortEntry.selection_reason``. Обязательные непустые CSV-поля каталога
    (``title``/``rule``) отклоняются загрузчиком (``run_config._req``) до линтера.
    """
    messages: list[str] = []
    for role in inputs.package.roles:
        if not (role.definition or "").strip():
            messages.append(
                f"A3: роль '{role.role_id}' не заполнена автором (definition)"
            )
    for entry in inputs.cohort.entries:
        if not (entry.selection_reason or "").strip():
            messages.append(
                f"A3: продукт '{entry.product_id}' когорты: обоснование выбора не "
                "заполнено автором (selection_reason)"
            )
    return sorted(messages)


#: Все правила линтера в детерминированном порядке.
RULES: tuple[LintRule, ...] = (
    LintRule("weights-sum-100", "blocking", check_weights_sum_to_100),
    LintRule("archetype-c-baselines", "blocking", check_archetype_c_baselines),
    # План 27 §10: связь «строка базлайна → объявленный reference-продукт», namespace
    # когорты и версии. Все три блокируют: без них наследование опирается на паспортную
    # визитку, а карта предка смешалась бы с картой продукта.
    LintRule(
        "baseline-without-reference-product", "blocking", check_baseline_reference_product
    ),
    LintRule(
        "reference-product-in-cohort", "blocking", check_reference_product_not_in_cohort
    ),
    LintRule("baseline-version-numeric", "blocking", check_reference_versions),
    # Реестр манифеста ссылается на объявленные линии (§27.3.1) — блокирует: имя, которого
    # нет среди ``baseline_id``, не даст наследования и неотличимо от опечатки.
    LintRule(
        "manifest-baseline-undeclared", "blocking", check_manifest_baselines_declared
    ),
    # Объявленная линия без строк ``baselines.csv`` — механизм выключен (видимо, не блок).
    LintRule("baseline-policy-empty", "warning", check_baseline_policy_present),
    LintRule("passport-upstreams", "blocking", check_passport_upstreams),
    LintRule("upstream-presence", "warning", check_upstream_presence),
    LintRule("absence-attestation", "blocking", check_absence_attestation),
    LintRule("golden-full-absent-stratum", "blocking", check_golden_full_absent_stratum),
    LintRule("showcase-metric", "blocking", check_showcase_metric),
    LintRule("slug-lowercase", "blocking", check_slug_lowercase),
    LintRule("anchor-in-products", "blocking", check_anchor_in_products),
    LintRule("products-match-cohort", "blocking", check_products_match_cohort),
    LintRule("unfilled-authoring", "incomplete", check_unfilled_authoring),
    LintRule("golden-coverage", "warning", check_golden_coverage),
    LintRule("empty-concept-terms", "warning", check_empty_concept_terms),
    LintRule("golden-presence", "warning", check_golden_presence),
    LintRule("golden-exempt-sync", "warning", check_golden_exempt_sync),
)


# ---------------------------------------------------------------------------
# Публичный вход
# ---------------------------------------------------------------------------


def _has_terms(terms: dict[str, list[str]] | None) -> bool:
    """Есть ли хотя бы один непустой термин в ``concept_terms``."""
    if not terms:
        return False
    return any(entry for entry in terms.values())


def lint_run_inputs(inputs: RunInputs) -> LintReport:
    """Прогоняет все :data:`RULES` и собирает :class:`LintReport`.

    Правила не зависят друг от друга; порядок сообщений детерминирован (порядок
    ``RULES`` и сортировка внутри предикатов). Не блокирует сама по себе — решение
    принимает вызывающий (``raise_if_blocking`` в CLI-композиции).
    """
    report = LintReport()
    for rule in RULES:
        messages = rule.check(inputs)
        if rule.severity == "blocking":
            report.blocking.extend(messages)
        elif rule.severity == "incomplete":
            report.incomplete.extend(messages)
        else:
            report.warnings.extend(messages)
    return report


def assert_lint(inputs: RunInputs) -> LintReport:
    """Линтует и поднимает :class:`ValueError`, если есть блокирующие нарушения."""
    report = lint_run_inputs(inputs)
    report.raise_if_blocking()
    return report
