"""``golden_status.yaml`` — **объявленное** состояние эталонного набора пакета.

**Зачем.** ``golden.csv`` — проверочный инструмент (§11.3), а не условие измерения:
он не входит в ``execution_contract_digest``, не влияет на вердикты и карту, и его
смена не меняет ``run_id``. Значит «сверять не с чем» — законный режим (первая версия
таблицы сравнения, новый продукт в когорте), но сегодня он выражался только
отсутствием файла, то есть **молчанием**: оператор не отличал «эталона ещё нет» от
«эталон потеряли». Ещё хуже случай «файл есть, но он непригоден» (устарел, размечен не
на те продукты, содержит намеренно неверные ячейки): единственным способом объявить это
было удалить данные.

**Три состояния**, все видимые, ни одно не является молчанием:

* ``absent``   — файла ``golden.csv`` нет: прогон идёт, сверка не выполняется;
* ``present``  — файл есть и используется (прежнее поведение);
* ``declined`` — файл есть, но **объявлен непригодным**: сверка не выполняется, а файл
  остаётся на месте — иначе оператор потерял бы данные, которые позже переделает.

**Отдельный продукт без эталона** (``exempt_products``). ``golden.csv`` — общая таблица
когорты, и «у этого продукта строк нет» было тем же молчанием, что и отсутствие файла:
нельзя отличить «продукт новый, эталон для него ещё не размечали» от «строки потеряли».
Поле ``exempt_products`` объявляет waiver **по продукту**: у него нет ни одной
golden-строки, и это решение с причиной и автором, а не пропуск. Оно осмысленно при
любом ``state`` (продукт может быть исключён и при ``present``), а линтер
(``check_golden_coverage``) и отчёт (аномалия ``golden_exempt_product``) делают его
видимым вместо того, чтобы считать отсутствие строк недочётом.

В ``cohort_digest`` waiver **не входит** (``core.pipeline.digests``): это объявление об
инструменте проверки, а не о составе когорты; иначе «эталона для продукта нет» меняло бы
``run_id`` и рвало межцикловый diff.

**Носитель объявления — файл пакета**, ``packages/{case_id}/sources/golden_status.yaml``,
а не CLI-флаг: ось, заданная флагом, теряется в следующем вызове, и ``run_id``/артефакты
перестают описывать, чем измеряли (``docs/architecture/12-cli-architecture-review.md``
§4.6). Пакет — носитель авторских входов; это авторский вход. Форма
(:data:`GOLDEN_STATUS_SCHEMA`)::

    schema: golden_status/1
    state: declined            # absent | present | declined
    reason: "почему эталон не используется"   # обязателен при declined
    declined_by: analyst                       # обязателен при declined
    declined_at: '2026-10-27'                  # опционален
    exempt_products:                           # опционален, при любом state
      - product_id: product_slug
        reason: "почему эталона для продукта нет"
        declared_by: analyst
        declared_at: '2026-10-27'              # опционален

**Разбор** (:func:`read_golden_status`):

* файла нет (или он содержит только комментарии) → состояние **выводится** из наличия
  ``golden.csv`` (``present`` либо ``absent``), ``declared=False``;
* ``state: present`` без ``golden.csv`` → ошибка: объявлено то, чего нет;
* ``state: absent`` при существующем ``golden.csv`` → ошибка: неоднозначность, файл надо
  либо убрать, либо объявить ``declined``;
* ``state: declined`` без ``reason`` или без ``declined_by`` → ошибка: причина
  обязательна, иначе это тихий waiver, а он запрещён;
* запись ``exempt_products`` без ``product_id``/``reason``/``declared_by`` → ошибка: у
  waiver по продукту та же причина запрета на молчание;
* дубликат ``product_id`` в ``exempt_products`` → ошибка: два разных решения об одном
  продукте неразрешимы;
* неизвестное состояние/ключ (в том числе ключ внутри записи ``exempt_products``),
  чужой ``schema`` → ошибка.

Все ошибки — :class:`ValueError` с ведущим кодом ``golden_status.yaml:``: CLI печатает
их как видимую ошибку пакета (``extract_code`` берёт ведущий токен), а не как ``error: E``.

Модуль — **лист** зависимостей (stdlib + ``yaml``): его импортируют ``normalize``,
``lint``, ``plan``, ``package_io`` и CLI ``golden``. Нейтрален (§1.3): ни названий
продуктов, ни предметных терминов.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import yaml

__all__ = [
    "GOLDEN_FILE",
    "GOLDEN_STATES",
    "GOLDEN_STATUS_DIR",
    "GOLDEN_STATUS_FILE",
    "GOLDEN_STATUS_SCHEMA",
    "STATE_ABSENT",
    "STATE_DECLINED",
    "STATE_PRESENT",
    "GoldenProductExemption",
    "GoldenState",
    "GoldenStatus",
    "golden_status_path",
    "read_golden_status",
]

#: Версия схемы файла (меняется только при несовместимой смене формы).
GOLDEN_STATUS_SCHEMA = "golden_status/1"

#: Подкаталог пакета с авторскими входами (03 §A.1): объявление эталона живёт там.
GOLDEN_STATUS_DIR = "sources"

#: Имя файла объявления внутри :data:`GOLDEN_STATUS_DIR`.
GOLDEN_STATUS_FILE = "golden_status.yaml"

#: Имя эталонного набора в корне пакета (§4.2/§11.6).
GOLDEN_FILE = "golden.csv"

#: Допустимые состояния эталона. ``Literal``, а не ``StrEnum``: это **не** вердикт и не
#: измеряемая ячейка, поэтому словарю статусов (``docs/status-dictionary.yaml``) он не
#: принадлежит — как и ``Severity`` линтера.
GoldenState = Literal["absent", "present", "declined"]

#: Канонический порядок состояний (для сообщений/перечислений).
GOLDEN_STATES: tuple[GoldenState, ...] = ("absent", "present", "declined")

STATE_ABSENT: GoldenState = "absent"
STATE_PRESENT: GoldenState = "present"
STATE_DECLINED: GoldenState = "declined"

#: Единственные допустимые ключи объявления (``extra="forbid"``-дисциплина домена).
_ALLOWED_KEYS: frozenset[str] = frozenset(
    {"schema", "state", "reason", "declined_by", "declined_at", "exempt_products"}
)

#: Ключи, осмысленные только при ``state: declined``.
_DECLINE_KEYS: tuple[str, ...] = ("reason", "declined_by", "declined_at")

#: Ключ объявления waiver'ов по продуктам (при любом ``state``).
_EXEMPT_KEY = "exempt_products"

#: Единственные допустимые ключи одной записи ``exempt_products``.
_EXEMPT_KEYS: frozenset[str] = frozenset(
    {"product_id", "reason", "declared_by", "declared_at"}
)

#: Обязательные поля записи waiver'а: без них объявление снова было бы молчанием.
_EXEMPT_REQUIRED: tuple[str, ...] = ("product_id", "reason", "declared_by")


@dataclass(frozen=True)
class GoldenProductExemption:
    """Объявленный waiver по одному продукту: эталона для него нет — и это решение.

    :param product_id: продукт когорты, для которого golden-строк нет.
    :param reason: почему эталона для продукта нет (обязателен: иначе тихий waiver).
    :param declared_by: кто объявил (обязателен).
    :param declared_at: дата объявления (опциональна), ISO-8601 ``YYYY-MM-DD``.
    """

    product_id: str
    reason: str
    declared_by: str
    declared_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление одной записи."""
        return {
            "product_id": self.product_id,
            "reason": self.reason,
            "declared_by": self.declared_by,
            "declared_at": self.declared_at,
        }


@dataclass(frozen=True)
class GoldenStatus:
    """Состояние эталонного набора пакета, как оно объявлено (или выведено) на диске.

    :param state: ``absent`` | ``present`` | ``declined``.
    :param declared: объявлено ли состояние файлом ``sources/golden_status.yaml``.
        ``False`` — состояние выведено из наличия ``golden.csv`` (объявления нет);
        различие важно: объявленное отсутствие — **решение**, необъявленное — недочёт,
        и линтер сообщает о них по-разному.
    :param file_present: существует ли ``golden.csv`` (файл мог остаться при
        ``declined`` — так и должно быть).
    :param reason: причина непригодности (обязательна при ``declined``).
    :param declined_by: кто объявил непригодность (обязателен при ``declined``).
    :param declined_at: дата объявления (опциональна), ISO-8601 ``YYYY-MM-DD``.
    :param exempt_products: объявленные waiver'ы по продуктам (``exempt_products``);
        пустой кортеж — поле не задано. Порядок — как в файле (детерминизм разбора);
        потребители сортируют сами.
    """

    state: GoldenState
    declared: bool
    file_present: bool
    reason: str | None = None
    declined_by: str | None = None
    declined_at: str | None = None
    exempt_products: tuple[GoldenProductExemption, ...] = ()

    @property
    def comparison_available(self) -> bool:
        """Можно ли сверять прогон с эталоном (только ``present``)."""
        return self.state == STATE_PRESENT

    @property
    def exempt_ids(self) -> frozenset[str]:
        """Продукты с объявленным waiver'ом — множество ``product_id``."""
        return frozenset(entry.product_id for entry in self.exempt_products)

    def describe(self) -> str:
        """Человекочитаемое состояние — для ``plan``/линтера/``hound golden``."""
        if self.state == STATE_PRESENT:
            return "эталон объявлен и используется (golden.csv)"
        if self.state == STATE_DECLINED:
            details = [f"reason={self.reason}"]
            if self.declined_by:
                details.append(f"declined_by={self.declined_by}")
            if self.declined_at:
                details.append(f"declined_at={self.declined_at}")
            return (
                "эталон объявлен непригодным, сверка не выполняется ("
                + ", ".join(details)
                + "); файл golden.csv не используется и остаётся на месте"
            )
        if self.declared:
            return "эталон не объявлен (state: absent), сверка не выполняется"
        return (
            "эталон отсутствует: файла golden.csv нет и объявления нет, "
            "сверка не выполняется"
        )

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (для ``plan --json``/``hound golden --json``)."""
        return {
            "state": self.state,
            "declared": self.declared,
            "file_present": self.file_present,
            "reason": self.reason,
            "declined_by": self.declined_by,
            "declined_at": self.declined_at,
            "exempt_products": [entry.as_dict() for entry in self.exempt_products],
        }


def golden_status_path(package_dir: str | Path) -> Path:
    """Путь объявления: ``{package}/sources/golden_status.yaml``."""
    return Path(package_dir) / GOLDEN_STATUS_DIR / GOLDEN_STATUS_FILE


def _error(path: Path, message: str) -> ValueError:
    """Ошибка пакета с ведущим кодом ``golden_status.yaml:`` (видна в CLI)."""
    return ValueError(f"{GOLDEN_STATUS_FILE}: {message} (файл: {path})")


def _text(value: Any) -> str | None:
    """Непустой текст объявления; ``None`` для пустого/отсутствующего значения."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _declined_at(value: Any, path: Path, *, key: str = "declined_at") -> str | None:
    """Дата объявления в ISO-8601; ``date`` (YAML-скаляр) и ``str`` равнозначны."""
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date().isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    text = _text(value)
    if text is None:
        return None
    try:
        return _dt.date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise _error(
            path,
            f"{key}={text!r} не является датой ISO-8601 (YYYY-MM-DD)",
        ) from exc


def _exempt_products(value: Any, path: Path) -> tuple[GoldenProductExemption, ...]:
    """Разбирает ``exempt_products`` — waiver'ы по продуктам (при любом ``state``).

    Отсутствие поля — пустой кортеж (обратная совместимость файлов без него). Каждая
    запись строгая: неизвестный ключ, не-словарь, пустые ``product_id``/``reason``/
    ``declared_by`` и дубликат ``product_id`` — видимая ошибка пакета. Причина та же, что
    у ``declined``: waiver без причины и автора был бы молчанием, а молчание здесь
    запрещено — иначе «строки потеряли» неотличимо от «эталона для продукта нет».

    :raises ValueError: ``exempt_products`` не список; элемент не словарь; неизвестный
        ключ записи; пустое обязательное поле; дубликат ``product_id``;
        ``declared_at`` не ISO-дата.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        raise _error(
            path,
            f"{_EXEMPT_KEY} обязан быть списком записей, получено "
            f"{type(value).__name__}",
        )
    entries: list[GoldenProductExemption] = []
    seen: set[str] = set()
    for index, raw in enumerate(value, start=1):
        where = f"{_EXEMPT_KEY}[{index}]"
        if not isinstance(raw, Mapping):
            raise _error(
                path,
                f"{where} обязан быть словарём, получено {type(raw).__name__}",
            )
        unknown = sorted(str(key) for key in raw if key not in _EXEMPT_KEYS)
        if unknown:
            raise _error(
                path,
                f"{where}: неизвестные ключи {unknown}; допустимы "
                f"{sorted(_EXEMPT_KEYS)}",
            )
        fields = {name: _text(raw.get(name)) for name in _EXEMPT_REQUIRED}
        missing = [name for name, text in fields.items() if text is None]
        if missing:
            raise _error(
                path,
                f"{where} требует {missing}: waiver без продукта, причины и автора — "
                "тихий waiver, а он запрещён",
            )
        # ``cast`` — сужение типа после проверки ``missing`` (mypy не выводит его из
        # списка имён); значения непусты, иначе ветка выше уже подняла ошибку.
        product_id = cast(str, fields["product_id"])
        if product_id in seen:
            raise _error(
                path,
                f"{_EXEMPT_KEY}: продукт {product_id!r} объявлен дважды — два решения "
                "об одном продукте неразрешимы",
            )
        seen.add(product_id)
        entries.append(
            GoldenProductExemption(
                product_id=product_id,
                reason=cast(str, fields["reason"]),
                declared_by=cast(str, fields["declared_by"]),
                declared_at=_declined_at(
                    raw.get("declared_at"), path, key=f"{where}.declared_at"
                ),
            )
        )
    return tuple(entries)


def _derived(file_present: bool) -> GoldenStatus:
    """Состояние без объявления: выводится из наличия ``golden.csv``."""
    return GoldenStatus(
        state=STATE_PRESENT if file_present else STATE_ABSENT,
        declared=False,
        file_present=file_present,
    )


def read_golden_status(package_dir: str | Path) -> GoldenStatus:
    """Читает и валидирует объявление состояния эталона пакета.

    Отсутствующий (или содержащий только комментарии) ``sources/golden_status.yaml`` —
    не ошибка: состояние выводится из наличия ``golden.csv`` (:func:`_derived`).
    Присутствующее объявление разбирается строго: неизвестные ключи, чужой ``schema``,
    неизвестное состояние и противоречия «объявлено ↔ на диске» — видимые ошибки пакета.

    :raises ValueError: файл не читается/не YAML-словарь; схема не та; состояние
        неизвестно; ``present`` без файла; ``absent`` при файле; ``declined`` без
        ``reason``/``declined_by``; ``reason``/``declined_by``/``declined_at`` при
        не-``declined`` состоянии; ``declined_at`` не ISO-дата; запись
        ``exempt_products`` без ``product_id``/``reason``/``declared_by``, с неизвестным
        ключом, не-словарём или дубликатом ``product_id``.
    """
    base = Path(package_dir)
    file_present = (base / GOLDEN_FILE).is_file()
    path = golden_status_path(base)
    if not path.is_file():
        return _derived(file_present)

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise _error(path, f"не удалось прочитать: {exc}") from exc
    except yaml.YAMLError as exc:
        raise _error(path, f"некорректный YAML: {exc}") from exc

    # Файл из одних комментариев безопасен как шаблон и не объявляет ничего:
    # «нет ключей» ≡ «нет объявления» (иначе шаблон пакета не мог бы показать форму,
    # не выбрав за автора состояние).
    if data is None:
        return _derived(file_present)
    if not isinstance(data, dict):
        raise _error(
            path, f"ожидался YAML-словарь, получено {type(data).__name__}"
        )

    unknown = sorted(str(key) for key in data if key not in _ALLOWED_KEYS)
    if unknown:
        raise _error(
            path,
            f"неизвестные ключи {unknown}; допустимы {sorted(_ALLOWED_KEYS)}",
        )

    if data.get("schema") != GOLDEN_STATUS_SCHEMA:
        raise _error(
            path,
            f"schema обязателен и равен {GOLDEN_STATUS_SCHEMA!r}, "
            f"получено {data.get('schema')!r}",
        )

    raw_state = data.get("state")
    if raw_state is None:
        raise _error(path, f"state обязателен: один из {list(GOLDEN_STATES)}")
    state = str(raw_state).strip()
    if state not in GOLDEN_STATES:
        raise _error(
            path, f"неизвестное состояние state={state!r}; допустимы {list(GOLDEN_STATES)}"
        )

    reason = _text(data.get("reason"))
    declined_by = _text(data.get("declined_by"))
    declined_at = _declined_at(data.get("declined_at"), path)
    # Waiver'ы по продуктам читаются до проверок состояния: поле осмысленно при любом
    # ``state`` (продукт может быть исключён и при ``present``), а ошибка в записи — ошибка
    # пакета независимо от того, годится ли набор целиком.
    exempt_products = _exempt_products(data.get(_EXEMPT_KEY), path)

    if state == STATE_PRESENT and not file_present:
        raise _error(
            path,
            "state: present, но golden.csv отсутствует — объявлено то, чего нет; "
            "уберите объявление либо объявите state: absent",
        )
    if state == STATE_ABSENT and file_present:
        raise _error(
            path,
            "state: absent, но golden.csv существует — неоднозначность; уберите файл "
            "либо объявите state: declined (файл остаётся, но не используется)",
        )
    if state == STATE_DECLINED:
        missing = [
            name
            for name, value in (("reason", reason), ("declined_by", declined_by))
            if value is None
        ]
        if missing:
            raise _error(
                path,
                f"state: declined требует {missing}: без причины и автора это тихий "
                "waiver, а он запрещён",
            )
    else:
        declared_only = [
            name
            for name, value in (
                ("reason", reason),
                ("declined_by", declined_by),
                ("declined_at", declined_at),
            )
            if value is not None
        ]
        if declared_only:
            raise _error(
                path,
                f"ключи {declared_only} имеют смысл только при state: declined "
                f"(получено state: {state})",
            )

    return GoldenStatus(
        state=state,  # type: ignore[arg-type] — проверено выше
        declared=True,
        file_present=file_present,
        reason=reason,
        declined_by=declined_by,
        declined_at=declined_at,
        exempt_products=exempt_products,
    )
