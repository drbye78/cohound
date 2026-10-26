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

**Разбор** (:func:`read_golden_status`):

* файла нет (или он содержит только комментарии) → состояние **выводится** из наличия
  ``golden.csv`` (``present`` либо ``absent``), ``declared=False``;
* ``state: present`` без ``golden.csv`` → ошибка: объявлено то, чего нет;
* ``state: absent`` при существующем ``golden.csv`` → ошибка: неоднозначность, файл надо
  либо убрать, либо объявить ``declined``;
* ``state: declined`` без ``reason`` или без ``declined_by`` → ошибка: причина
  обязательна, иначе это тихий waiver, а он запрещён;
* неизвестное состояние/ключ, чужой ``schema`` → ошибка.

Все ошибки — :class:`ValueError` с ведущим кодом ``golden_status.yaml:``: CLI печатает
их как видимую ошибку пакета (``extract_code`` берёт ведущий токен), а не как ``error: E``.

Модуль — **лист** зависимостей (stdlib + ``yaml``): его импортируют ``normalize``,
``lint``, ``plan``, ``package_io`` и CLI ``golden``. Нейтрален (§1.3): ни названий
продуктов, ни предметных терминов.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

__all__ = [
    "GOLDEN_FILE",
    "GOLDEN_STATES",
    "GOLDEN_STATUS_DIR",
    "GOLDEN_STATUS_FILE",
    "GOLDEN_STATUS_SCHEMA",
    "GoldenState",
    "GoldenStatus",
    "STATE_ABSENT",
    "STATE_DECLINED",
    "STATE_PRESENT",
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
    {"schema", "state", "reason", "declined_by", "declined_at"}
)

#: Ключи, осмысленные только при ``state: declined``.
_DECLINE_KEYS: tuple[str, ...] = ("reason", "declined_by", "declined_at")


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
    """

    state: GoldenState
    declared: bool
    file_present: bool
    reason: str | None = None
    declined_by: str | None = None
    declined_at: str | None = None

    @property
    def comparison_available(self) -> bool:
        """Можно ли сверять прогон с эталоном (только ``present``)."""
        return self.state == STATE_PRESENT

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


def _declined_at(value: Any, path: Path) -> str | None:
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
            f"declined_at={text!r} не является датой ISO-8601 (YYYY-MM-DD)",
        ) from exc


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
        не-``declined`` состоянии; ``declined_at`` не ISO-дата.
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
    )
