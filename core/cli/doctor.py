"""coHound — ``hound doctor``: самодиагностика инсталляции (03 §E.1, read-only, A1).

Команда проверяет **готовность окружения** к прогону, не выполняя сам прогон:
runtime/инсталляция, зависимости по extras-группам, наличие env/ключей (значения **не**
печатаются), файлы конфигурации, операционные каталоги и запись в них, CLI-поверхность
и (опционально) фундамент ``tests/selfcheck.py``.

**Гарантии.**

* **Read-only и без сети по умолчанию.** Ни одна проверка не ходит в интернет; запуск
  ``selfcheck`` — только при ``--with-selfcheck``.
* **Никогда не падает.** Каждая проверка обёрнута в try/except: исключение превращается
  в запись ``status="fail"`` с сообщением, команда продолжает работу.
* **Секреты не печатаются.** Env-проверки фиксируют лишь **наличие** переменной.
* **Нейтральность §1.3.** Никаких названий продуктов/вендоров/предметных терминов.

Каждая запись — :class:`CheckResult` со ``status ∈ {"ok","warn","fail"}``, ``detail`` и
опциональным ``hint``. Группы — :class:`CheckGroup`. Итог — :class:`DoctorReport` с
детерминированным JSON (``sort_keys=True``, ``ensure_ascii=False``).

Коды выхода: :data:`~core.cli.main.EXIT_OK`, если нет ``fail``; :data:`~core.cli.main.EXIT_ERROR`,
если есть ``fail``. Флаг ``--strict`` переводит любой ``warn`` в :data:`~core.cli.main.EXIT_ERROR`.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.adapters.search_duckduckgo import ENV_BACKEND as ENV_DDG_BACKEND
from core.adapters.search_glm import ENV_API_KEY as ENV_GLM_API_KEY
from core.adapters.search_google import (
    ENV_API_KEY as ENV_GOOGLE_API_KEY,
    ENV_API_KEY_ALT as ENV_GOOGLE_API_KEY_ALT,
    ENV_CSE_ID as ENV_GOOGLE_CSE_ID,
    ENV_CSE_ID_ALT as ENV_GOOGLE_CSE_ID_ALT,
)
from core.adapters.search_openwebsearch import (
    ENV_BINARY as ENV_OWS_BIN,
    ENV_ENGINE as ENV_OWS_ENGINE,
)
from core.adapters.search_yandex import (
    ENV_API_KEY as ENV_YANDEX_API_KEY,
    ENV_FOLDER_ID as ENV_YANDEX_FOLDER_ID,
    ENV_SEARCH_MODE as ENV_YANDEX_MODE,
)
from core.cli.main import EXIT_ERROR, EXIT_OK
from core.cli.wiring import (
    ENV_LLM_API_KEY,
    ENV_LLM_BASE_URL,
    ENV_LLM_DECODER,
    ENV_LLM_MAX_TOKENS,
    ENV_LLM_MODEL,
    ENV_LLM_TIMEOUT,
    ENV_PLUSVIBE_API_KEY,
    ENV_SCOUT_CRAWLER,
    SUPPORTED_SCOUT_CRAWLERS,
    SUPPORTED_SEARCH_ADAPTERS,
    WiringError,
    build_search_adapter,
)
from core.config.deploy import DEFAULT_DEPLOY_PATH, load_deploy_config
from core.config.paths import (
    DEFAULT_PATHS_FILE,
    ENV_BY_FIELD,
    ENV_HOME,
    ENV_PATHS_FILE,
    HARNESS_FIELDS,
    load_paths_file,
    resolve_paths,
)
from core.ports.search import SearchAdapterPort

__all__ = [
    "CheckResult",
    "CheckGroup",
    "DoctorReport",
    "run_doctor",
    "cmd_doctor",
]

#: Коды состояния проверки.
OK = "ok"
WARN = "warn"
FAIL = "fail"

#: Требование к Python по умолчанию, если ``pyproject.toml`` не читается.
DEFAULT_REQUIRES_PYTHON = ">=3.14"

#: Обязательные команды CLI (03 §E.1) — должны присутствовать в поверхности.
REQUIRED_COMMANDS: tuple[str, ...] = (
    "resolve",
    "plan",
    "run",
    "discover",
    "review",
    "promote",
    "report",
    "golden",
    "sensitivity",
    "diff",
    "deploy-check",
)

#: Операционные каталоги (раскладка A.1): присутствие/создаваемость + запись.
OPERATIONAL_DIRS: tuple[str, ...] = ("runs", "index_store", "corpus_raw", "cache")

#: Группы зависимостей: (имя группы, обязательна ли, модули). Имена extras — из
#: ``pyproject.toml`` (05-dependency-stack.md). Отсутствие обязательной → ``fail``,
#: опциональной → ``warn``.
DEPENDENCY_GROUPS: tuple[tuple[str, bool, tuple[str, ...]], ...] = (
    ("deps:core", True, ("pydantic", "yaml")),
    ("deps:retrieval", False, ("numpy", "tantivy")),
    ("deps:search", False, ("httpx", "ddgs")),
    ("deps:crawl", False, ("scrapy", "playwright")),
    ("deps:extract", False, ("trafilatura", "docling", "openpyxl", "rtree")),
    ("deps:safety", False, ("nh3", "defusedxml")),
    ("deps:ops", False, ("tenacity", "structlog")),
    ("deps:embed", False, ("sentence_transformers",)),
    ("deps:embed-lite", False, ("model2vec",)),
    ("deps:dev", False, ("pytest",)),
)

#: Имя модуля → имя дистрибутива (для ``importlib.metadata.version``), где расходятся.
_DIST_NAMES: dict[str, str] = {
    "yaml": "PyYAML",
    "sentence_transformers": "sentence-transformers",
}


# ---------------------------------------------------------------------------
# Модель отчёта
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    """Результат одной проверки: ``status`` (ok/warn/fail), ``detail``, опц. ``hint``."""

    name: str
    status: str
    detail: str
    hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (``hint`` включается только при наличии)."""
        data: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
        }
        if self.hint:
            data["hint"] = self.hint
        return data


@dataclass
class CheckGroup:
    """Именованная группа проверок."""

    group: str
    checks: list[CheckResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"group": self.group, "checks": [c.to_dict() for c in self.checks]}


@dataclass
class DoctorReport:
    """Полный отчёт диагностики: группы + сводка + сведения о Python."""

    groups: list[CheckGroup] = field(default_factory=list)

    def checks(self) -> list[CheckResult]:
        """Плоский список всех проверок (для подсчёта сводки)."""
        return [c for group in self.groups for c in group.checks]

    def summary(self) -> dict[str, int]:
        """Подсчёт ``ok``/``warn``/``fail`` по всем проверкам."""
        counts = {OK: 0, WARN: 0, FAIL: 0}
        for check in self.checks():
            counts[check.status] = counts.get(check.status, 0) + 1
        return counts

    def ok(self) -> bool:
        """``True``, если нет ни одной проверки со ``status="fail"``."""
        return self.summary()[FAIL] == 0

    def to_payload(self) -> dict[str, Any]:
        """Детерминированный JSON-совместимый отчёт (``sort_keys=True`` при печати)."""
        return {
            "ok": self.ok(),
            "summary": self.summary(),
            "groups": [group.to_dict() for group in self.groups],
            "python": platform.python_version(),
            "executable": sys.executable,
        }


class _GroupBuilder:
    """Накопитель проверок одной группы с защитой от исключений (не роняет команду)."""

    def __init__(self, name: str) -> None:
        self.group = CheckGroup(group=name)

    def add(
        self,
        name: str,
        fn: Callable[[], str | tuple[str, str] | tuple[str, str, str | None]],
        *,
        hint: str | None = None,
    ) -> None:
        """Прогоняет ``fn``, собирая ``(status, detail[, hint])``; исключение → ``fail``.

        ``fn`` может вернуть:
        * строку ``detail`` (статус — :data:`OK`);
        * ``(status, detail)``;
        * ``(status, detail, hint)``.
        """
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001 — диагностика не должна падать
            self.group.checks.append(
                CheckResult(
                    name=name,
                    status=FAIL,
                    detail=f"{type(exc).__name__}: {exc}".strip(),
                    hint=hint,
                )
            )
            return
        resolved = _normalize_result(result, hint)
        self.group.checks.append(CheckResult(name=name, status=resolved[0], detail=resolved[1], hint=resolved[2]))


def _normalize_result(
    result: str | tuple[str, str] | tuple[str, str, str | None],
    default_hint: str | None,
) -> tuple[str, str, str | None]:
    """Нормализует возврат проверки к ``(status, detail, hint)``."""
    if isinstance(result, str):
        return OK, result, default_hint
    if len(result) == 2:
        return str(result[0]), str(result[1]), default_hint
    return str(result[0]), str(result[1]), result[2]


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def repo_root() -> Path:
    """Корень репозитория: ``core/cli/doctor.py`` → ``parents[2]``."""
    return Path(__file__).resolve().parents[2]


def _env_set(name: str) -> bool:
    """Задана ли env-переменная непустым значением (значение НЕ возвращается)."""
    return bool(os.environ.get(name, "").strip())


def _version(module: str) -> str | None:
    """Версия дистрибутива модуля (или ``None``, если метаданных нет)."""
    dist = _DIST_NAMES.get(module, module)
    try:
        return importlib.metadata.version(dist)
    except importlib.metadata.PackageNotFoundError:
        return None
    except Exception:  # noqa: BLE001 — метаданные не критичны
        return None


def _required_python(root: Path) -> str:
    """Читает ``requires-python`` из ``pyproject.toml``; иначе :data:`DEFAULT_REQUIRES_PYTHON`."""
    try:
        import tomllib

        data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
        value = data.get("project", {}).get("requires-python")
        if isinstance(value, str) and value.strip():
            return value.strip()
    except Exception:  # noqa: BLE001 — нет файла/парсера — дефолт
        pass
    return DEFAULT_REQUIRES_PYTHON


# ---------------------------------------------------------------------------
# Группа 1: runtime / инсталляция
# ---------------------------------------------------------------------------


def _runtime_group(root: Path) -> CheckGroup:
    builder = _GroupBuilder("runtime")

    def _python_version() -> tuple[str, str]:
        required = _required_python(root)
        match = re.search(r">=\s*(\d+)\.(\d+)", required)
        current = f"{platform.python_version()} (требуется {required})"
        if match:
            want = (int(match.group(1)), int(match.group(2)))
            if sys.version_info[:2] >= want:
                return OK, current
            return FAIL, current
        return WARN, f"{current}; требование не распознано"

    builder.add("python_version", _python_version, hint="требуется Python по pyproject.toml (05 §1)")
    builder.add("sys_executable", lambda: str(sys.executable))
    builder.add(
        "venv_active",
        lambda: (OK, "проектный venv активен")
        if ".venv" in str(sys.prefix)
        else (WARN, f"запущено вне .venv (sys.prefix={sys.prefix})"),
        hint="запустите через .venv/bin/python или hound.py (автоперезапуск)",
    )
    builder.add("platform", lambda: platform.platform())
    builder.add(
        "hound_root_script",
        lambda: (OK, "hound.py присутствует в корне")
        if (root / "hound.py").is_file()
        else (WARN, "hound.py не найден в корне"),
        hint="hound.py — удобная обёртка (перезапуск в .venv + sys.path)",
    )
    builder.add(
        "no_reexec",
        lambda: f"COHOUND_NO_REEXEC={'задан' if _env_set('COHOUND_NO_REEXEC') else 'не задан'}",
    )
    return builder.group


# ---------------------------------------------------------------------------
# Группа 2: зависимости по extras-группам
# ---------------------------------------------------------------------------


def _dependency_group(group_name: str, required: bool, modules: tuple[str, ...]) -> CheckGroup:
    builder = _GroupBuilder(group_name)

    def _check(module: str, *, _required: bool = required) -> Callable[[], tuple[str, str]]:
        def fn() -> tuple[str, str]:
            installed = importlib.util.find_spec(module) is not None
            if not installed:
                status = FAIL if _required else WARN
                need = "обязательна" if _required else "опциональна"
                return status, f"не установлен ({need}; extra из pyproject.toml)"
            version = _version(module)
            suffix = f" {version}" if version else " (версия неизвестна)"
            return OK, f"установлен{suffix}"

        return fn

    for module in modules:
        builder.add(module, _check(module))
    return builder.group


# ---------------------------------------------------------------------------
# Группа 3: переменные окружения / ключи (наличие, значения не печатаются)
# ---------------------------------------------------------------------------

#: Намёк о зоне секретов эгрессии (07 §4/§7): в split поисковые ключи живут только в edge.
_EGRESS_HINT = (
    "в профиле split ключи внешнего поиска доступны только в edge (07 §4); "
    "оценщик (I18) веб-тула не имеет"
)


def _env_group() -> CheckGroup:
    builder = _GroupBuilder("env")

    # -- LLM-контур (env-ключи + фолбэки) --------------------------------- #
    builder.add(
        "llm_base_url",
        lambda: (OK, "задан") if _env_set(ENV_LLM_BASE_URL) else (WARN, "не задан → дефолт"),
        hint="пусто → DEFAULT_LLM_BASE_URL; переопределяется --llm-base-url",
    )
    builder.add(
        "llm_api_key",
        lambda: (OK, f"{ENV_LLM_API_KEY} задан")
        if _env_set(ENV_LLM_API_KEY)
        else (
            (OK, f"фолбэк {ENV_PLUSVIBE_API_KEY} задан")
            if _env_set(ENV_PLUSVIBE_API_KEY)
            else (WARN, f"ни {ENV_LLM_API_KEY}, ни {ENV_PLUSVIBE_API_KEY} не заданы")
        ),
        hint="нужен только для вызова LLM (passport-scout)",
    )
    builder.add(
        "llm_model",
        lambda: (OK, "задан") if _env_set(ENV_LLM_MODEL) else (WARN, "не задан → дефолт"),
        hint="пусто → DEFAULT_LLM_MODEL; переопределяется --llm-model",
    )
    for name, env in (
        ("llm_decoder", ENV_LLM_DECODER),
        ("llm_timeout", ENV_LLM_TIMEOUT),
        ("llm_max_tokens", ENV_LLM_MAX_TOKENS),
    ):
        builder.add(
            name,
            lambda _env=env: (OK, "задан") if _env_set(_env) else (WARN, "не задан → дефолт"),
        )

    # -- Search-адаптеры -------------------------------------------------- #
    def _yandex() -> tuple[str, str]:
        key = _env_set(ENV_YANDEX_API_KEY)
        folder = _env_set(ENV_YANDEX_FOLDER_ID)
        if key and folder:
            return OK, f"{ENV_YANDEX_API_KEY}+{ENV_YANDEX_FOLDER_ID} заданы"
        missing = [n for n, ok in ((ENV_YANDEX_API_KEY, key), (ENV_YANDEX_FOLDER_ID, folder)) if not ok]
        return WARN, "не заданы: " + ", ".join(missing)

    builder.add("search_yandex", _yandex, hint=_EGRESS_HINT)
    builder.add(
        "yandex_search_mode",
        lambda: (OK, "задан") if _env_set(ENV_YANDEX_MODE) else (WARN, "не задан → async"),
    )
    builder.add(
        "search_glm",
        lambda: (OK, f"{ENV_GLM_API_KEY} задан") if _env_set(ENV_GLM_API_KEY) else (WARN, "не задан"),
        hint=_EGRESS_HINT,
    )

    def _google() -> tuple[str, str]:
        key = _env_set(ENV_GOOGLE_API_KEY) or _env_set(ENV_GOOGLE_API_KEY_ALT)
        cse = _env_set(ENV_GOOGLE_CSE_ID) or _env_set(ENV_GOOGLE_CSE_ID_ALT)
        if key and cse:
            return OK, "ключ + cx заданы"
        missing = []
        if not key:
            missing.append(f"{ENV_GOOGLE_API_KEY}/{ENV_GOOGLE_API_KEY_ALT}")
        if not cse:
            missing.append(f"{ENV_GOOGLE_CSE_ID}/{ENV_GOOGLE_CSE_ID_ALT}")
        return WARN, "не заданы: " + ", ".join(missing)

    builder.add("search_google", _google, hint=_EGRESS_HINT)
    builder.add(
        "duckduckgo_backend",
        lambda: (OK, "задан") if _env_set(ENV_DDG_BACKEND) else (WARN, "не задан → duckduckgo"),
    )
    builder.add(
        "open_websearch",
        lambda: (OK, "задан")
        if (_env_set(ENV_OWS_BIN) or _env_set(ENV_OWS_ENGINE))
        else (WARN, f"не заданы {ENV_OWS_BIN}/{ENV_OWS_ENGINE}"),
    )

    # -- Scout ------------------------------------------------------------ #
    def _scout() -> tuple[str, str, str | None]:
        raw = os.environ.get(ENV_SCOUT_CRAWLER)
        if raw is None or not raw.strip():
            return OK, "не задан → http", None
        name = raw.strip().lower()
        if name in SUPPORTED_SCOUT_CRAWLERS:
            return OK, f"{name}", None
        return (
            FAIL,
            f"недопустимый {ENV_SCOUT_CRAWLER}={name!r}; поддерживаются: "
            f"{', '.join(SUPPORTED_SCOUT_CRAWLERS)}",
            None,
        )  # noqa: ERA001 — все ветки отдают 3-tuple (status, detail, hint)

    builder.add("scout_crawler", _scout)
    return builder.group


# ---------------------------------------------------------------------------
# Группа 4: файлы конфигурации
# ---------------------------------------------------------------------------


def _config_group(root: Path) -> CheckGroup:
    builder = _GroupBuilder("config")

    def _deploy() -> tuple[str, str, str | None]:
        path = root / DEFAULT_DEPLOY_PATH
        if not path.is_file():
            return (
                WARN,
                f"{DEFAULT_DEPLOY_PATH} отсутствует",
                "профиль local работает без файла; split требует config/deploy.yaml",
            )
        config = load_deploy_config(path)
        return OK, f"profile={config.profile}", None

    builder.add("deploy_yaml", _deploy)

    def _registries() -> tuple[str, str]:
        reg = root / "registries"
        if not reg.is_dir():
            return WARN, "каталог registries/ отсутствует"
        yamls = sorted(p.name for p in reg.glob("*.yaml"))
        if not yamls:
            return WARN, "registries/ без *.yaml"
        return OK, f"{len(yamls)} yaml: {', '.join(yamls)}"

    builder.add("registries", _registries)

    def _env_example() -> tuple[str, str]:
        if (root / ".env.example").is_file():
            return OK, ".env.example присутствует"
        return WARN, ".env.example не найден (ожидается в репозитории)"

    builder.add("env_example", _env_example)
    builder.add(
        "env_file",
        lambda: (OK, ".env присутствует") if (root / ".env").is_file() else (OK, ".env отсутствует (опционален)"),
    )

    def _packages() -> tuple[str, str]:
        pkgs = root / "packages"
        if not pkgs.is_dir():
            return WARN, "каталог packages/ отсутствует"
        names = sorted(
            child.name
            for child in pkgs.iterdir()
            if child.is_dir() and (child / "package.yaml").is_file()
        )
        if not names:
            return WARN, "packages/ без пакетов (package.yaml не найден)"
        return OK, f"{len(names)}: {', '.join(names)}"

    builder.add("packages", _packages)
    return builder.group


# ---------------------------------------------------------------------------
# Группа 5: операционные каталоги и запись
# ---------------------------------------------------------------------------


def _dir_writable(path: Path, label: str) -> tuple[str, str]:
    """Проверяет существование/создаваемость и запись в каталог ``path`` (без мусора)."""
    if path.exists():
        if not path.is_dir():
            return FAIL, f"{label}: существует, но не каталог"
        with tempfile.NamedTemporaryFile(dir=path, prefix=".doctor-", delete=True):
            pass
        if os.access(path, os.W_OK):
            return OK, f"{label}: запись доступна"
        return FAIL, f"{label}: нет доступа на запись"
    parent = path.parent
    if parent.exists() and os.access(parent, os.W_OK):
        return OK, f"{label}: отсутствует, будет создан при прогоне"
    return FAIL, f"{label}: отсутствует и родитель недоступен на запись"


def _effective_dir(base: Path, value: Path) -> Path:
    """Эффективный каталог раскладки: абсолютный ``value`` как есть, относительный — от ``base``.

    Резолвер раскладки возвращает абсолютные пути при заданном ``COHOUND_HOME``/абсолютных
    env; относительные дефолты означают «от CWD». Для диагностики в границах ``base``
    (корень репозитория) относительные приклеиваются к нему — так ``run_doctor(root=tmp)``
    тестируем, а env/``home``-значения остаются абсолютными.
    """
    return value if value.is_absolute() else base / value


def _dirs_group(base: Path) -> CheckGroup:
    """Операционные каталоги (раскладка A.1) с учётом resolved-путей (03 §A.1).

    Каталоги берутся из :func:`core.config.paths.resolve_paths` (env/``paths.yaml``/дефолт),
    поэтому диагностика показывает доступность записи именно в **эффективные** каталоги.
    Ошибка резолва (битый ``paths.yaml``) не роняет doctor: используется безопасный
    относительный дефолт.
    """
    builder = _GroupBuilder("dirs")
    try:
        config = resolve_paths()
    except Exception:  # noqa: BLE001 — диагностика не должна падать (см. load_paths_file)
        config = None
    for rel in OPERATIONAL_DIRS:
        value = getattr(config, rel) if config is not None else Path(rel)
        target = _effective_dir(base, value)
        builder.add(rel, lambda _t=target, _rel=rel: _dir_writable(_t, _rel))
    return builder.group


# ---------------------------------------------------------------------------
# Группа 5b: эффективная раскладка каталогов (resolved layout, 03 §A.1)
# ---------------------------------------------------------------------------


def _layout_source(field: str, file_doc: dict[str, Any]) -> str:
    """Источник эффективного пути: ``env`` > ``file`` > ``home`` > ``default``.

    Per-field env (``COHOUND_<FIELD>_DIR``) — ``env``; значение из ``paths.yaml`` — ``file``;
    заданный ``COHOUND_HOME`` (сдвигает относительные) — ``home``; иначе ``default``.
    """
    if os.environ.get(ENV_BY_FIELD.get(field, "")):
        return "env"
    if field in file_doc:
        return "file"
    if os.environ.get(ENV_HOME):
        return "home"
    return "default"


def _layout_group() -> CheckGroup:
    """Показывает эффективную раскладку (resolved) и её источник для каждой оси (03 §A.1).

    Значения — абсолютные при ``COHOUND_HOME``/абсолютных env; относительные — от CWD
    (эффективное поведение). Секреты не печатаются (только пути).
    """
    builder = _GroupBuilder("layout")
    file_target = Path(os.environ.get(ENV_PATHS_FILE) or DEFAULT_PATHS_FILE)
    try:
        file_doc = load_paths_file(file_target) if file_target.is_file() else {}
    except ValueError:
        file_doc = {}

    def _check(field: str) -> tuple[str, str, str | None]:
        config = resolve_paths()
        value = getattr(config, field)
        source = _layout_source(field, file_doc)
        hint = None
        if field in ("runs", "packages", "passports"):
            hint = "CLI-флаг (--runs-dir/--passports-dir/--dir) переопределяет resolved-значение"
        return OK, f"{value} (источник: {source})", hint

    for field in HARNESS_FIELDS:
        builder.add(f"layout.{field}", lambda _f=field: _check(_f))
    return builder.group


# ---------------------------------------------------------------------------
# Группа 6: CLI-поверхность
# ---------------------------------------------------------------------------


def _cli_group() -> CheckGroup:
    builder = _GroupBuilder("cli")

    def _commands() -> list[str]:
        from core.cli.main import build_parser

        parser = build_parser()
        subparsers = next(
            action
            for action in parser._actions
            if hasattr(action, "choices") and isinstance(action.choices, dict)
        )
        return list(subparsers.choices or {})

    def _command_count() -> tuple[str, str]:
        return OK, f"{len(_commands())} команд"

    def _required() -> tuple[str, str]:
        commands = set(_commands())
        missing = [name for name in REQUIRED_COMMANDS if name not in commands]
        if missing:
            return FAIL, "отсутствуют: " + ", ".join(missing)
        return OK, f"все {len(REQUIRED_COMMANDS)} ключевых команд зарегистрированы"

    builder.add("command_count", _command_count)
    builder.add("required_commands", _required)

    def _search_adapters() -> tuple[str, str]:
        broken: list[str] = []
        for name in SUPPORTED_SEARCH_ADAPTERS:
            try:
                adapter = build_search_adapter(name)
            except WiringError as exc:
                broken.append(f"{name}: {exc}")
                continue
            if not isinstance(adapter, SearchAdapterPort):
                broken.append(f"{name}: не SearchAdapterPort")
        if broken:
            return FAIL, "; ".join(broken)
        return OK, f"все {len(SUPPORTED_SEARCH_ADAPTERS)} адаптеров собираются"

    builder.add("search_adapters", _search_adapters)
    return builder.group


# ---------------------------------------------------------------------------
# Группа 7: фундамент (selfcheck)
# ---------------------------------------------------------------------------


def _foundation_group(root: Path, *, with_selfcheck: bool) -> CheckGroup:
    builder = _GroupBuilder("foundation")
    script = root / "tests" / "selfcheck.py"

    def _present() -> tuple[str, str, str | None]:
        if script.is_file():
            return OK, "tests/selfcheck.py присутствует", None
        return WARN, "tests/selfcheck.py не найден", None

    builder.add("selfcheck_present", _present)

    if with_selfcheck:

        def _run() -> tuple[str, str]:
            completed = subprocess.run(
                [sys.executable, str(script)],
                cwd=str(root),
                capture_output=True,
                text=True,
                timeout=900,
                check=False,
            )
            rc = completed.returncode
            lines = [line for line in completed.stdout.strip().splitlines() if line.strip()]
            last = lines[-1] if lines else "(нет вывода)"
            if rc == 0:
                return OK, f"rc=0; {last}"
            return FAIL, f"rc={rc}; {last}"

        builder.add("selfcheck_run", _run)
    else:
        builder.add(
            "selfcheck_run",
            lambda: (OK, "пропущен (нет --with-selfcheck)"),
        )
    return builder.group


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------


def run_doctor(
    *, root: str | Path | None = None, with_selfcheck: bool = False
) -> DoctorReport:
    """Собирает полный отчёт диагностики (read-only, без сети, не бросает исключений).

    Каждая проверка изолирована: исключение внутри превращается в запись ``status="fail"``
    (см. :class:`_GroupBuilder`), поэтому функция завершается всегда.

    :param root: корень репозитория; ``None`` → :func:`repo_root`.
    :param with_selfcheck: запускать ли ``tests/selfcheck.py`` как subprocess (по умолчанию
        нет — только проверка наличия файла).
    """
    base = Path(root) if root is not None else repo_root()
    report = DoctorReport()
    report.groups.append(_runtime_group(base))
    for group_name, required, modules in DEPENDENCY_GROUPS:
        report.groups.append(_dependency_group(group_name, required, modules))
    report.groups.append(_env_group())
    report.groups.append(_config_group(base))
    report.groups.append(_layout_group())
    report.groups.append(_dirs_group(base))
    report.groups.append(_cli_group())
    report.groups.append(_foundation_group(base, with_selfcheck=with_selfcheck))
    return report


def _print_human(payload: dict[str, Any]) -> None:
    """Человекочитаемый групповой отчёт (без секретов)."""
    sys.stdout.write(f"doctor: инсталляция coHound (python {payload['python']})\n")
    sys.stdout.write(f"executable: {payload['executable']}\n")
    for group in payload["groups"]:
        sys.stdout.write(f"\n[{group['group']}]\n")
        for check in group["checks"]:
            marker = {"ok": "ok  ", "warn": "warn", "fail": "FAIL"}[check["status"]]
            sys.stdout.write(f"  {marker} {check['name']}: {check['detail']}\n")
            if check.get("hint"):
                sys.stdout.write(f"       hint: {check['hint']}\n")
    summary = payload["summary"]
    sys.stdout.write(
        f"\nsummary: ok={summary['ok']} warn={summary['warn']} fail={summary['fail']}\n"
    )


def cmd_doctor(args: Any) -> int:
    """``doctor``: собрать и напечатать диагностику; вернуть код выхода.

    ``--json`` — детерминированный JSON (``sort_keys=True``, ``ensure_ascii=False``).
    ``--with-selfcheck`` — дополнительно прогнать ``tests/selfcheck.py``.
    ``--strict`` — любой ``warn`` даёт ненулевой код.

    :returns: :data:`~core.cli.main.EXIT_OK`, если нет ``fail`` (и при ``--strict`` нет
        ``warn``); иначе :data:`~core.cli.main.EXIT_ERROR`.
    """
    report = run_doctor(with_selfcheck=bool(getattr(args, "with_selfcheck", False)))
    payload = report.to_payload()
    if getattr(args, "json", False):
        sys.stdout.write(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
        )
    else:
        _print_human(payload)
    summary = payload["summary"]
    if summary[FAIL] > 0:
        return EXIT_ERROR
    if getattr(args, "strict", False) and summary[WARN] > 0:
        return EXIT_ERROR
    return EXIT_OK
